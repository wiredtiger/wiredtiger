#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from code_coverage_utils import check_build_dirs, run_task_lists_in_parallel

class PushWorkingDirectory:
    def __init__(self, new_working_directory: str) -> None:
        self.original_working_directory = os.getcwd()
        self.new_working_directory = new_working_directory
        os.chdir(self.new_working_directory)

    def pop(self):
        os.chdir(self.original_working_directory)

def delete_runtime_coverage_files(build_dir_base: str, verbose: bool):
    for root, dirs, files in os.walk(build_dir_base):
        for filename in files:
            if filename.endswith('.gcda'):
                #print(f"{root}, {dirs}, {filename}")
                file_path = os.path.join(root, filename)
                if verbose:
                    print(f"Deleting: {file_path}")
                os.remove(file_path)
                if verbose:
                    print(f"Deleted: {file_path}")


def run_coverage_task_list(task_list_info):
    build_dir = task_list_info["build_dir"]
    task_list = task_list_info["task_bucket"]
    verbose = task_list_info['verbose']
    list_start_time = datetime.now()
    for index in range(len(task_list)):
        task = task_list[index]
        if verbose:
            print("Running task {} in {}".format(task, build_dir))

        start_time = datetime.now()
        try:
            delete_runtime_coverage_files(build_dir_base=build_dir, verbose=verbose)
            os.chdir(build_dir)
            split_command = task.split()
            subprocess.run(split_command, check=True)
            copy_dest_dir = f"{build_dir}_{index}_copy"
            if (verbose):
                print(f"Copying directory {build_dir} to {copy_dest_dir}")
            shutil.copytree(src=build_dir, dst=copy_dest_dir)

            task_info = {"task": task}
            task_info_as_json_object = json.dumps(task_info, indent=2)
            task_info_file_path = os.path.join(copy_dest_dir, "task_info.json")
            with open(task_info_file_path, "w") as output_file:
                output_file.write(task_info_as_json_object)

        except subprocess.CalledProcessError as exception:
            print(f'Command {exception.cmd} failed with error {exception.returncode}')
        end_time = datetime.now()
        diff = end_time - start_time

        if verbose:
            print("Finished task {} in {} : took {} seconds".format(task, build_dir, diff.total_seconds()))

    list_end_time = datetime.now()
    diff = list_end_time - list_start_time

    return_value = "Completed task list in {} : took {} seconds".format(build_dir, diff.total_seconds())

    if verbose:
        print(return_value)

    return return_value


# Execute each list of tasks in parallel
def run_coverage_task_lists_in_parallel(label, task_bucket_info):
    parallel = len(task_bucket_info)
    verbose = task_bucket_info[0]['verbose']
    task_start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel) as executor:
        for e in executor.map(run_coverage_task_list, task_bucket_info):
             if verbose:
                print(e)

    task_end_time = datetime.now()
    task_diff = task_end_time - task_start_time

    if verbose:
        print("Time taken to perform {}: {} seconds".format(label, task_diff.total_seconds()))


def run_gcovr(build_dir_base: str, gcovr_dir: str, verbose: bool):
    print(f"Starting run_gcovr({build_dir_base}, {gcovr_dir})")
    dir_name = os.path.dirname(build_dir_base)
    filenames_in_dir = os.listdir(dir_name)
    filenames_in_dir.sort()
    for file_name in filenames_in_dir:
        if file_name.startswith('build_') and file_name.endswith("copy"):
            build_copy_name = file_name
            build_copy_path = os.path.join(dir_name, build_copy_name)
            task_info_path = os.path.join(build_copy_path, "task_info.json")
            coverage_output_dir = os.path.join(gcovr_dir, build_copy_name)
            if verbose:
                print(f"build_copy_name = {build_copy_name}, build_copy_path = {build_copy_path}, task_info_path = {task_info_path}, coverage_output_dir = {coverage_output_dir}")
            os.mkdir(coverage_output_dir)
            shutil.copy(src=task_info_path, dst=coverage_output_dir)
            gcovr = "gcovr"
            gcov = "/opt/mongodbtoolchain/v4/bin/gcov"
            gcovr_command = f"{gcovr} {build_copy_name} -f src -j 4 --html-self-contained --html-details {coverage_output_dir}/2_coverage_report.html --json-summary-pretty --json-summary {coverage_output_dir}/1_coverage_report_summary.json --json {coverage_output_dir}/full_coverage_report.json"
            split_command = gcovr_command.split()
            env = os.environ.copy()
            env['GCOV'] = gcov
            if verbose:
                print(f'env: {env}')
            try:
                completed_process = subprocess.run(split_command, check=True, env=env, shell=True)
                output = completed_process.stdout
                print(f'Command returned {output}')
            except subprocess.CalledProcessError as exception:
                print(f'Command {exception.cmd} failed with error {exception.returncode} "{exception.output}"')
            if verbose:
                print(f'Completed a run of gcovr on {build_copy_name}')
    print(f"Ending run_gcovr({build_dir_base}, {gcovr_dir})")



def read_json(json_file_path: str) -> dict:
    with open(json_file_path) as json_file:
        info = json.load(json_file)
        return info


def collate_coverage_data(gcovr_dir: str, verbose: bool):
    filenames_in_dir = os.listdir(gcovr_dir)
    filenames_in_dir.sort()
    if verbose:
        print(f"Starting collate_coverage_data({gcovr_dir})")
        print(f"filenames_in_dir {filenames_in_dir}")
    collated_coverage_data = {}
    for file_name in filenames_in_dir:
        if file_name.startswith('build_') and file_name.endswith("copy"):
            build_coverage_name = file_name
            build_coverage_path = os.path.join(gcovr_dir, build_coverage_name)
            task_info_path = os.path.join(build_coverage_path, "task_info.json")
            full_coverage_path = os.path.join(build_coverage_path, "full_coverage_report.json")
            if verbose:
                print(f"task_info_path = {task_info_path}, full_coverage_path = {full_coverage_path}")
            task_info = read_json(json_file_path=task_info_path)
            coverage_info = read_json(json_file_path=full_coverage_path)
            task = task_info["task"]
            dict_entry = {
                        "task": task,
                        "build_coverage_name": build_coverage_name,
                        "full_coverage": coverage_info
                         }
            collated_coverage_data[task] = dict_entry
    if verbose:
        print(f"Ending collate_coverage_data({gcovr_dir})")
    return collated_coverage_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_path', required=True, help='Path to the json config file')
    parser.add_argument('-b', '--build_dir_base', required=True, help='Base name for the build directories')
    parser.add_argument('-j', '--parallel', default=1, type=int, help='How many tests to run in parallel')
    parser.add_argument('-g', '--gcovr_dir', help='Directory to store gcovr output in')
    parser.add_argument('-s', '--setup', action="store_true",
                        help='Perform setup actions from the config in each build directory')
    parser.add_argument('-v', '--verbose', action="store_true", help='Be verbose')
    args = parser.parse_args()

    verbose = args.verbose
    config_path = args.config_path
    build_dir_base = args.build_dir_base
    gcovr_dir = args.gcovr_dir
    parallel_tests = args.parallel
    setup = args.setup

    if verbose:
        print('Per-Test Code Coverage')
        print('======================')
        print('Configuration:')
        print(f'  Config file                      {config_path}')
        print(f'  Base name for build directories: {build_dir_base}')
        print(f'  Number of parallel tests:        {parallel_tests}')
        print(f'  Perform setup actions:           {setup}')
        print(f'  Gcovr output directory:          {gcovr_dir}')
        print()

    if parallel_tests < 1:
        sys.exit("Number of parallel tests must be >= 1")

    # Load test config json file
    with open(config_path) as json_file:
        config = json.load(json_file)

    if verbose:
        print('  Configuration:')
        print(config)

    if len(config['test_tasks']) < 1:
        sys.exit("No test tasks")

    setup_actions = config['setup_actions']

    if setup and len(setup_actions) < 1:
        sys.exit("No setup actions")

    if verbose:
        print('  Setup actions: {}'.format(setup_actions))

    if gcovr_dir:
        if not Path(gcovr_dir).is_absolute():
            sys.exit("gcovr_dir must be an absolute path")

    build_dirs = check_build_dirs(build_dir_base=build_dir_base, parallel=parallel_tests, setup=setup, verbose=verbose)

    setup_bucket_info = []
    task_bucket_info = []
    for build_dir in build_dirs:
        if setup:
            if len(os.listdir(build_dir)) > 0:
                sys.exit("Directory {} is not empty".format(build_dir))
            setup_bucket_info.append({'build_dir': build_dir, 'task_bucket': config['setup_actions'],
                                      'verbose': verbose})
        task_bucket_info.append({'build_dir': build_dir, 'task_bucket': [], 'verbose': verbose})

    if setup:
        # Perform setup operations
        run_task_lists_in_parallel(label="setup", task_bucket_info=setup_bucket_info)

    # Prepare to run the tasks in the list
    for test_num in range(len(config['test_tasks'])):
        test = config['test_tasks'][test_num]
        build_dir_number = test_num % parallel_tests

        if verbose:
            print("Prepping test [{}] as build number {}: {} ".format(test_num, build_dir_number, test))

        task_bucket_info[build_dir_number]['task_bucket'].append(test)

    if verbose:
        print("task_bucket_info: {}".format(task_bucket_info))

    # Perform task operations in parallel across the build directories
    run_coverage_task_lists_in_parallel(label="tasks", task_bucket_info=task_bucket_info)

    # Run gcovr if required
    if gcovr_dir:
        run_gcovr(build_dir_base=build_dir_base, gcovr_dir=gcovr_dir, verbose=verbose)
        # collected_coverage_data = collate_coverage_data(gcovr_dir=gcovr_dir, verbose=verbose)
        # if verbose:
        #     print('About to dump results to json')
        # report_as_json_object = json.dumps(collected_coverage_data, indent=0)
        # if verbose:
        #     print('Dumped results to json')
        # report_path = os.path.join(gcovr_dir, "per_task_coverage_report.json")
        # with open(report_path, "w") as output_file:
        #     output_file.write(report_as_json_object)


if __name__ == '__main__':
    main()
