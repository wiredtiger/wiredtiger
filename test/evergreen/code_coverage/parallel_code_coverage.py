import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
from datetime import datetime


def run_task(task_info):
    build_dir = task_info["build_dir"]
    task = task_info["task"]
    print("Running task {} in {}".format(task, build_dir))
    try:
        os.chdir(build_dir)
        split_command = task.split()
        subprocess.run(split_command, check=True)
    except subprocess.CalledProcessError as exception:
        print(f'Command {exception.cmd} failed with error {exception.returncode}')
    print("Finished task {} in {}".format(task, build_dir))


def run_task_list(task_list_info):
    build_dir = task_list_info["build_dir"]
    task_list = task_list_info["task_bucket"]
    list_start_time = datetime.now()
    for task in task_list:
        print("Running task {} in {}".format(task, build_dir))
        start_time = datetime.now()
        try:
            os.chdir(build_dir)
            split_command = task.split()
            subprocess.run(split_command, check=True)
        except subprocess.CalledProcessError as exception:
            print(f'Command {exception.cmd} failed with error {exception.returncode}')
        end_time = datetime.now()
        diff = end_time - start_time
        print("Finished task {} in {} : took {} seconds".format(task, build_dir, diff.total_seconds()))
    list_end_time = datetime.now()
    diff = list_end_time - list_start_time
    print("Completed task list in {} : took {} seconds".format(build_dir, diff.total_seconds()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_path', required=True, help='Path to the json config file')
    parser.add_argument('-b', '--build_dir_base', required=True, help='Base name for the build directories')
    parser.add_argument('-j', '--parallel', default=1, type=int, help='How many tests to run in parallel')
    parser.add_argument('-v', '--verbose', action="store_true", help='Be verbose')
    args = parser.parse_args()

    verbose = args.verbose
    config_path = args.config_path
    build_dir_base = args.build_dir_base
    parallel_tests = args.parallel

    if verbose:
        print('Code Coverage')
        print('=============')
        print('Configuration:')
        print('  Config file                      {}'.format(config_path))
        print('  Base name for build directories: {}'.format(build_dir_base))
        print('  Number of parallel tests:        {}'.format(parallel_tests))

    if parallel_tests < 1:
        os.error("Number of parallel tests must be >= 1")

    # Load test config json file
    with open(config_path) as json_file:
        config = json.load(json_file)

    if verbose:
        print('Configuration:')
        print(config)

    if len(config['test_tasks']) < 1:
        sys.exit("No test tasks")

    build_dirs = list()

    # Check the relevant build directories exist and have the correct status
    for build_num in range(parallel_tests):
        this_build_dir = "{}{}".format(build_dir_base, build_num)
        if not os.path.exists(this_build_dir):
            sys.exit("Build directory {} doesn't exist".format(this_build_dir))
        build_dirs.append(this_build_dir)

        found_compile_time_coverage_files = False
        found_run_time_coverage_files = False

        # Check build dir for coverage files
        for root, dirs, files in os.walk(this_build_dir):
            for filename in files:
                if filename.endswith('.gcno'):
                    found_compile_time_coverage_files = True
                if filename.endswith('.gcda'):
                    found_run_time_coverage_files = True

        if verbose:
            print('Found compile time coverage files in {} = {}'.
                  format(this_build_dir, found_compile_time_coverage_files))
            print('Found run time coverage files in {}     = {}'.
                  format(this_build_dir, found_run_time_coverage_files))

        if not found_compile_time_coverage_files:
            sys.exit('No compile time coverage files found within {}. Please build for code coverage.'
                     .format(args.build_dir))

        # if found_run_time_coverage_files:
        #     sys.exit('Run time coverage files found within {}. Please remove them.'.format(this_build_dir))

    if verbose:
        print("Build dirs: {}".format(build_dirs))

    # task_info = []
    task_bucket_info = []
    for build_dir in build_dirs:
        task_bucket_info.append({'build_dir': build_dir, 'task_bucket': []})

    for test_num in range(len(config['test_tasks'])):
        test = config['test_tasks'][test_num]
        build_dir_number = test_num % parallel_tests

        if verbose:
            print("Prepping test [{}] as build number {}: {} ".format(test_num, build_dir_number, test))

        task_bucket_info[build_dir_number]['task_bucket'].append(test)
        # task_info.append({'build_dir': build_dirs[build_dir_number], 'task' : test})

    print("task_bucket_info: {}".format(task_bucket_info))

    start_time = datetime.now()

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel_tests) as executor:
        for e in executor.map(run_task_list, task_bucket_info):
            print('Test')

    end_time = datetime.now()
    diff = end_time - start_time
    print("Time taken {} seconds".format(diff.total_seconds()))


if __name__ == '__main__':
    main()
