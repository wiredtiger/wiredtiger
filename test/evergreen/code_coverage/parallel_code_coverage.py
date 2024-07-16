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
import re
import json
import logging
import sys
import subprocess
import os
from datetime import datetime
from code_coverage_utils import check_build_dirs, run_task_lists_in_parallel, setup_build_dirs

def run_task_list(task_list_info):
    build_dir = task_list_info["build_dir"]
    task_list = task_list_info["task_bucket"]
    list_start_time = datetime.now()

    env = os.environ.copy()
    env["GCOV_PREFIX_STRIP"] = "4"
    for task in task_list:
        logging.debug("Running task {} in {}".format(task, build_dir))
        env["GCOV_PREFIX_STRIP"] = build_dir
        start_time = datetime.now()
        try:
            os.chdir(build_dir)
            split_command = task.split()
            subprocess.run(split_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        except subprocess.CalledProcessError as exception:
            logging.error(f'Command {exception.cmd} failed with error {exception.returncode}')
        end_time = datetime.now()
        diff = end_time - start_time

        logging.debug("Finished task {} in {} : took {} seconds".format(task, build_dir, diff.total_seconds()))

    list_end_time = datetime.now()
    diff = list_end_time - list_start_time

    return_value = "Completed task list in {} : took {} seconds".format(build_dir, diff.total_seconds())
    logging.debug(return_value)

    return return_value

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_path', required=True, help='Path to the json config file')
    parser.add_argument('-b', '--build_dir_base', required=True, help='Base name for the build directories')
    parser.add_argument('-j', '--parallel', default=1, type=int, help='How many tests to run in parallel')
    parser.add_argument('-s', '--setup', action="store_true",
                        help='Perform setup actions from the config in each build directory')
    parser.add_argument('-v', '--verbose', action="store_true", help='Be verbose')
    parser.add_argument('-p', '--python', action="store_true", help='Base name for the build directories')
    args = parser.parse_args()

    verbose = args.verbose
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    config_path = args.config_path
    build_dir_base = args.build_dir_base
    parallel_tests = args.parallel
    python = args.python
    setup = args.setup

    logging.debug('Code Coverage')
    logging.debug('=============')
    logging.debug('Configuration:')
    logging.debug('  Config file                      {}'.format(config_path))
    logging.debug('  Base name for build directories: {}'.format(build_dir_base))
    logging.debug('  Number of parallel tests:        {}'.format(parallel_tests))
    logging.debug('  Perform setup actions:           {}'.format(setup))

    if parallel_tests < 1:
        sys.exit("Number of parallel tests must be >= 1")

    # Load test config json file
    with open(config_path) as json_file:
        config = json.load(json_file)

    logging.debug('  Configuration:')
    logging.debug(config)

    if len(config['test_tasks']) < 1:
        sys.exit("No test tasks")

    setup_actions = config['setup_actions']

    if setup and len(setup_actions) < 1:
        sys.exit("No setup actions")

    logging.debug('  Setup actions: {}'.format(setup_actions))
    task_bucket_info = []
    if (setup):
        task_bucket_info = setup_build_dirs(build_dir_base=build_dir_base, parallel=parallel_tests, setup_task_list=config['setup_actions'])
    else:
        task_bucket_info = check_build_dirs(build_dir_base=build_dir_base, parallel=parallel_tests)

    # Prepare to run the tasks in the list
    for test_num in range(len(config['test_tasks'])):
        test = config['test_tasks'][test_num]
        res = re.search("python", test)
        if (res and not python):
            continue
        elif (not res and python):
            continue

        build_dir_number = test_num % parallel_tests
        logging.debug("Prepping test [{}] as build number {}: {} ".format(test_num, build_dir_number, test))
        task_bucket_info[build_dir_number]['task_bucket'].append(test)

    logging.debug("task_bucket_info: {}".format(task_bucket_info))

    # Perform task operations in parallel across the build directories
    run_task_lists_in_parallel(label="tasks", task_bucket_info=task_bucket_info, run_func=run_task_list)


if __name__ == '__main__':
    main()
