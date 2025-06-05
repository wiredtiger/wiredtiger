#!/usr/bin/env python3

import argparse
import csv
import glob
import os
import json
import subprocess
import sys
import re
from collections import defaultdict
from smart_test import last_commit_from_dev

sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'py_utility'))
import test_util

# diff_smart_test.py
# Using the code coverage data from the coverage-report-per-test task, this script:
# - Takes your git diff since branching from develop and determines which functions have been modified
# - Determines which tests most effectively cover those functions
# - Runs those tests for you

# To run simply call
#     `./diff_smart_test.py`
# The script needs to format the coverage data, so I've pre-processed it and uploaded the data to GDrive. You can generate your own data with
#     `./diff_smart_test.py --generate-coverage`
# and then update the GDrive file. To generate the coverage data you need to have the metrixpp.csv file and untarred coverage_data in the same 
# directory as this script. You also need to manually modify the metrixpp.csv file to change the "line start" and "line end" columns to be "line_start" and 
# "line_end" because spaces break the csv parsing. This is currently performed in the coverage-report-per-test task, and the 
# resulting funcs_covered_by_test.json file can be uploaded to GDrive.


# TODO
# - If we call the --generate-coverage command as part of coverage-report-per-test I don't need to manually download the files
#     - Also need to fix up those metrixpp.csv line_(start|end) headers
# - Look into algo: Doesn't seem great for __wt_txn_commit
#     - Weight on percentage of function covered
#     - Weight on runtime
# - Integrate into smart_test.py
# - Add a README
# - Find a longer term solution than GDrive
# - The coverage data only covers the src/ folder. It'd be useful to detect that a specific file is modified and always run that test
# - Testing can't handle when a brand new function is added. The coverage data has no way to know which tests will reach it.
# - Add logic to automatically update code_coverage_per_test_config.json when new tests are added

COVERAGE_DATA_FILE = "funcs_covered_by_test.json"
GDRIVE_DOWNLOAD_URL = "https://drive.google.com/uc?export=download&id=1zxSeNT4o-YAXtJd8D7A5xyV68FLVgH9J"

# Using the metrixpp.csv file and coverage data from a waterfall coverage-per-test patch, build a mapping 
# from a line of code to the function it belongs to.
def build_lines_to_func_mapping():
    files_line_mappings = defaultdict(list) # {file_name -> {(start line, end line) -> function}}

    # TODO - This requires manual setup. You need to download the metrixpp.csv file from waterfall and then
    # manually modify it to change the "line start" and "line end" columns to be "line_start" and "line_end".
    # You also need to download the coverage_data.tar.gz file from waterfall and extract it to the coverage_data directory
    with open('metrixpp.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        headers = next(reader)

        file = headers.index("file")
        type = headers.index("type")
        region = headers.index("region")
        line_start = headers.index("line_start")
        line_end = headers.index("line_end")

        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if row[type] == "function":
                func_name = row[region]
                start = int(row[line_start])
                end = int(row[line_end])
                file_name = row[file]
                file_name = "src" + file_name[1:] # hack - one file uses ./txn/txn.c and the other one src/txn/txn.c

                files_line_mappings[file_name].append( (start, end, func_name) )

    return files_line_mappings

def build_test_to_covered_funcs_mapping(files_line_mappings):
    funcs_covered_by_test = defaultdict(set)

    num_folders = len(glob.glob("coverage_data/*"))

    i = 0
    for folder in glob.glob("coverage_data/*"):
        i += 1
        print(f"\rProcessing folder {i} of {num_folders}", end='', flush=True)
        with open(f"{folder}/task_info.json", 'r') as file:
            test_name = json.load(file)['task']

        with open(f"{folder}/full_coverage_report.json", 'r') as file:
            data = json.load(file)
            for f in data['files']:
                file_name = f['file']
                lines = f['lines']
                for line in lines:
                    if line['count'] != 0:
                        line_number = int(line['line_number'])
                        function_line_mapping = files_line_mappings[file_name]

                        # FIXME - super naiive lookup. Runtime is only a 3 mins though, so not the end of the world
                        for (start, end, func) in function_line_mapping:
                            if start <= line_number <= end:
                                funcs_covered_by_test[test_name].add(func)
                                break

    return funcs_covered_by_test


# Generate a list of tests to run sorted by how effectively they cover the functions modified in our diff
def function_coverage_score(funcs, funcs_covered_by_test, verbose=False):
    scored_tests = defaultdict(int)
    for (test, covered_funcs) in funcs_covered_by_test.items():

        overlap = funcs.intersection(covered_funcs)
        if not overlap:
            continue

        # TODO - This is a trivial scoring heuristic. If a test touches 15 functions and one of them is a
        # function we want to cover, then increment the score by 1/15
        for overlapping_func in overlap:
            scored_tests[test] += (1/len(covered_funcs))

    sorted_scored_tests = []

    for (test, score) in scored_tests.items():
        sorted_scored_tests.append((score, test))
    sorted_scored_tests.sort(reverse=True)

    if verbose:
        print("DBG: Scored tests:")
        for (score, test) in sorted_scored_tests:
            print(f"    {score}: {test}")

    # TODO - Take runtime into account instead of taking the first 50 tests
    tests_to_run = [t for t in map(lambda x: x[1], sorted_scored_tests[:50])]

    return tests_to_run

# Naiive check that takes the git diff and returns a list all functions in the src/ folder that have been modified.
def find_functions_modified_in_git_diff():
    commit = last_commit_from_dev()

    modified_functions = set()

    # FIXME - super naiive grepping for wiredtiger function names. It assumes any line following a regex match belongs
    # to the function the regex matched on
    # Doesn't handle:
    # - Python functions
    # - Functions not following the WT standard of starting with __
    # - Probably a tonne of other edge cases
    # - When we rename a function the new name is on a later line, so this will report the modified function using
    #   its new name and that won't have any coverage mapping. Think about this case and how we test renamed functions
    diff = subprocess.run(f"git diff {commit}", shell=True, capture_output=True, text=True).stdout.strip().splitlines()
    for line in diff:
        if m := re.match(r"\s+\* (__[a-zA-Z_]+) --", line):
            modified_functions.add(m[1])

        elif m:= re.match(r"@@ .* @@ (__[a-zA-Z_]+)\(.*", line):
            modified_functions.add(m[1])

        elif m:= re.match(r" (__[a-zA-Z_]+)\(", line):
            modified_functions.add(m[1])

    return modified_functions

# Create the COVERAGE_DATA_FILE json file that we use to determine which tests to run.
# This file should be uploaded to GDrive so other users can download it instead of spending 3 minutes recomputing it.
def generate_coverage_data():
    print("Generating coverage data to be uploaded to GDrive")

    files_line_mappings = build_lines_to_func_mapping()
    print("Parsed lines to functions mapping")

    funcs_covered_by_test = build_test_to_covered_funcs_mapping(files_line_mappings)
    print("Built test -> covered_functions map")

    writable_dict = {k: list(v) for k, v in funcs_covered_by_test.items()}
    with open(COVERAGE_DATA_FILE, 'w') as file:
        json.dump(writable_dict, file, indent=4)

    print(f"Dumped coverage data to {COVERAGE_DATA_FILE}")
    print("This should be uploaded to GDrive")

def main(generate_coverage, dry_run, verbose):

    if generate_coverage:
        generate_coverage_data()
        exit(0)

    modified_functions = find_functions_modified_in_git_diff()

    # TODO - this should update when the modified data of the GDrive file is more recent than the local file
    # TODO - Move this out of GDrive and into a long term solution that supportd uploading
    if not os.path.exists(COVERAGE_DATA_FILE):
        print("Downloading coverage data")
        subprocess.run(f"curl -L -o {COVERAGE_DATA_FILE} '{GDRIVE_DOWNLOAD_URL}'", shell=True)

    with open(COVERAGE_DATA_FILE, 'r') as file:
        funcs_covered_by_test = json.load(file)
        # We can't write a dict to file if it contains sets, so we need to convert them to lists
        # This converts them back to sets on open
        for k, v in funcs_covered_by_test.items():
            funcs_covered_by_test[k] = set(v)

    tests_to_run = function_coverage_score(modified_functions, funcs_covered_by_test, verbose)

    # TODO - Look into merging similar tests. This loop runs each python test one by one.
    # The scenarios run in parallel, but we should merge all python tests into one test call, and all csuite tasks into one ctest call

    print("Running tests:")
    for t in tests_to_run:
        print(f"    {t}")

    if not dry_run:
        for test in tests_to_run:
            print(f"running test {test}")
            res = subprocess.run(test, shell=True, cwd=test_util.find_build_dir(), capture_output=True, text=True)
            if res.returncode != 0:
                print("Test failed!!!")
                print(res.stdout)
                sys.exit(res.returncode)

if __name__ == "__main__":
    # Always run this script from its containing directory. We have some
    # hardcoded paths that require it.
    working_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(working_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument("--generate-coverage", action="store_true", help="Generate the coverage data")
    parser.add_argument("--dry-run", action="store_true", help="List tests, don't run them")
    parser.add_argument("--verbose", action="store_true", help="Extra traces")
    args = parser.parse_args()

    main(args.generate_coverage, args.dry_run, args.verbose)