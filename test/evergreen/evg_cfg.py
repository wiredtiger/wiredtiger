#! /usr/bin/env python3

"""
This program provides an CLI interface to check and generate Evergreen configuration.
"""

import os
import sys
import re
import docopt

TEST_TYPES = ('make_check', 'csuite', 'all')
EVG_CFG_FILE = "test/evergreen.yml"
MAKE_SUBDIRS_FILE = "build_posix/Make.subdirs"
CSUITE_TEST_DIR = "test/csuite"

# This list of sub directories will be skipped from checking
# They are not expected to trigger any 'make check' testing.
make_check_subdir_skips = [
    "ext/collators/reverse",
    "ext/collators/revint",
    "ext/compressors/lz4",
    "ext/compressors/nop",
    "ext/compressors/snappy",
    "ext/compressors/zlib",
    "ext/compressors/zstd",
    "ext/datasources/helium",
    "ext/encryptors/nop",
    "ext/encryptors/rotn",
    "ext/extractors/csv",
    "ext/test/kvs_bdb",
    "ext/test/fail_fs",
    "api/leveldb",
    "lang/java",
    "test/utility",
    "examples/java",
    "test/csuite",  # csuite has its own set of Evergreen tasks, skip the checking here
    "test/syscall",
    "bench/workgen",
]

PROGNAME = os.path.basename(sys.argv[0])
DESCRIPTION = 'Evergreen configuration helper 0.1'
USAGE = """
Evergreen configuration helper.

Usage:
  {progname} check [-t <test_type>] [-v]
  {progname} generate [-t <test_type>] [-v]
  {progname} (-h | --help)

Options:
  -h --help     Show this screen.
  -t TEST_TYPE  The type of test to be checked/generated. 
  -v            Enable verbose logging.
  check         Check if any missing tests that should be added into Evergreen configuration. 
  generate      Generate Evergreen configuration for missing tests. 
""".format(progname=PROGNAME)

verbose = False

def debug(msg):
    """ A wrapper to print function with checking of verbose flag """

    if verbose is True:
        print(msg)


def find_tests_missing_evg_cfg(test_type, dirs, evg_cfg_file):
    """ 
    Check the list of 'make check' directories to find out those 
    that are missing from the Evergreen configuration file.

    The existing Evergreen configuration is expected to have
    included one task for each applicable 'make check' directory.
    Newly added 'make check' directories that involve real tests
    should be identified by this function.
    """

    if not dirs:
        sys.exit("\nNo %s directory is found ..." % test_type)

    assert os.path.isfile(evg_cfg_file), "'%s' does not exist" % evg_cfg_file

    with open(evg_cfg_file, 'r') as f:
        evg_cfg = f.readlines()

    missing_tests = []
    for dir in dirs: 
        # Figure out the Evergreen task name from the directory name

        if test_type == 'make_check':
            # The Evergreen task name for each 'make check' test is worked out from the directory name
            # E.g. for 'make check' directory 'test/cursor_order', the corresponding Evergreen task name
            # will be 'cursor-order-test'.

            dir_wo_test_prefix = dir[len("test/"):] if dir.startswith("test/") else dir
            evg_task_name = dir_wo_test_prefix.replace('/', '-').replace('_', '-') + '-test'
            debug("Evergreen task name for make check directory '%s' is: %s" % (dir, evg_task_name))

        elif test_type == 'csuite':
            # The Evergreen task name for each 'csuite' test is worked out from the sub directory name
            # E.g. for 'test/csuite' sub directory 'wt3184_dup_index_collator', the corresponding Evergreen 
            # task name will be 'csuite-wt3184-dup-index-collator-test'.

            evg_task_name = 'csuite-' + dir.replace('_', '-') + '-test'
            debug("Evergreen task name for csuite sub directory '%s' is: %s" % (dir, evg_task_name))

        else:
            sys.exit("Unknow test_type '%s'" % test_type)

        # Check if the Evergreen task name exists in current Evergreen configuration
        if evg_task_name in str(evg_cfg):
            # Match found
            continue
        else:
            # Missing task/test found
            missing_tests.append(evg_task_name)
            print("Test '%s' (for directory %s) is missing in %s!" % (evg_task_name, dir, evg_cfg_file))

    return missing_tests


def check_make_check_tests(test_type):
    """ 
    Check to see if any make check tests are missing from the Evergreen configuration.
    Loop through the list of directories in 'Make.subdirs' file and skip a few known 
    directories that do not require any test.
    """

    assert os.path.isfile(MAKE_SUBDIRS_FILE), "'%s' does not exist" % MAKE_SUBDIRS_FILE

    subdirs = []
    with open(MAKE_SUBDIRS_FILE, 'r') as f:
        lines = f.readlines()
        for line in lines: 
            # Retrieve directory info from the 1st column
            subdirs.append(line.strip().split(" ")[0])

        # Remove comment and blank lines 
        subdirs = [d for d in subdirs if d not in ('#', '.', '')]
        debug("\nThe list of directories captured from 'Make.subdirs' file:\n %s" % subdirs)

    # Remove directories in the skip list
    make_check_dirs = [d for d in subdirs if d not in make_check_subdir_skips]
    debug("\nThe list of 'make check' directories that should be included in Evergreen configuration:\n %s" % make_check_dirs)

    missing_tests = find_tests_missing_evg_cfg(test_type, make_check_dirs, EVG_CFG_FILE)

    return missing_tests


def check_csuite_tests(test_type):
    """
    Check to see if any csuite tests are missing from the Evergreen configuration.
    Loop through the list of sub directories under test/csuite/ and skip those WT_TEST.* directories.
    """

    assert os.path.isdir(CSUITE_TEST_DIR), "'%s' does not exist" % CSUITE_TEST_DIR

    # Retrieve all sub directories under 'test/csuite' directory
    subdirs = [x[1] for x in os.walk(CSUITE_TEST_DIR)][0]

    # Remove directories with name starting with 'WT_TEST' or '.'
    regex = re.compile(r'^(WT_TEST|\.)')
    csuite_dirs = [d for d in subdirs if not regex.search(d)]
    debug("The list of 'csuite' directories that should be included in Evergreen configuration:\n %s" % csuite_dirs)

    missing_tests = find_tests_missing_evg_cfg(test_type, csuite_dirs, EVG_CFG_FILE)
    
def evg_cfg(action, test_type):
    """ The main entry function calls different functions based on action type """

    assert test_type in TEST_TYPES, "Unknown test type '%s'" % test_type

    if action == 'check':
        if test_type == 'make_check': 
            check_make_check_tests(test_type)
        elif test_type == 'csuite':
            check_csuite_tests(test_type)
        else: # 'all'
            check_make_check_tests(test_type)
            check_csuite_tests(test_type)
    elif action == 'generate':
        pass
    else:
        sys.exit("Unknown action type '%s'" % action)


if __name__ == '__main__':

    args = docopt.docopt(USAGE, version=DESCRIPTION)

    verbose = args['-v']
    debug('\nargs:%s' % args)

    action = None
    if args['check']:
        action = 'check'
    elif args['generate']:
        action = 'generate'
    assert action in ('check', 'generate')

    test_type = args.get('-t', None)
    # If test type is not provided, assuming 'all' types need to be checked
    if test_type is None:
        test_type = 'all'

    evg_cfg(action, test_type)
