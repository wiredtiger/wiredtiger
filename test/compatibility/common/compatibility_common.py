#!/usr/bin/env python
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

import os, sys
from compatibility_config import *

# Remember the top-level test directory and the top-level project directory.
TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
DIST_TOP_DIR = os.path.abspath(os.path.join(TEST_DIR, '..'))

# Set up Python import paths, so that we can find the core dependencies.
sys.path.insert(1, os.path.join(TEST_DIR, 'py_utility'))

# Now set up the rest of the paths, so that we can access our third-party dependencies.
# IMPORTANT: Don't import wiredtiger until you get to the actual test, so that you don't
# accidentally import the wrong version of the library.
import test_util
test_util.setup_3rdparty_paths()

def is_branch_supported(branch):
    '''
    Check whether the branch is supported by the compatibility test.
    '''
    return branch == 'this' or branch == 'develop' or branch.startswith('mongodb-')

def is_branch_order_asc(a, b):
    '''
    Compare two branches for whether the two branches are in ascending order with respect their
    version numbers; i.e., whether branch "b" is newer than branch "a".
    '''
    if not is_branch_supported(a):
        raise Exception('Unsupported branch "%s"' % a)
    if not is_branch_supported(b):
        raise Exception('Unsupported branch "%s"' % b)
    if a == b:
        return False
    
    # Assume that 'this' branch is newer than 'develop'.
    if a == 'this':
        return False
    if b == 'this':
        return True
    if a == 'develop':
        return False
    if b == 'develop':
        return True
    return b > a

def branch_path(branch):
    '''
    Get path to a branch.
    '''
    if branch == 'this':
        return DIST_TOP_DIR
    return os.path.abspath(os.path.join(DIST_TOP_DIR, BRANCHES_DIR, branch))

def system(command):
    '''
    Run a command, fail if the command execution failed.
    '''
    r = os.system(command)
    if r != 0:
        raise Exception('Command \'%s\' failed with code %d.' % (command, r))

def prepare_branch(branch, standalone=True):
    '''
    Check out and build a WiredTiger branch.
    '''

    # If we are running the test on the current branch, make sure it is compiled. We assume that the
    # user has already run cmake.
    if branch == 'this':
        build_path = test_util.find_build_dir()
        print(f'Building {test_util.get_dist_top_dir()}')
        system(f'cd "{build_path}" && ninja')
        return

    # Clone the repository and check out the correct branch.
    path = branch_path(branch)
    if os.path.exists(path):
        print(f'Branch {branch} is already cloned')
        system(f'git -C "{path}" pull')
    else:
        source = 'git@github.com:wiredtiger/wiredtiger.git'
        print(f'Cloning branch {branch}')
        system(f'git clone "{source}" "{path}" -b {branch}')

    # Build
    build_path = os.path.join(path, 'build')
    # Note: This build code works only on branches 6.0 and newer. We will need to update it to
    # support older branches, which use autoconf.
    if not os.path.exists(os.path.join(build_path, 'build.ninja')):
        os.mkdir(build_path)
        # Disable WT_STANDALONE_BUILD, because it is not compatible with branches 6.0 and
        # earlier.
        cmake_args = '-DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_v4_clang.cmake'
        cmake_args += ' -DENABLE_PYTHON=1'
        if not standalone:
            cmake_args += ' -DWT_STANDALONE_BUILD=0'
        system(f'cd "{build_path}" && cmake {cmake_args} -G Ninja ../.')

    print(f'Building {path}')
    system(f'cd "{build_path}" && ninja')
