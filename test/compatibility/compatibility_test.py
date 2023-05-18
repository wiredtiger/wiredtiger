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

import inspect, os, shutil, sys

# If unittest2 is available, use it in preference to (the old) unittest
try:
    import unittest2 as unittest
except ImportError:
    import unittest

class CompatibilityTestCase(unittest.TestCase):
    '''
    The base class for running compatibility tests.
    '''

    def pr(self, s):
        '''
        Print diagnostic output.
        '''
        print(s)

    def branch_path(self, branch):
        '''
        Get path to a branch.
        '''
        return os.path.abspath(os.path.join('..', branch))

    def module_file(self):
        '''
        Get the (base) file name for this test.
        '''
        return os.path.basename(inspect.getfile(self.__class__))
    
    def module_name(self):
        '''
        Get the module name for this test.
        '''
        return self.module_file().replace('.py', '')

    def prepare_wt_branch(self, branch):
        '''
        Check out and build another WT branch.
        '''

        # Clone the repository and check out the correct branch
        path = self.branch_path(branch)
        if os.path.exists(path):
            self.pr(f'Branch {branch} is already cloned')
            r = os.system(f'git -C "{path}" pull')
            self.assertEqual(r, 0)
        else:
            self.pr(f'Cloning branch {branch}')
            r = os.system(f'git clone git@github.com:wiredtiger/wiredtiger.git "{path}" -b {branch}')
            self.assertEqual(r, 0)

        # Build
        build_path = os.path.join(path, 'build')
        if not os.path.exists(os.path.join(build_path, 'build.ninja')):
            os.mkdir(build_path)
            # Disable WT_STANDALONE_BUILD, because it is not compatible with banches 6.0 and
            # earlier.
            cmake_args = '-DWT_STANDALONE_BUILD=0 -DENABLE_PYTHON=1'
            r = os.system(f'cd "{build_path}" && cmake {cmake_args} -G Ninja ../.')
            self.assertEqual(r, 0)

        self.pr(f'Building {path}')
        r = os.system(f'cd "{build_path}" && ninja')
        self.assertEqual(r, 0)

    def run_method_on_branch(self, branch, method):
        '''
        Run a method on a branch.
        '''
        cwd = os.getcwd()
        branch_path = self.branch_path(branch)
        build_python_path = os.path.join(branch_path, 'build', 'lang', 'python')

        class_name = self.__class__.__name__
        module_dir = os.path.join(branch_path, 'test', 'compatibility')
        module_name = self.module_name()
        
        os.makedirs(module_dir, exist_ok=True)
        shutil.copy(__file__, os.path.join(module_dir, os.path.basename(__file__)))
        shutil.copy(inspect.getfile(self.__class__), os.path.join(module_dir, self.module_file()))

        if not method.endswith(')'):
            method += '()'
        method = method.replace('\'', '"')

        commands = f'import os, sys;'
        commands += f'sys.path.insert(1, "{build_python_path}");'
        commands += f'import {module_name};'
        commands += f'os.chdir("{cwd}");'
        commands += f'sys.exit({module_name}.{class_name}().{method})'

        r = os.system(f'cd \'{module_dir}\' && python3 -c \'{commands}\'')
        self.assertEqual(r, 0)
    
    def setUp(self):
        '''
        Setup the test.
        '''
        self._start_dir = os.getcwd()

        self._test_dir = os.path.join('compatibility-test', self.module_name())
        shutil.rmtree(self._test_dir, ignore_errors=True)

        if os.path.exists(self._test_dir):
            raise Exception(self._test_dir + ": cannot remove directory")
        os.makedirs(self._test_dir)
        os.chdir(self._test_dir)

    def tearDown(self):
        '''
        Tear down the test.
        '''
        os.chdir(self._start_dir)


def run_suite(suite):
    '''
    Run the test suite.
    '''
    try:
        result = unittest.TextTestRunner(verbosity=1, resultclass=None).run(suite)
        return result
    except BaseException as e:
        # This should not happen for regular test errors, unittest should catch everything
        print("[pid:{}]: ERROR: running test: {}".format(os.getpid(), e))
        raise e

def run(name='__main__'):
    '''
    Run the test.
    '''
    result = run_suite(unittest.TestLoader().loadTestsFromName(name))
    sys.exit(0 if result.wasSuccessful() else 1)
