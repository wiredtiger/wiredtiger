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
#
# test_config12.py
#   Test the debug configuration setting for config validation.
#

import re
import wttest
from contextlib import contextmanager

class test_config12(wttest.WiredTigerTestCase):

    @contextmanager
    def expect_verbose(self, config, patterns, expect_output = True):
        # Clean the stdout resource before yielding the context to the execution block. We only want to
        # capture the verbose output of the using context (ignoring any previous output up to this point).
        self.cleanStdout()
        conn = self.wiredtiger_open(self.home, config)
        # Yield the connection resource to the execution context, allowing it to perform any necessary
        # operations on the connection (for generating the expected verbose output).
        yield conn
        # Read the contents of stdout to extract our verbose messages.
        output = self.readStdout(1000)
        # Split the output into their individual messages. We want validate the contents of each message
        # to ensure we've only generated verbose messages for the expected categories.
        verbose_messages = output.splitlines()

        if expect_output:
            self.assertGreater(len(verbose_messages), 0)
        else:
            self.assertEqual(len(verbose_messages), 0)

        if len(output) >= 1000:
            # If we've read the maximum number of characters, its likely that the last line is truncated ('...'). In this
            # case, trim the last message as we can't parse it.
            verbose_messages = verbose_messages[:-1]

        # Test the contents of each verbose message, ensuring it satisfies the expected pattern.
        verb_pattern = re.compile('|'.join(patterns))
        for line in verbose_messages:
            self.assertTrue(verb_pattern.search(line) != None, 'Unexpected verbose message: ' + line)

        # Close the connection resource and clean up the contents of the stdout file, flushing out the
        # verbose output that occurred during the execution of this context.
        conn.close()
        self.cleanStdout()

    def test_config12(self):
        self.conn.close()
        # Test invalid config with debug mode enabled, warning messages expected.
        with self.expect_verbose('debug_mode=(configuration=true)', ['config checkpoint target=.*is less than eviction dirty target=.*',
                                                                     'config eviction updates target=.*is less than DBL_EPSILON=.*',
                                                                     'config eviction updates trigger=.*is less than DBL_EPSILON=.*']):
            pass
        # Disable debug mode, expect no warning messages.
        with self.expect_verbose('debug_mode=(configuration=false)', [], False):
            pass
