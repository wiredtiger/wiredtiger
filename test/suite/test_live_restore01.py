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

import os
import wiredtiger, wttest
from helper import copy_wiredtiger_home
import glob
import shutil

# test_live_restore01.py
# Test live restore compatibility with various other connection options.
class test_live_restore01(wttest.WiredTigerTestCase):

    def expect_success(self, config_str):
        self.open_conn("DEST", config=config_str)
        self.close_conn()

        # Clean out the destination. Subsequent live_restore opens will expect it to contain nothing.
        shutil.rmtree("DEST")
        os.mkdir("DEST")

    def expect_failure(self, config_str, expected_error):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.open_conn("DEST", config=config_str), expected_error)

        # No need to clean up the destination as WiredTiger failed to open.


    def test_live_restore01(self):
        # Close the default connection.
        self.close_conn()

        copy_wiredtiger_home(self, '.', "SOURCE")
        # Remove everything but SOURCE / stderr / stdout.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

        os.mkdir("DEST")

        # Test that live restore connection will fail on windows.
        if os.name == 'nt':
            self.expect_failure("live_restore=(enabled=true,path=SOURCE)", "/Live restore is not supported on Windows/")
            return

        # Open a valid connection.
        self.expect_success("live_restore=(enabled=true,path=SOURCE)")

        # Specify an in memory connection with live restore.
        self.expect_failure("in_memory=true,live_restore=(enabled=true,path=SOURCE)", "/Live restore is not compatible with an in-memory connection/")

        # Specify an in memory connection with live restore not enabled.
        self.expect_success("in_memory=true,live_restore=(enabled=false,path=SOURCE)")

        # Specify an empty path string.
        self.expect_failure("live_restore=(enabled=true,path=\"\")", "/No such file or directory/")

        # Specify a non existant path.
        self.expect_failure("live_restore=(enabled=true,path=\"fake.fake.fake\")", "/fake.fake.fake/")

        # Specify the max number of threads
        self.expect_success("live_restore=(enabled=true,path=SOURCE,threads_max=12)")

        # Specify one too many threads.
        self.expect_failure("live_restore=(enabled=true,path=SOURCE,threads_max=13)", "/Value too large for key/")

        # Specify the minimum allowed number of threads.
        self.expect_success("live_restore=(enabled=true,path=SOURCE,threads_max=0)")
