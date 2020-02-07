#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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

import wiredtiger, wttest
import os
from suite_subprocess import suite_subprocess

# test_backup13.py
# Test error using an incremental cursor on a full backup cursor without a source.
class test_backup13(wttest.WiredTigerTestCase, suite_subprocess):
    dir='backup.dir'                    # Backup directory name
    logmax="100K"
    uri="table:test"
    nops=100

    pfx = 'test_backup'

    # Create a large cache, otherwise this test runs quite slowly.
    def conn_config(self):
        return 'cache_size=1G,log=(enabled,file_max=%s)' % self.logmax

    def add_data(self):
        self.session.create(self.uri, "key_format=S,value_format=S")

        # Insert small amounts of data at a time stopping after we
        # cross into log file 2.
        c = self.session.open_cursor(self.uri)
        for i in range(0, self.nops):
            num = i + self.nops
            key = 'key' + str(num)
            val = 'value' + str(num)
            c[key] = val
        self.session.checkpoint()
        c.close()

    def test_backup13(self):

        self.add_data()

        # Open up the backup cursor. This causes a new log file to be created.
        # That log file is not part of the list returned. This is a full backup
        # primary cursor with incremental configured.
        os.mkdir(self.dir)
        # Note, this first backup is actually done before a checkpoint is taken.
        #
        config = 'incremental=(enabled,this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)

        # Now try to open an incremental cursor off this backup cursor. It should
        # return EINVAL.
        ret = bkup_c.next()
        newfile = bkup_c.get_key()
        config = 'incremental=(file=' + newfile + ')'
        msg = '/known source identifier/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(None, bkup_c, config), msg)

        bkup_c.close()
        return

if __name__ == '__main__':
    wttest.run()
