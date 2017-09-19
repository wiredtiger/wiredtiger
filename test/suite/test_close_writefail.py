#!/usr/bin/env python
#
# Public Domain 2014-2017 MongoDB, Inc.
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

import os, shutil
import wiredtiger, wttest

class test_close_writefail(wttest.WiredTigerTestCase):
    '''Test closing tables when writes fail'''

    conn_config = 'log=(enabled)'

    def setUp(self):
        if os.name != 'posix' or os.uname()[0] != 'Linux':
            self.skipTest('Linux-specific test skipped on ' + os.name)
        super(test_close_writefail, self).setUp()

    def create_table(self, uri):
        self.session.create(uri, 'key_format=S,value_format=S')
        return self.session.open_cursor(uri)

    def test_close_writefail(self):
        '''Test closing multiple tables'''
        basename = 'writefail'
        baseuri = 'file:' + basename
        c1 = self.create_table(baseuri + '01.wt')
        c2 = self.create_table(baseuri + '02.wt')

        self.session.begin_transaction()
        c1['key'] = 'value'
        c2['key'] = 'value'
        self.session.commit_transaction()

        # Simulate a write failure by close the file descriptor for the second
        # table out from underneath WiredTiger.
        # This is Linux-specific code to figure out the file descriptor.
        for f in os.listdir('/proc/self/fd'):
            try:
                if os.readlink('/proc/self/fd/' + f).endswith(basename + '02.wt'):
                    os.close(int(f))
            except OSError:
                pass

        # expect an error and error messages, so turn off stderr checking.
        with self.expectedStderrPattern(''):
            try:
                self.close_conn()
            except wiredtiger.WiredTigerError:
                self.conn = None

        # Make a backup for forensics in case something goes wrong.
        backup_dir = 'BACKUP'
        shutil.rmtree(backup_dir, ignore_errors=True)
        shutil.copytree('.', backup_dir, lambda src, names: (n for n in names if n != backup_dir))

        self.open_conn()

        c1 = self.session.open_cursor(baseuri + '01.wt')
        c2 = self.session.open_cursor(baseuri + '02.wt')
        self.assertEqual(list(c1), list(c2))

if __name__ == '__main__':
    wttest.run()
