#!/usr/bin/env python
#
# Public Domain 2008-2013 WiredTiger, Inc.
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
# test_bug002.py
#       Regression tests.

import shutil, os
from helper import confirm_empty, key_populate, value_populate
from suite_subprocess import suite_subprocess
from wtscenario import multiply_scenarios, number_scenarios
import wiredtiger, wttest

# Regression tests.
class test_bulk_load_checkpoint(wttest.WiredTigerTestCase, suite_subprocess):
    types = [
        ('file', dict(uri='file:data')),
        ('table', dict(uri='table:data')),
    ]
    ckpt_type = [
        ('named', dict(ckpt_type='named')),
        ('unnamed', dict(ckpt_type='unnamed')),
    ]

    scenarios = number_scenarios(multiply_scenarios('.', types, ckpt_type))

    # Bulk-load handles return EBUSY to the checkpoint code, causing the
    # checkpoint call to find a handle anyway, and create fake checkpoint.
    # Named and unnamed checkpoint versions.
    def test_bulk_load_checkpoint(self):
        # Open a bulk cursor and insert a few records.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None, 'bulk')
        for i in range(1, 10):
            cursor.set_key(key_populate(cursor, i))
            cursor.set_value(value_populate(cursor, i))
            cursor.insert()

        # Checkpoint a few times (to test the drop code).
        for i in range(1, 5):
            if self.ckpt_type == 'named':
                self.session.checkpoint('name=myckpt')
            else:
                self.session.checkpoint()

        # Close the bulk cursor.
        cursor.close()

        # In the case of named checkpoints, verify they're still there,
        # reflecting an empty file.
        if self.ckpt_type == 'named':
            cursor = self.session.open_cursor(
                self.uri, None, 'checkpoint=myckpt')
            self.assertEquals(cursor.next(), wiredtiger.WT_NOTFOUND)
            cursor.close()


class test_backup_bulk(wttest.WiredTigerTestCase, suite_subprocess):
    types = [
        ('file', dict(uri='file:data')),
        ('table', dict(uri='table:data')),
    ]
    ckpt_type = [
        ('named', dict(ckpt_type='named')),
        ('none', dict(ckpt_type='none')),
        ('unnamed', dict(ckpt_type='unnamed')),
    ]
    session_type = [
        ('different', dict(session_type='different')),
        ('same', dict(session_type='same')),
    ]
    scenarios = number_scenarios(
        multiply_scenarios('.', types, ckpt_type, session_type))

    # Backup a set of chosen tables/files using the wt backup command.
    # The only files are bulk-load files, so they shouldn't be copied.
    def backup(self, session):
        # Create a backup directory.
        backupdir = 'backup.dir'
        os.mkdir(backupdir)

        # Open up the backup cursor, and copy the files.
        cursor = session.open_cursor('backup:', None, None)
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            shutil.copy(cursor.get_key(), backupdir)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        cursor.close()

        # Open the target directory, and confirm the object has no contents.
        conn = wiredtiger.wiredtiger_open(backupdir)
        session = conn.open_session()
        cursor = session.open_cursor(self.uri, None, None)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        conn.close()

    def test_backup_bulk(self):
        # Open a bulk cursor and insert a few records.
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None, 'bulk')
        for i in range(1, 10):
            cursor.set_key(key_populate(cursor, i))
            cursor.set_value(value_populate(cursor, i))
            cursor.insert()

        # Test without a checkpoint, with an unnamed checkpoint, with a named
        # checkpoint.
        if self.ckpt_type == 'named':
            self.session.checkpoint('name=myckpt')
        elif self.ckpt_type == 'unnamed':
            self.session.checkpoint()

        # Test with the same and different sessions than the bulk-get call,
        # test both the database handle and session handle caches.
        if self.session_type == 'same':
            self.backup(self.session)
        else:
            self.backup(self.conn.open_session())


if __name__ == '__main__':
    wttest.run()
