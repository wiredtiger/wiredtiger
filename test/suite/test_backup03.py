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

import glob
import os
import shutil
import string
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from helper import compare_files,\
    complex_populate, complex_populate_lsm, simple_populate

# test_backup03.py
#    Utilities: wt backup
# Test cursor backup with target URIs
class test_backup(wttest.WiredTigerTestCase, suite_subprocess):
    dir='backup.dir'            # Backup directory name

    pfx = 'test_backup'
    objs = [
        ('table:' + pfx + '.1',  simple_populate, 100),
        ('lsm:' + pfx + '.2',  simple_populate, 50000),
        ('table:' + pfx + '.3', complex_populate, 100),
        ('table:' + pfx + '.4', complex_populate_lsm, 100),
    ]

    # Populate a set of objects.
    def populate(self):
        for i in self.objs:
            i[1](self, i[0], 'key_format=S', i[2])
        # Backup needs a checkpoint
        self.session.checkpoint(None)

    # Compare the original and backed-up files using the wt dump command.
    def compare(self, uri):
        self.runWt(['dump', uri], outfilename='orig')
        self.runWt(['-h', self.dir, 'dump', uri], outfilename='backup')
        compare_files(self, 'orig', 'backup')

    # Check that a URI doesn't exist, both the meta-data and the file names.
    def confirmPathDoesNotExist(self, uri):
        conn = wiredtiger.wiredtiger_open(self.dir)
        session = conn.open_session()
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: session.open_cursor(uri, None, None))
        conn.close()

        self.assertEqual(
            glob.glob(self.dir + '*' + uri.split(":")[1] + '*'), [],
            'confirmPathDoesNotExist: URI exists, file name matching \"' +
            uri.split(":")[1] + '\" found')

    # Backup a set of chosen tables/files using the wt backup command.
    def backup_table_cursor(self, l):
        # Remove any previous backup directories.
        shutil.rmtree(self.dir, True)
        os.mkdir(self.dir)

        # Build the target list.
        config = 'target=('
        for i in range(0, len(self.objs)):
            if i in l:
                config += '"' + self.objs[i][0] + '",'
        config += ')'

        # Open up the backup cursor, and copy the files.
        cursor = self.session.open_cursor('backup:', None, config)
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            #print 'Copy from: ' + cursor.get_key() + ' to ' + self.dir
            shutil.copy(cursor.get_key(), self.dir)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        cursor.close()

        # Confirm the objects we backed up exist, with correct contents.
        for i in range(0, len(self.objs)):
            if i in l:
                self.compare(self.objs[i][0])

        # Confirm the other objects don't exist.
        for i in range(0, len(self.objs)):
            if i not in l:
                self.confirmPathDoesNotExist(self.objs[i][0])

    # Test backup with targets
    def test_targets_groups(self):
        self.populate()
        self.backup_table_cursor([0,2])
        self.backup_table_cursor([1,3])
        self.backup_table_cursor([0,1,2])
        self.backup_table_cursor([0,1,2,3])

    def test_target_individual(self):
        self.populate()
        self.backup_table_cursor([0])
        self.backup_table_cursor([1])
        self.backup_table_cursor([2])
        self.backup_table_cursor([3])


if __name__ == '__main__':
    wttest.run()
