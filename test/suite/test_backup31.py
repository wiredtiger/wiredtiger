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
import shutil
from wtbackup import backup_base
import wiredtiger
from wtdataset import SimpleDataSet

# test_backup31.py
#    Test the exclude_target backup configuration option.
class test_backup31(backup_base):
    dir='backup.dir'

    def populate_data(self, nrows=100):
        self.uri_keep = 'table:keep_table'
        self.uri_exclude1 = 'table:exclude_table1'
        self.uri_exclude2 = 'table:exclude_table2'

        SimpleDataSet(self, self.uri_keep, nrows).populate()
        SimpleDataSet(self, self.uri_exclude1, nrows).populate()
        SimpleDataSet(self, self.uri_exclude2, nrows).populate()
        self.session.checkpoint()

    def get_backup_files(self, config):
        cursor = self.session.open_cursor('backup:', None, config)
        files = []
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            files.append(cursor.get_key())
        cursor.close()
        return files

    # Test excluding a single table.
    def test_exclude_single(self):
        self.populate_data()
        config = 'exclude_target=("table:exclude_table1")'
        files = self.get_backup_files(config)

        self.assertTrue('keep_table.wt' in files)
        self.assertFalse('exclude_table1.wt' in files)
        self.assertTrue('exclude_table2.wt' in files)

    # Test excluding multiple tables.
    def test_exclude_multiple(self):
        self.populate_data()
        config = 'exclude_target=("table:exclude_table1","table:exclude_table2")'
        files = self.get_backup_files(config)

        self.assertTrue('keep_table.wt' in files)
        self.assertFalse('exclude_table1.wt' in files)
        self.assertFalse('exclude_table2.wt' in files)

    # Test excluding via file: URI.
    def test_exclude_file(self):
        self.populate_data()
        config = 'exclude_target=("file:exclude_table1.wt")'
        files = self.get_backup_files(config)

        self.assertTrue('keep_table.wt' in files)
        self.assertFalse('exclude_table1.wt' in files)
        self.assertTrue('exclude_table2.wt' in files)

    # Test conflict with target configuration.
    def test_exclude_conflict_target(self):
        self.populate_data()
        config = 'target=("table:keep_table"),exclude_target=("table:exclude_table1")'
        msg = '/mutually exclusive/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor('backup:', None, config), msg)

    # Test conflict with incremental configuration.
    def test_exclude_conflict_incremental(self):
        self.populate_data()
        config = 'incremental=(enabled=true,this_id="ID0"),exclude_target=("table:exclude_table1")'
        msg = '/incompatible with exclude_target/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor('backup:', None, config), msg)

    # Test invalid prefix in exclude_target.
    def test_exclude_invalid_prefix(self):
        self.populate_data()
        config = 'exclude_target=("invalid_uri_no_prefix")'
        msg = '/missing prefix/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor('backup:', None, config), msg)

    # Test that excluded tables are missing from backup metadata content.
    def test_exclude_metadata_content(self):
        self.populate_data()

        config = 'exclude_target=("table:exclude_table1")'
        cursor = self.session.open_cursor('backup:', None, config)

        meta_file = os.path.join(self.home, 'WiredTiger.backup')

        # Check if the file exists and its content
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as f:
                content = f.read()
                self.assertTrue('table:keep_table' in content)
                self.assertFalse('table:exclude_table1' in content)
                self.assertTrue('table:exclude_table2' in content)

        cursor.close()

    # Test that we can open the backup database and read the kept table,
    # and that the excluded table is truly gone.
    def test_exclude_restore_and_read(self):
        nrows = 100
        self.populate_data(nrows)

        # 1. Perform backup with exclusion
        backup_dir = os.path.join(self.home, self.dir)
        shutil.rmtree(backup_dir, ignore_errors=True)
        os.mkdir(backup_dir)

        config = 'exclude_target=("table:exclude_table1")'
        cursor = self.session.open_cursor('backup:', None, config)
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            filename = cursor.get_key()
            shutil.copy(os.path.join(self.home, filename), os.path.join(backup_dir, filename))
        cursor.close()

        # 2. Open the backup directory as a new database.
        # Use a separate connection to avoid interference.
        backup_conn = wiredtiger.wiredtiger_open(backup_dir, "create")
        try:
            backup_session = backup_conn.open_session()

            # 3. Verify kept table exists and has exact data
            cursor_keep = backup_session.open_cursor(self.uri_keep, None, None)
            count = 0
            while cursor_keep.next() == 0:
                key = cursor_keep.get_key()
                val = cursor_keep.get_value()
                # SimpleDataSet uses numeric strings as keys.
                self.assertEqual(int(key), count + 1)
                count += 1
            self.assertEqual(count, nrows)
            cursor_keep.close()

            # 4. Verify excluded table does NOT exist (should throw ENOENT)
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: backup_session.open_cursor(self.uri_exclude1, None, None))

            # 5. Verify the other table that wasn't excluded also exists
            cursor_exclude2 = backup_session.open_cursor(self.uri_exclude2, None, None)
            cursor_exclude2.close()
        finally:
            backup_conn.close()
