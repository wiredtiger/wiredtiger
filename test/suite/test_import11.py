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
# Tests import using backup:export cursor and metadata_file import option.
#

import glob, os, random, shutil, string
import wttest
from wiredtiger import stat

# Shared base class used by import tests.
class test_import_base(wttest.WiredTigerTestCase):

    # Insert or update a key/value at the supplied timestamp.
    def update(self, uri, key, value, ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def delete(self, uri, key, ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor.set_key(key)
        self.assertEqual(0, cursor.remove())
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    # Verify the specified key/value is visible at the supplied timestamp.
    def check_record(self, uri, key, value):
        cursor = self.session.open_cursor(uri)
        cursor.set_key(key)
        self.assertEqual(0, cursor.search())
        self.assertEqual(value, cursor.get_value())
        cursor.close()

    # Verify a range of records/timestamps.
    def check(self, uri, keys, values):
        for i in range(len(keys)):
            self.check_record(uri, keys[i], values[i])

    # Populate a database with N tables, each having M rows.
    def populate(self, ntables, nrows):
        for table in range(0, ntables):
            uri = 'table:test_import_{}'.format(
                ''.join(random.choice(string.ascii_letters) for i in range(10)))
            self.session.create(uri, 'key_format=i,value_format=S')
            cursor = self.session.open_cursor(uri)
            for key in range(0, nrows):
                cursor[key] = 'value_{}_{}'.format(table, key)
            cursor.close()

    # Copy a file from a source directory to a destination directory.
    def copy_files_by_pattern(self, file_pattern, src_dir, dest_dir):
        path_pattern = os.path.join(src_dir, file_pattern)
        for file in glob.glob(path_pattern):
            if not file.endswith('WiredTiger.wt') and not file.endswith('WiredTigerHS.wt'):
                shutil.copy(file, dest_dir)

    def copy_files(self, src_dir, dest_dir):
        self.copy_files_by_pattern('*.wt', src_dir, dest_dir)

# test_import11
@wttest.skip_for_hook("tiered", "Fails with tiered storage")
class test_import11(test_import_base):
    uri_a = 'table:test_a'
    uri_b = 'table:test_b'

    nrows = 100
    ntables = 10
    keys = [b'1', b'2', b'3', b'4', b'5', b'6']
    values = [b'\x01\x02aaa\x03\x04', b'\x01\x02bbb\x03\x04', b'\x01\x02ccc\x03\x04',
              b'\x01\x02ddd\x03\x04', b'\x01\x02eee\x03\x04', b'\x01\x02fff\x03\x04']
    ts = [10*k for k in range(1, len(keys)+1)]
    create_config = 'allocation_size=512,key_format=u,value_format=u'
    conn_config = 'statistics=(all)'

    def create_and_populate(self, uri):
        self.session.create(uri, self.create_config)

        # Add data and perform a checkpoint.
        min_idx = 0
        max_idx = len(self.keys) // 3
        for i in range(min_idx, max_idx):
            self.update(uri, self.keys[i], self.values[i], self.ts[i])
        self.session.checkpoint()

        # Add more data and checkpoint again.
        min_idx = max_idx
        max_idx = 2*len(self.keys) // 3
        for i in range(min_idx, max_idx):
            self.update(uri, self.keys[i], self.values[i], self.ts[i])
        self.session.checkpoint()

        return max_idx

    def test_file_import(self):
        # Create and populate two tables
        max_idx = self.create_and_populate(self.uri_a)
        max_idx = self.create_and_populate(self.uri_b)

        # Create backup export cursor.
        c = self.session.open_cursor('backup:export', None, None)

        # Copy WiredTiger.export file.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        shutil.copy('WiredTiger.export', 'IMPORT_DB')

        # Reopen connection in the imported folder.
        self.reopen_conn(newdir)

        # Make a bunch of files and fill them with data.
        self.populate(self.ntables, self.nrows)
        self.session.checkpoint()

        # Bring forward the oldest to be past or equal to the timestamps we'll be importing.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.ts[max_idx]))

        # Copy over the datafiles for the object we want to import.
        self.copy_files('.', newdir)

        import_config = 'import=(enabled,repair=false,metadata_file="WiredTiger.export")'

        # Import the files.
        self.session.create(self.uri_a, import_config)
        self.session.checkpoint()

        # Check the number of files imported after doing an import operation.
        files_imported_prev = self.get_stat(stat.conn.session_table_create_import_success)
        self.assertTrue(files_imported_prev == 1)

        self.session.create(self.uri_b, import_config)
        self.session.checkpoint()

        # Check the number of files imported has increased after doing another import operation.
        files_imported = self.get_stat(stat.conn.session_table_create_import_success)
        self.assertTrue(files_imported == files_imported_prev + 1)

        # Remove WiredTiger.export file.
        export_file_path = os.path.join('IMPORT_DB', 'WiredTiger.export')
        os.remove(export_file_path)

        # Check that the previously inserted values survived the import.
        self.check(self.uri_a, self.keys[:max_idx], self.values[:max_idx])
        self.check(self.uri_b, self.keys[:max_idx], self.values[:max_idx])

        # Add some data and check that the table operates as usual after importing.
        min_idx = max_idx
        max_idx = len(self.keys)
        for i in range(min_idx, max_idx):
            self.update(self.uri_a, self.keys[i], self.values[i], self.ts[i])
        self.check(self.uri_a, self.keys, self.values)

        # Perform a checkpoint.
        self.session.checkpoint()
