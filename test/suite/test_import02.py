#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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
# test_import02.py
# Error conditions when trying to import files.

import os, shutil
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_import02(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'
    scenarios = make_scenarios([
        ('no_metadata', dict(error_type='no_metadata')),
        ('empty_metadata', dict(error_type='empty_metadata'))
    ])

    def update(self, uri, key, value, commit_ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def copy_file(self, file_name, old_dir, new_dir):
        if os.path.isfile(file_name) and "WiredTiger.lock" not in file_name and \
            "Tmplog" not in file_name and "Preplog" not in file_name:
            shutil.copy(os.path.join(old_dir, file_name), new_dir)

    def test_file_import_no_metadata(self):
        original_db_file = 'original_db_file'
        uri = 'file:' + original_db_file

        create_config = 'allocation_size=512,key_format=u,log=(enabled=true),value_format=u'
        self.session.create(uri, create_config)

        key1 = b'1'
        key2 = b'2'
        value1 = b'\x01\x02aaa\x03\x04'
        value2 = b'\x01\x02bbb\x03\x04'

        # Add some data.
        self.update(uri, key1, value1, 10)
        self.update(uri, key2, value2, 20)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Contruct the config string.
        # Either don't provide the "file_metadata" key or provide empty configuration.
        if self.error_type == 'empty_metadata':
            import_config = 'import=(enabled,repair=false,file_metadata="")'
        else:
            self.assertEqual(self.error_type, 'no_metadata')
            import_config = 'import=(enabled,repair=false)'

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)

        # Copy over the datafiles for the object we want to import.
        self.copy_file(original_db_file, '.', newdir)

        # Import the file.
        # Since we need "file_metadata" without the "repair" option, we should expect an error here.
        with self.expectedStderrPattern(
            'file:original_db_file: import requires that \'file_metadata\' is specified'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.create(uri, import_config))
