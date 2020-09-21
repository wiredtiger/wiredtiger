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
# test_import03.py
# Import a table into a running database.

import os, re, shutil
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class test_import03(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'
    scenarios = make_scenarios([
        # Importing into a new database.
        ('new', dict(import_type='new')),
        # Importing into an existing database with other files.
        ('existing', dict(import_type='existing')),
        # Importing into the same database. We should expect a failure.
        ('same', dict(import_type='same')),
    ])

    def update(self, uri, key, value, commit_ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def check(self, uri, key, value, read_ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor.set_key(key)
        self.assertEqual(0, cursor.search())
        self.assertEqual(value, cursor.get_value())
        self.session.rollback_transaction()
        cursor.close()

    # Helper for populating a database to simulate importing files into an existing database.
    def populate(self):
        # Create file:test_import03_[1-100].
        for fileno in range(1, 100):
            uri = 'file:test_import03_{}'.format(fileno)
            self.session.create(uri, 'key_format=i,value_format=S')
            cursor = self.session.open_cursor(uri)
            # Insert keys [1-100] with value 'foo'.
            for key in range(1, 100):
                cursor[key] = 'foo'
            cursor.close()

    def copy_file(self, file_name, old_dir, new_dir):
        if os.path.isfile(file_name) and "WiredTiger.lock" not in file_name and \
            "Tmplog" not in file_name and "Preplog" not in file_name:
            shutil.copy(os.path.join(old_dir, file_name), new_dir)

    def test_table_import(self):
        original_db_table = 'original_db_table'
        uri = 'table:' + original_db_table

        create_config = 'allocation_size=512,key_format=u,log=(enabled=true),value_format=u'
        self.session.create(uri, create_config)

        key1 = b'1'
        key2 = b'2'
        key3 = b'3'
        key4 = b'4'
        value1 = b'\x01\x02aaa\x03\x04'
        value2 = b'\x01\x02bbb\x03\x04'
        value3 = b'\x01\x02ccc\x03\x04'
        value4 = b'\x01\x02ddd\x03\x04'

        # Add some data.
        self.update(uri, key1, value1, 10)
        self.update(uri, key2, value2, 20)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Add more data.
        self.update(uri, key3, value3, 30)
        self.update(uri, key4, value4, 40)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Export the metadata for the table.
        original_db_file_uri = 'file:' + original_db_table + '.wt'
        c = self.session.open_cursor('metadata:', None, None)
        original_db_table_config = c[original_db_file_uri]
        c.close()

        self.printVerbose(3, '\nFILE CONFIG\n' + original_db_table_config)

        # Contruct the config string.
        import_config = 'import=(enabled,repair=false,file_metadata=(' + \
            original_db_table_config + '))'

        if self.import_type == 'same':
            # Try to import the file even though it already exists in our database.
            # We should get an error back.
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.create(uri, import_config))
            return

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)

        # Simulate importing a file into an existing database.
        # Make a bunch of files and fill them with data.
        if self.import_type == 'existing':
            self.populate()

        # Copy over the datafiles for the object we want to import.
        self.copy_file(original_db_table + '.wt', '.', newdir)

        # Import the file.
        self.session.create(uri, import_config)

        # Verify object.
        self.session.verify(uri)

        # Check that the previously inserted values survived the import.
        self.check(uri, key1, value1, 10)
        self.check(uri, key2, value2, 20)
        self.check(uri, key3, value3, 30)
        self.check(uri, key4, value4, 40)

        # Compare checkpoint information.
        # TO-DO: We really want to compare the complete metadata, but we need to split and
        # sort it first. We can't use the easy python split, but re.split might do the trick.
        c = self.session.open_cursor('metadata:', None, None)
        current_db_table_config = c[uri]
        c.close()
        original_db_table_ckpt = re.match("checkpoint=\(.*\)\)", original_db_table_config)
        current_db_table_ckpt = re.match("checkpoint=\(.*\)\)", current_db_table_config)
        self.assertEqual(original_db_table_ckpt, current_db_table_ckpt)

        key5 = b'5'
        key6 = b'6'
        value5 = b'\x01\x02eee\x03\x04'
        value6 = b'\x01\x02fff\x03\x04'

        # Add some data and check that the file operates as usual after importing.
        self.update(uri, key5, value5, 50)
        self.update(uri, key6, value6, 60)

        self.check(uri, key5, value5, 50)
        self.check(uri, key6, value6, 60)

        # Perform a checkpoint.
        self.session.checkpoint()

if __name__ == '__main__':
    wttest.run()
