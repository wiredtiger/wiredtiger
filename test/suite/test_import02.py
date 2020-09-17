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
# Import a table into a running database.

import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_import02(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,log=(enabled),statistics=(all)')
    session_config = 'isolation=snapshot'

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

    def test_table_import(self):
        original_db_file = 'original_db_file'
        uri = 'table:' + original_db_file
        column_config = 'columns=(id,country,population,area)'
        projection_uri = uri + '(country,population)'

        create_config = 'allocation_size=512,key_format=r,log=(enabled=true),value_format=5sii,{}'.format(column_config)
        self.session.create(uri, create_config)

        # Add some data.
        self.update(projection_uri, b'1', b'\x01\x02aaa\x03\x04', 10)
        self.update(projection_uri, b'2', b'\x01\x02bbb\x03\x04', 20)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Add more data.
        self.update(projection_uri, b'3', b'\x01\x02ccc\x03\x04', 30)
        self.update(projection_uri, b'4', b'\x01\x02ddd\x03\x04', 40)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Export the metadata for the table.
        c = self.session.open_cursor('metadata:', None, None)
        original_db_file_config = c[uri]
        c.close()

        self.tty('\nFILE CONFIG\n' + original_db_file_config)

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

        # Contruct the config string.
        import_config = 'import=(enabled,repair=false,file_metadata=({0})),{1}'.format(original_db_file_config, column_config)

        # Import the file.
        self.session.create(uri, import_config)

        # Verify object.
        self.session.verify(uri)

        # Compare checkpoint information.
        # TO-DO: We really want to compare the complete metadata, but we need to split and
        # sort it first. We can't use the easy python split, but re.split might do the trick.
        c = self.session.open_cursor('metadata:', None, None)
        current_db_file_config = c[uri]
        c.close()
        original_db_file_ckpt = re.match("checkpoint=\(.*\)\)", original_db_file_config)
        current_db_file_ckpt = re.match("checkpoint=\(.*\)\)", current_db_file_config)
        self.assertEqual(original_db_file_ckpt, current_db_file_ckpt)

        # Add some data.
        self.update(projection_uri, b'5', b'\x01\x02eee\x03\x04', 50)
        self.update(projection_uri, b'6', b'\x01\x02fff\x03\x04', 60)

        # Perform a checkpoint.
        self.session.checkpoint()

if __name__ == '__main__':
    wttest.run()
