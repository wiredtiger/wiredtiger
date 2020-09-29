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
# test_import05.py
# Error conditions when trying to import files with timestamps past stable.

import os, shutil
import wiredtiger
from wtscenario import make_scenarios
from test_import01 import test_import_base

class test_import05(test_import_base):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    ntables = 10
    nrows = 100
    keys = [b'1', b'2', b'3', b'4', b'5', b'6']
    values = [b'\x01\x02aaa\x03\x04', b'\x01\x02bbb\x03\x04', b'\x01\x02ccc\x03\x04',
              b'\x01\x02ddd\x03\x04', b'\x01\x02eee\x03\x04', b'\x01\x02fff\x03\x04']
    ts = [10*k for k in range(1, len(keys)+1)]
    scenarios = make_scenarios([
        ('insert', dict(op_type='insert')),
        ('delete', dict(op_type='delete')),
    ])

    def test_file_import_future_ts(self):
        original_db_file = 'original_db_file'
        uri = 'file:' + original_db_file
        create_config = 'allocation_size=512,key_format=u,log=(enabled=true),value_format=u'
        self.session.create(uri, create_config)

        # Add data and perform a checkpoint.
        for i in range(0, len(self.keys) - 1):
            self.update(uri, self.keys[i], self.values[i], self.ts[i])

        self.session.checkpoint()

        if self.op_type == 'insert':
            # Insert the last test record.
            self.update(uri, self.keys[-1], self.values[-1], self.ts[-1])
        else:
            self.assertEqual(self.op_type, 'delete')
            self.delete(uri, self.keys[0], self.ts[-1])

        # Perform a checkpoint.
        self.session.checkpoint()

        # Export the metadata for the table.
        c = self.session.open_cursor('metadata:', None, None)
        original_db_file_config = c[uri]
        c.close()

        self.printVerbose(3, '\nFILE CONFIG\n' + original_db_file_config)

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)

        # Place the stable timestamp to 10.
        # The table we're importing had a insert timestamped with 20 so we're expecting an error.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.ts[-2]))

        # Copy over the datafiles for the object we want to import.
        self.copy_file(original_db_file, '.', newdir)

        # Contruct the config string.
        import_config = 'import=(enabled,repair=false,file_metadata=(' + \
            original_db_file_config + '))'

        # Import the file.
        # Since the file has timestamps past stable, we return an error.
        with self.expectedStderrPattern('.*'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.create(uri, import_config))
