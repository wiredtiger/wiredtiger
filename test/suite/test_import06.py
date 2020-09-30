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
# test_import06.py
# Import a file with the repair option (without the file metadata).

import os, shutil
from test_import01 import test_import_base

class test_import06(test_import_base):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    original_db_file = 'original_db_file'
    uri = 'file:' + original_db_file

    nrows = 100
    ntables = 10
    keys = [b'1', b'2', b'3', b'4', b'5', b'6']
    values = [b'\x01\x02aaa\x03\x04', b'\x01\x02bbb\x03\x04', b'\x01\x02ccc\x03\x04',
              b'\x01\x02ddd\x03\x04', b'\x01\x02eee\x03\x04', b'\x01\x02fff\x03\x04']
    ts = [10*k for k in range(1, len(keys)+1)]
    create_config = 'allocation_size=512,key_format=u,log=(enabled=true),value_format=u'

    def test_import_repair(self):
        self.session.create(self.uri, self.create_config)

        # Add data and perform a checkpoint.
        min_idx = 0
        max_idx = len(self.keys) // 3
        for i in range(min_idx, max_idx):
            self.update(self.uri, self.keys[i], self.values[i], self.ts[i])
        self.session.checkpoint()

        # Add more data and checkpoint again.
        min_idx = max_idx
        max_idx = 2*len(self.keys) // 3
        for i in range(min_idx, max_idx):
            self.update(self.uri, self.keys[i], self.values[i], self.ts[i])
        self.session.checkpoint()

        # Export the metadata for the table.
        #
        # We're not going to use it as the "file_metadata" argument this time but we will use it to
        # compare with the reconstructed metadata after importing.
        c = self.session.open_cursor('metadata:', None, None)
        original_db_file_config = c[self.uri]
        c.close()

        self.printVerbose(3, '\nFile configuration:\n' + original_db_file_config)

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)

        # Make a bunch of files and fill them with data.
        self.populate(self.ntables, self.nrows)
        self.session.checkpoint()

        # Copy over the datafiles for the object we want to import.
        self.copy_file(self.original_db_file, '.', newdir)

        # Construct the config string.
        import_config = 'import=(enabled,repair=true)'

        # Import the file.
        self.session.create(self.uri, import_config)

        # Verify object.
        self.session.verify(self.uri)

        # Check that the previously inserted values survived the import.
        self.check(self.uri, self.keys[:max_idx], self.values[:max_idx], self.ts[:max_idx])

        # Compare configuration metadata.
        c = self.session.open_cursor('metadata:', None, None)
        current_db_file_config = c[self.uri]
        c.close()
        self.config_compare(original_db_file_config, current_db_file_config)

        # Add some data and check that the table operates as usual after importing.
        min_idx = max_idx
        max_idx = len(self.keys)
        for i in range(min_idx, max_idx):
            self.update(self.uri, self.keys[i], self.values[i], self.ts[i])
        self.check(self.uri, self.keys, self.values, self.ts)

        # Perform a checkpoint.
        self.session.checkpoint()
