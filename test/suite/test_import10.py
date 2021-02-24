#!/usr/bin/env python
#
# Public Domain 2014-2021 MongoDB, Inc.
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

from suite_subprocess import suite_subprocess
import wiredtiger, wttest, shutil, os
from wtscenario import make_scenarios
from test_import01 import test_import_base

# test_import10.py
#    Run live import/export while backup cursor is open
class test_import10(test_import_base):
    tablename = 'test_import10.wt'
    nentries = 1000
    create_config = 'allocation_size=512,key_format=i,value_format=i'

    def test_backup_with_live_export(self):
            """
            Test live export in a 'wt' process while backup cursor is open.
            """
            # Create and populate the table.
            uri = 'file:' + self.tablename
            self.session.create(uri, self.create_config)
            cursor = self.session.open_cursor(uri)
            for i in range(1, 1000):
                cursor[i] = i
            cursor.close()

            self.session.checkpoint()
            
            # Open backup cursor
            bkup_c = self.session.open_cursor('backup:', None, None)

            # Export the metadata for the file.
            c = self.session.open_cursor('metadata:', None, None)
            original_db_file_config = c[uri]
            c.close()

            bkup_c.close()

    def test_backup_with_live_import(self):
        """
        Test live import in a 'wt' process while backup cursor is open.
        """

        # Create and populate the table.
        uri = 'file:' + self.tablename
        self.session.create(uri, self.create_config)
        cursor = self.session.open_cursor(uri)
        for i in range(1, 1000):
            cursor[i] = i
        cursor.close()
        self.session.checkpoint()

        self.session.drop(uri, 'remove_files=false')

        # Can't open the cursor on the file anymore since we dropped it.
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(uri))

        # Open backup cursor
        bkup_c = self.session.open_cursor('backup:', None, None)

        # Contruct the config string.
        import_config = 'import=(enabled,repair=true)'

        # Import the file.
        self.session.create(uri, import_config)

        # Verify object.
        self.session.verify(uri)

        # Check that the data got imported correctly.
        cursor = self.session.open_cursor(uri)
        for i in range(1, 1000):
            self.assertEqual(cursor[i], i)
        cursor.close()
        bkup_c.close()

if __name__ == '__main__':
    wttest.run()
