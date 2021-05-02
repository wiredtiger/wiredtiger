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

import wiredtiger, os
from wtscenario import make_scenarios
from wtbackup import backup_base

# test_import11.py
#    Run import/export while backup cursor is open.
class test_import11(backup_base):
    create_config = 'allocation_size=512,key_format=i,value_format=i'
    dir='backup.dir'                    # Backup directory name
    uri = 'test_import11'
    conn_config = 'verbose=[backup]'
    scenarios = make_scenarios([
        ('import_with_metadata', dict(repair=False)),
        #('import_repair', dict(repair=True)),
    ])

    def test_import_with_open_backup_cursor(self):
        # Create and populate the table.
        table_uri = 'table:' + self.uri
        self.session.create(table_uri, self.create_config)
        cursor = self.session.open_cursor(table_uri)
        for i in range(1, 1000):
            cursor[i] = i
        cursor.close()
        self.session.checkpoint()

        # Export the metadata for the file.
        file_uri = 'file:' + self.uri + '.wt'
        c = self.session.open_cursor('metadata:', None, None)
        original_db_table_config = c[table_uri]
        original_db_file_config = c[file_uri]
        c.close()

        os.mkdir(self.dir)
        os.mkdir(self.dir + "_incr")
        config = 'incremental=(enabled,granularity=1M,this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)
        self.take_full_backup(self.dir, bkup_c)
        bkup_c.close()
        self.session.drop(table_uri, 'remove_files=false')

        # First construct the config string for the default or repair import scenario,
        # then call create to import the table.
        if self.repair:
            import_config = 'import=(enabled,repair=true)'
        else:
            import_config = '{},import=(enabled,repair=false,file_metadata=({}))'.format(
                original_db_table_config, original_db_file_config)
        self.session.create(table_uri, import_config)

        self.session.checkpoint()
        # Open backup cursor.
        self.take_incr_backup(self.dir + "_incr", 2)
        self.compare_backups(self.uri, self.dir, self.dir + "_incr", str(self.bkup_id))

if __name__ == '__main__':
    wttest.run()
