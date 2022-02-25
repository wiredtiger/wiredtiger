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

import wiredtiger, wttest
import os, re
from wtbackup import backup_base
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet, SimpleIndexDataSet, SimpleLSMDataSet

# test_backup28.py
# Test selective backup with different schema types. Recovering a partial backup with colgroups, 
# index or lsm formats should raise a message. The only supported types are file: and table:.
class test_backup28(backup_base):
    dir='backup.dir'    # Backup directory name
    uri="table_backup"

    types = [
        ('file', dict(uri='file:', use_cg=False, use_index=False, remove_list=False)),
        ('lsm', dict(uri='lsm:', use_cg=False, use_index=False, remove_list=False)),
        ('table-simple', dict(uri='table:', use_cg=False, use_index=False, remove_list=False)),
        ('table-cg-copy', dict(uri='table:', use_cg=True, use_index=False, remove_list=False)),
        ('table-index-copy', dict(uri='table:', use_cg=False, use_index=True, remove_list=False)),
        ('table-cg-no-copy', dict(uri='table:', use_cg=True, use_index=False, remove_list=True)),
        ('table-index-no-copy', dict(uri='table:', use_cg=False, use_index=True, remove_list=True)),
    ]

    scenarios = make_scenarios(types)

    def test_backup28(self):
        selective_remove_file_list = []
        uri = self.uri + 'table0'
        create_params = 'key_format=S,value_format=S,'

        cgparam = ''
        suburi = None
        if self.use_cg or self.use_index:
            cgparam = 'columns=(k,v),'
        if self.use_cg:
            cgparam += 'colgroups=(g0),'

        # Create the main table.
        self.session.create(uri, create_params + cgparam)
        # Add in column group or index tables.
        if self.use_cg:
            cgparam = 'columns=(v),'
            suburi = 'colgroup:table0:g0'
            self.session.create(suburi, cgparam)
            selective_remove_file_list.append("table0_g0.wt")
        elif self.use_index:
            suburi = 'index:table0:i0'
            self.session.create(suburi, cgparam)
            selective_remove_file_list.append("table0_i0.wt")
        self.session.checkpoint()

        os.mkdir(self.dir)

        # Now copy the files using full backup. Selectively don't copy files based on remove list.
        all_files = self.take_selective_backup(self.dir, selective_remove_file_list if self.remove_list else [])
        
        if self.use_cg or self.use_index or self.uri == "lsm:":
            # After the full backup, perform partial backup restore on the backup database, it should
            # fail and return with a message, because we currently don't support colgroups, indexes
            # or lsm formats.
            self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
                lambda: self.wiredtiger_open(self.dir, "backup_partial_restore=true"),
                '/partial backup currently only support .* files./')
        else:
            # After the full backup, open and recover the backup database, and it should succeed.
            backup_conn = self.wiredtiger_open(self.dir, "backup_partial_restore=true")
            bkup_session = backup_conn.open_session()

            # Make sure that the table recovered properly.
            c = bkup_session.open_cursor(uri, None, None)
            c.close()
            backup_conn.close()
            
if __name__ == '__main__':
    wttest.run()
