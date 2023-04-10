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

import os, re
from wtscenario import make_scenarios
from wtbackup import backup_base

# test_backup29.py
#    Test interaction between restart, checkpoint and incremental backup.
#
class test_backup29(backup_base):
    #conn_config = 'create,verbose=(backup)'
    create_config = 'allocation_size=512,key_format=i,value_format=S'
    # Backup directory name
    dir='backup.dir'
    incr_dir = 'incr_backup.dir'
    uri = 'test_backup29'
    uri2 = 'test_other'
    value_base = '-abcdefghijkl'

    few = 100
    nentries = 5000

    def parse_blkmods(self, uri):
        meta_cursor = self.session.open_cursor('metadata:')
        config = meta_cursor[uri]
        meta_cursor.close()
        # The search string will look like: 'blocks=hex)'
        # We want just the value after the =.
        b = re.search(',blocks=\w+', config)
        self.assertTrue(b is not None)
        blocks = b.group(0)
        i = 0
        for c in blocks:
            i += 1
            if c == '=':
                break

        blocks_bitmap = blocks[i:]
        self.pr(uri + " BITMAP: " + blocks_bitmap)
        return blocks_bitmap


    def test_backup29(self):
        os.mkdir(self.dir)
        os.mkdir(self.incr_dir)

        # Create and populate the table.
        file_uri = 'file:' + self.uri + '.wt'
        file2_uri = 'file:' + self.uri2 + '.wt'
        table_uri = 'table:' + self.uri
        table2_uri = 'table:' + self.uri2
        self.session.create(table_uri, self.create_config)
        self.session.create(table2_uri, self.create_config)
        c = self.session.open_cursor(table_uri)
        c2 = self.session.open_cursor(table2_uri)
        # Only add a few entries.
        self.pr("Write: " + str(self.few) + " initial data items")
        for i in range(1, self.few):
            val = str(i) + self.value_base
            c[i] = val
            c2[i] = val
        self.session.checkpoint()

        # Take the initial full backup for incremental.
        config = 'incremental=(enabled,granularity=4k,this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)
        self.take_full_backup(self.dir, bkup_c)
        bkup_c.close()

        # Add a lot more data to both tables to generate a filled in block mod bitmap.
        last_i = self.few
        self.pr("Write: " + str(self.nentries) + " additional data items")
        for i in range(self.few, self.nentries):
            val = str(i) + self.value_base
            c[i] = val
            c2[i] = val
        last_i = self.nentries
        c.close()
        c2.close()
        self.session.checkpoint()
        orig_id1blocks = self.parse_blkmods(file2_uri)
        self.pr("CLOSE and REOPEN conn")
        self.reopen_conn()
        self.pr("Reopened conn")


        # After reopening we want to open both tables, but only modify one of them for
        # the first checkpoint. Then modify both tables, checkpoint, take an incremental
        # backup and then test the backup of the table that was not initially modified.
        c = self.session.open_cursor(table_uri)
        c2 = self.session.open_cursor(table2_uri)

        # Do a no-op read from the table we're not changing.
        val = c2[1]
        # Change one table and checkpoint.
        self.pr("Update only table 1: " + str(last_i))
        far_key = self.nentries * 2
        val = str(far_key) + self.value_base
        c[far_key] = val
        self.session.checkpoint()
        # Now change both tables and checkpoint again.
        val = str(last_i) + self.value_base
        self.pr("Update both tables: " + str(last_i))
        c[last_i] = val
        c2[last_i] = val
        self.session.checkpoint()
        new_id1blocks = self.parse_blkmods(file2_uri)

        c.close()
        c2.close()

        # Compare the bitmaps from the metadata. Once a bit is set, it should never
        # be cleared. But new bits could be set. So the check is only: if the original
        # bitmap has a bit set then the current bitmap must be set. 
        for orig, new in zip(orig_id1blocks, new_id1blocks):
            if orig != '0':
                self.assertTrue(new != '0')

if __name__ == '__main__':
    wttest.run()
