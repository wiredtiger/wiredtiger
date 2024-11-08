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

import os
from wtbackup import backup_base
from wiredtiger import stat

class test_backup31(backup_base):
    conn_config = 'cache_size=1G'
    dir='backup.dir'
    uris = [f"table:uri_{i}" for i in range(1, 11)]

    def add_timestamp_data(self, uri, key, val, timestamp):
        self.session.begin_transaction()
        c = self.session.open_cursor(uri, None, None)
        for i in range(0, 1000):
            k = key + str(i)
            v = val
            c[k] = v
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))

    def test_backup31(self):
        for uri in self.uris:
            # create 10 URIs and large amount of data in each.
            self.session.create(uri, "key_format=S,value_format=S")
            for i in range(1, 10):
                self.add_timestamp_data(uri, "key", f"val{i}", i)

        # Ensure all the data added is stable to persist data in HS.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(15))
        self.session.checkpoint()

        # Take selective backup of last first 5 tables.
        os.mkdir(self.dir)
        self.take_selective_backup(self.dir, [uri.replace("table:", "") + ".wt" for uri in self.uris[-5:]])

        # Open the backup directory. As part of opening, it will run RTS internally to truncate any HS pages
        # that belong to the tables that are not part of the selective backup.
        backup_conn = self.wiredtiger_open(self.dir, "backup_restore_target=[\"{0}\"]".format('","'.join(self.uris[:5])))
        backup_session = backup_conn.open_session()
        stat_cursor = backup_session.open_cursor('statistics:', None, None)

        # Assert that fast truncate was performed.
        fast_truncate_pages = stat_cursor[stat.conn.rec_page_delete_fast][2]
        self.assertGreater(fast_truncate_pages, 0)

        # Reopen the connection with verify_metadata=true. This will ensure that the metadata is verified and
        # the tables that are not part of the selective backup don't exist in HS and metadata.
        backup_conn.close()
        backup_conn = self.wiredtiger_open(self.dir, "verify_metadata=true")
        backup_conn.close()
