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

import time
from helper_tiered import TieredConfigMixin
from suite_subprocess import suite_subprocess
import wttest
from wtscenario import make_scenarios
from wtdataset import simple_key, simple_value

import threading, wttest
import wiredtiger
# from wtbackup import backup_base
# from wtthread import op_thread

# test_schema09.py
class test_schema09(TieredConfigMixin, wttest.WiredTigerTestCase, suite_subprocess):
    conn_config_string = 'log=(enabled,file_max=100k,prealloc=false,remove=false),'
    
    types = [
        ('table-simple', dict(uri='table:', use_cg=False, use_index=False)),
    ]
    ops = [
        ('none', dict(schema_ops='none')),
    ]
    ckpt = [
        ('no_ckpt', dict(ckpt=False)),
    ]

    scenarios = make_scenarios(types, ops, ckpt)
    count = 0
    lsns = []
    backup_pfx = "BACKUP."

    # Setup connection config.
    def conn_config(self):
        return self.conn_config_string + self.tiered_conn_config()

    uri = 'table:test_schema_abort'
    table_url = uri + 'table0'
 
    def drop_table_thread_function(self):
        wttest.WiredTigerTestCase.printVerbose(3, "drop_table_thread_function() starting: self.drop_table_url = " + self.table_url)
        sess = self.conn.open_session()
        # Try 10 times to do the drop in case we time out.
        for i in range(10):
            try:
                sess.drop(self.table_url, "force=true")
                break
            except wiredtiger.WiredTigerError as e:
                continue
        sess.close()
        wttest.WiredTigerTestCase.printVerbose(3, "drop_table_thread_function() exiting")

    def test_schema10_test(self):
        self.count = 0
        self.lsns = []
        create_params = 'key_format=i,value_format=S,'

        # Create main table.
        wttest.WiredTigerTestCase.printVerbose(3, "Creating main table " + self.table_url)
        self.session.create(self.table_url, create_params)

        self.session.begin_transaction()

        wttest.WiredTigerTestCase.printVerbose(3, "Bulk upload - about to start")

        cursor = self.session.open_cursor(self.table_url, None, 'bulk')
        for i in range(1, 1000):
            cursor[simple_key(cursor, i)] = simple_value(cursor, i)

        wttest.WiredTigerTestCase.printVerbose(3, "Bulk upload - completed")

        wttest.WiredTigerTestCase.printVerbose(3, "About to create table drop thread")
        thread = threading.Thread(target=self.drop_table_thread_function)
        wttest.WiredTigerTestCase.printVerbose(3, "About to start the table drop thread")
        thread.start()
        thread.join()

        wttest.WiredTigerTestCase.printVerbose(3, "Drop table thread ended")

        time.sleep(2)

        wttest.WiredTigerTestCase.printVerbose(3, "Closing the bulk cursor")
        cursor.close()
        wttest.WiredTigerTestCase.printVerbose(3, "Committing the transaction")
        self.session.commit_transaction()


if __name__ == '__main__':
    wttest.run()
