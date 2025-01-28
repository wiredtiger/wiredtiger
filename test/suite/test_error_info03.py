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

import errno
import time
import wiredtiger
import threading
from compact_util import compact_util
from wtscenario import make_scenarios

# test_error_info03.py
#   Test that the get_last_error() session API returns the last error to occur in the session.
class test_error_info03(compact_util):
    conn_config = 'timing_stress_for_test=[session_alter_slow,open_index_slow]'
    types = [('table-index', dict(uri="table:test_error_info", use_cg=True, use_index=False))]
    tiered_storage_sources = [('non_tiered', dict(is_tiered = False))]
    scenarios = make_scenarios(tiered_storage_sources, types)

    def assert_error_equal(self, err_val, sub_level_err_val, err_msg_val):
        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, err_val)
        self.assertEqual(sub_level_err, sub_level_err_val)
        self.assertEqual(err_msg, err_msg_val)

    def hold_schema_lock(self):
        session = self.conn.open_session()
        session.alter(self.uri, 'access_pattern_hint=random')

    def hold_table_lock(self):
        session = self.conn.open_session()
        c = session.open_cursor(self.uri, None)
        c['key'] = 'value'
        c.close()

    def try_drop(self):
        self.assertTrue(self.raisesBusy(lambda: self.session.drop(self.uri, "checkpoint_wait=0,lock_wait=0")), "was expecting drop call to fail with EBUSY")

    def test_conflict_schema(self):
        """
        Try to drop the table while another thread holds the schema lock.
        """
        self.session.create(self.uri, 'key_format=S,value_format=S')

        lock_thread = threading.Thread(target=self.hold_schema_lock)
        drop_thread = threading.Thread(target=self.try_drop)

        lock_thread.start()
        time.sleep(1)
        drop_thread.start()

        lock_thread.join()
        drop_thread.join()

        self.assert_error_equal(errno.EBUSY, wiredtiger.WT_CONFLICT_SCHEMA_LOCK, "another thread is currently accessing the schema")

    def test_conflict_table(self):
        """
        Try to drop the table while another thread holds the table lock.
        """
        name = "test_error_info"
        self.uri = "table:" + name
        self.session.create(self.uri, 'key_format=S,value_format=S,columns=(k,v)')
        self.session.create('index:' + name + ':i0', 'columns=(k,v)')

        lock_thread = threading.Thread(target=self.hold_table_lock)
        drop_thread = threading.Thread(target=self.try_drop)

        lock_thread.start()
        time.sleep(1)
        drop_thread.start()

        lock_thread.join()
        drop_thread.join()

        self.assert_error_equal(errno.EBUSY, wiredtiger.WT_CONFLICT_TABLE_LOCK, "another thread is currently accessing the table")
