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

import wttest
import wiredtiger
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_oligarch09.py
# Simple read write testing for leaf page delta

class test_oligarch09(wttest.WiredTigerTestCase, DisaggConfigMixin):

    conn_base_config = 'oligarch_log=(enabled),transaction_sync=(enabled,method=fsync),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    conn_config = conn_base_config + 'oligarch=(role="leader")'
    disagg_storages = gen_disagg_storages('test_oligarch08', disagg_only = True)

    # Make scenarios for different cloud service providers
    scenarios = make_scenarios(disagg_storages)

    nitems = 1000

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_oligarch_read_write(self):
        uri = "oligarch:test_oligarch08"
        create_session_config = 'key_format=S,value_format=S'
        self.pr('CREATING')
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)
        value1 = "aaaa"
        value2 = "bbbb"

        for i in range(self.nitems):
            cursor[str(i)] = value1

        self.session.checkpoint()

        for i in range(self.nitems):
            if i % 10 == 0:
                cursor[str(i)] = value2

        # XXX
        # Inserted timing delays around reopen, apparently needed because of the
        # oligarch watcher implementation
        import time
        time.sleep(1.0)
        follower_config = self.conn_base_config + 'oligarch=(role="follower")'
        self.reopen_conn(config = follower_config)
        time.sleep(1.0)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            if i % 10 == 0:
                self.assertEquals(cursor[str(i)], value2)
            else:
                self.assertEquals(cursor[str(i)], value1)

    def test_oligarch_read_modify(self):
        uri = "oligarch:test_oligarch08"
        create_session_config = 'key_format=S,value_format=S'
        self.pr('CREATING')
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)
        value1 = "aaaa"
        value2 = "abaa"

        for i in range(self.nitems):
            cursor[str(i)] = value1

        self.session.checkpoint()

        for i in range(self.nitems):
            if i % 10 == 0:
                cursor.set_key(str(i))
                mods = [wiredtiger.Modify('b', 1, 1)]
                self.assertEqual(cursor.modify(mods), 0)

        # XXX
        # Inserted timing delays around reopen, apparently needed because of the
        # oligarch watcher implementation
        import time
        time.sleep(1.0)
        follower_config = self.conn_base_config + 'oligarch=(role="follower")'
        self.reopen_conn(config = follower_config)
        time.sleep(1.0)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            if i % 10 == 0:
                self.assertEquals(cursor[str(i)], value2)
            else:
                self.assertEquals(cursor[str(i)], value1)

    def test_oligarch_read_delete(self):
        uri = "oligarch:test_oligarch08"
        create_session_config = 'key_format=S,value_format=S'
        self.pr('CREATING')
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)
        value1 = "aaaa"

        for i in range(self.nitems):
            cursor[str(i)] = value1

        self.session.checkpoint()

        for i in range(self.nitems):
            if i % 10 == 0:
                cursor.set_key(str(i))
                self.assertEqual(cursor.remove(), 0)

        # XXX
        # Inserted timing delays around reopen, apparently needed because of the
        # oligarch watcher implementation
        import time
        time.sleep(1.0)
        follower_config = self.conn_base_config + 'oligarch=(role="follower")'
        self.reopen_conn(config = follower_config)
        time.sleep(1.0)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            if i % 10 == 0:
                cursor.set_key(str(i))
                self.assertEquals(cursor.search(), wiredtiger.WT_NOTFOUND)
            else:
                self.assertEquals(cursor[str(i)], value1)

    def test_oligarch_read_multiple_delta(self):
        uri = "oligarch:test_oligarch08"
        create_session_config = 'key_format=S,value_format=S'
        self.pr('CREATING')
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)
        value1 = "aaaa"
        value2 = "bbbb"
        value3 = "cccc"

        for i in range(self.nitems):
            cursor[str(i)] = value1

        self.session.checkpoint()

        for i in range(self.nitems):
            if i % 10 == 0:
                cursor[str(i)] = value2

        self.session.checkpoint()

        for i in range(self.nitems):
            if i % 20 == 0:
                cursor[str(i)] = value3

        # XXX
        # Inserted timing delays around reopen, apparently needed because of the
        # oligarch watcher implementation
        import time
        time.sleep(1.0)
        follower_config = self.conn_base_config + 'oligarch=(role="follower")'
        self.reopen_conn(config = follower_config)
        time.sleep(1.0)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            if i % 20 == 0:
                self.assertEquals(cursor[str(i)], value3)
            elif i % 10 == 0:
                self.assertEquals(cursor[str(i)], value2)
            else:
                self.assertEquals(cursor[str(i)], value1)
