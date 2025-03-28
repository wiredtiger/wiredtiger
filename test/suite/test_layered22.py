#!/usr/bin/env python3
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
import time
import wiredtiger
import wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered22.py
# Test a secondary can perform reads and writes to the ingest component
# of a layered table, without the stable component.
class test_layered22(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'cache_size=10MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),transaction_sync=(enabled,method=fsync),' \
                     + 'disaggregated=(page_log=palm),'

    disagg_storages = gen_disagg_storages('test_layered22', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    nitems = 100000
    uri = 'layered:test_layered22'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignoreStdoutPattern('WT_VERB_RTS')

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', 'palm')

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="follower"),'

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def session_create_config(self):
        return 'key_format=S,value_format=S,'

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def test_largest_key_without_stable(self):
        # Avoid checkpoint error with precise checkpoint
        self.conn.set_timestamp('stable_timestamp=1')

        # The node started as a follower, so step it up as the leader
        self.conn.reconfigure('disaggregated=(role="leader")')

        self.session.create(self.uri, self.session_create_config())

        self.session.checkpoint()

        page_log = self.conn.get_page_log('palm')
        (ret, last_lsn) = page_log.pl_get_last_lsn(self.session)
        print(f"{last_lsn=}")
        self.assertEqual(ret, 0)
        self.conn.reconfigure(f'disaggregated=(last_materialized_lsn={last_lsn})')

        time.sleep(5)
        # reads_pre = self.get_stat()
        # evictions_pre = self.get_stat()

        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            # if i % 10000 == 0:
            #     if i < nitems / 2:
            #         self.conn.reconfigure(f'disaggregated=last_materialized_lsn={last_lsn}')
        cursor.close()

        # stats cursor: shouldn't evict or read anything
        time.sleep(5)
