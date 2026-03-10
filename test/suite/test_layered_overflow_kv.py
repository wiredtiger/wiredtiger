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
#
# Ensure overflow keys and values are not being generated in disaggregated
# storage (WT-15632).

import random, string, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

# test_layered_overflow_kv.py
#    Ensure overflow keys and values are not being generated in disaggregated storage.

@disagg_test_class
class test_layered_overflow_kv(wttest.WiredTigerTestCase):
    nitems = 500
    num_updates = 10
    table_name = "test_layered_overflow_kv"

    # Enable stats, precise checkpoints, and disaggregated leader role.
    conn_base_config = (
        'statistics=(all),'
        'statistics_log=(wait=1,json=true,on_close=true),'
        'precise_checkpoint=true,'
    )
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    # Small per-key/value limits so wed normally create overflow when
    # stuffing large random strings into the table.
    create_session_config = (
        'key_format=S,value_format=S,'
        'leaf_key_max=256,leaf_value_max=256'
    )

    # Run against layered and shared prefixes in disagg mode.
    disagg_storages = gen_disagg_storages(table_name, disagg_only=True)
    scenarios = make_scenarios(disagg_storages, [
        ('layered', dict(prefix='layered:')),
        ('shared',  dict(prefix='table:')),
    ])

    def get_conn_stat(self, stat_key):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat_key][2]
        stat_cursor.close()
        return val

    def random_string(self, length):
        chars = string.ascii_letters + string.digits + string.punctuation
        return ''.join(random.choices(chars, k=length))

    def test_no_overflow_in_disagg(self):
        # Create table in the configured namespace.
        self.uri = self.prefix + self.table_name
        table_config = self.create_session_config

        # For non-layered URIs, explicitly request disagg block manager.
        if not self.uri.startswith('layered:'):
            table_config += ',block_manager=disagg,log=(enabled=false)'

        self.session.create(self.uri, table_config)

        key_prefix = self.random_string(1000)
        base_value = 'v'
        ts1 = 100

        # Insert large keys/values that would naturally tend to overflow
        # if overflow were allowed.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[key_prefix + str(i)] = base_value + str(i)
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(ts1))
        cursor.close()

        # First checkpoint; verify no overflow was created.
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(ts1))
        self.session.checkpoint()
        self.assertEqual(self.get_conn_stat(stat.conn.rec_overflow_key_leaf), 0)
        self.assertEqual(self.get_conn_stat(stat.conn.rec_overflow_value), 0)

        # Now perform several large updates to a subset of keys.
        ts2 = 200
        update_value = self.random_string(1000)
        for n in range(1, self.num_updates):
            self.session.begin_transaction()
            cursor = self.session.open_cursor(self.uri)
            key = key_prefix + str(n * 100)
            cursor[key] = update_value + '-' + str(n)
            cursor.close()
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(ts2))

        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(ts2))
        self.session.checkpoint()

        # After updates + checkpoint, still no overflow keys/values
        # should have been generated for disaggregated trees.
        self.assertEqual(self.get_conn_stat(stat.conn.rec_overflow_key_leaf), 0)
        self.assertEqual(self.get_conn_stat(stat.conn.rec_overflow_value), 0)

