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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin

# A key present in both constituents must read as the ingest version, which is the newer one.
# That still holds when the walk reaches the key by resuming from a prepared conflict rather than
# by stepping onto it directly: resuming advances the blocked constituent onto the key the other
# one is already parked on, and the walk must not let the resume decide which version wins.
@disagg_test_class
class test_layered_async_stepdown21(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages(disagg_only=True)

    uri = 'layered:test_layered_async_stepdown21'

    def test_stale_value_after_resuming_onto_shared_key(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Content written before the step-down is announced lands on the stable constituent: a
        # leading key, so the walk is under way rather than starting fresh when it blocks, and an
        # older value for the key both constituents end up holding.
        self.write_at(self.uri, {'key1': 'v1', 'key3': 'OLD'}, 10)
        self.set_global_ts(1, 10)

        # A prepared update between the two blocks the walk once it has returned the leading key.
        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor['key2'] = 'prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # Content written after the announcement lands on ingest: the current value of the key.
        self.set_step_down_ts(15)
        self.write_at(self.uri, {'key3': 'NEW'}, 20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))

        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'key1')
        self.assertRaisesException(wiredtiger.WiredTigerError, cursor.next,
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT))

        # Resolving by rollback removes the blocking key, so resuming the walk moves the blocked
        # constituent onto the key the other one is already positioned on.
        prepare_session.rollback_transaction()
        prepare_cursor.close()
        prepare_session.close()

        self.assertEqual(cursor.next(), 0)
        self.assertEqual((cursor.get_key(), cursor.get_value()), ('key3', 'NEW'))

        self.session.commit_transaction()
        cursor.close()

if __name__ == '__main__':
    wttest.run()
