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
from wtscenario import make_scenarios

# A walk blocked on a stable prepared update, whose node then steps down, resumes from the key it
# stopped at. The stable cursor is reopened on the follower's checkpoint like any other, which holds
# no prepared update, so the walk returns the stopped key with its value from before the update.
@disagg_test_class
class test_layered_async_stepdown17(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),disaggregated=(role="leader")'

    directions = [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ]

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, directions)

    uri = 'layered:test_layered_async_stepdown17'

    def test_walk_blocked_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'key10': 'v', 'key20': 'v', 'key30': 'v'}, 10)
        self.set_global_ts(1, 10)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor['key20'] = 'v-prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # The reader begins before the step-down is announced, so it reads stable alone.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(14))
        step = cursor.next if self.forward else cursor.prev
        self.assertEqual(step(), 0)
        self.assertEqual(cursor.get_key(), 'key10' if self.forward else 'key30')
        self.assertRaisesException(wiredtiger.WiredTigerError, step,
            '/conflict with a prepared update/')

        self.set_step_down_ts(15)
        self.complete_step_down(15)

        resumed = []
        while step() == 0:
            resumed.append((cursor.get_key(), cursor.get_value()))
        self.assertEqual(resumed,
            [('key20', 'v'), ('key30', 'v')] if self.forward else [('key20', 'v'), ('key10', 'v')])

        # Resetting releases the position, which pins the live stable table. Resolving a straddling
        # prepared transaction on the follower is not supported yet (FIXME-WT-18422), so the
        # rollback can be refused.
        cursor.reset()
        self.session.rollback_transaction()
        cursor.close()
        try:
            prepare_session.rollback_transaction()
        except wiredtiger.WiredTigerError:
            pass
        prepare_cursor.close()
        prepare_session.close()

if __name__ == '__main__':
    wttest.run()
