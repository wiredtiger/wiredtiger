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

import time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered_frozen_prepare02.py
#    A prepared transaction left open across a demote pins the frozen tree: a successor checkpoint
#    whose timestamp is above the frozen tree's bound is still deferred until the transaction is
#    resolved, and once resolved the outcome survives adopting that checkpoint, a promotion and a
#    fresh follower's pickup.
@disagg_test_class
class test_layered_frozen_prepare02(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    base_config = 'statistics=(all),precise_checkpoint=true,'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # A value of None removes the key. A rollback is only mirrored when checkpoints may carry the
    # prepared cell. The successor's checkpoint is above the frozen tree's bound (10); with
    # preserve_prepared it stays below the prepare (20), since the successor never saw the prepared
    # transaction and a checkpoint above its prepare would carry the prepared cell.
    outcomes = [
        ('commit', dict(ops={'k': 'prepared'}, commit=True, preserve=False, ckpt_b=30)),
        ('rollback', dict(ops={'k': 'prepared'}, commit=False, preserve=False, ckpt_b=30)),
        ('rollback_preserve', dict(ops={'k': 'prepared'}, commit=False, preserve=True, ckpt_b=15)),
        ('commit_preserve', dict(ops={'k': 'prepared'}, commit=True, preserve=True, ckpt_b=15)),
        ('insert_remove', dict(ops={'n': 'new', 'r': None}, commit=True, preserve=False,
            ckpt_b=30)),
    ]
    scenarios = make_scenarios(disagg_storages, outcomes)

    uri = 'layered:test_layered_frozen_prepare02'
    base = {'k': 'base', 'r': 'base', 'x': 'base'}

    @property
    def conn_base_config(self):
        return self.base_config + ('preserve_prepared=true,' if self.preserve else '')

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def wait_for(self, predicate, what, timeout=60):
        deadline = time.time() + timeout
        while not predicate():
            self.assertLess(time.time(), deadline, what)
            time.sleep(0.1)

    def adopted(self, conn):
        return (self.connection_stat(stat.conn.disagg_checkpoint_meta_lsn, conn) >=
            self.connection_stat(stat.conn.disagg_checkpoint_delivered_lsn, conn))

    def expected(self, resolved):
        kv = {**self.base, 'b': 'successor'}
        if resolved and self.commit:
            for k, v in self.ops.items():
                if v is None:
                    kv.pop(k, None)
                else:
                    kv[k] = v
        return kv

    def test_prepared_pins_frozen_tree(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, self.base, 10)
        self.checkpoint_at(10)

        prep = self.conn.open_session()
        prep_cursor = prep.open_cursor(self.uri, None, None)
        prep.begin_transaction()
        for k, v in self.ops.items():
            prep_cursor.set_key(k)
            if v is None:
                self.assertEqual(prep_cursor.remove(), 0)
            else:
                prep_cursor.set_value(v)
                self.assertEqual(prep_cursor.insert(), 0)
        prep.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20) +
            ',prepared_id=' + self.prepared_id_str(1))
        prep_cursor.close()
        self.demote()
        self.assertGreater(
            self.connection_stat(stat.conn.disagg_frozen_prepared_pending), 0)

        conn_b = self.open_node('successor', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b)
        session_b = conn_b.open_session('')
        self.promote(conn_b)
        self.write_at(self.uri, {'b': 'successor'}, self.ckpt_b, session_b)
        self.checkpoint_at(self.ckpt_b, conn_b)

        deferred = self.connection_stat(stat.conn.disagg_checkpoint_defer_not_covering)
        self.disagg_advance_checkpoint(self.conn, conn_b)
        def pickup_deferred():
            self.assertFalse(self.adopted(self.conn),
                'a checkpoint superseded a frozen tree holding unresolved prepared updates')
            return self.connection_stat(stat.conn.disagg_checkpoint_defer_not_covering) > deferred
        self.wait_for(pickup_deferred, 'the pickup was neither deferred nor adopted')
        self.assertFalse(self.adopted(self.conn))

        mirrored = self.connection_stat(stat.conn.disagg_frozen_prepared_mirrored)
        if self.commit:
            prep.commit_transaction('commit_timestamp=' + self.timestamp_str(40) +
                ',durable_timestamp=' + self.timestamp_str(40))
        else:
            prep.rollback_transaction('rollback_timestamp=' + self.timestamp_str(40))
        prep.close()
        self.assertEqual(self.connection_stat(stat.conn.disagg_frozen_prepared_pending), 0)
        if self.commit or self.preserve:
            self.assertGreater(
                self.connection_stat(stat.conn.disagg_frozen_prepared_mirrored), mirrored)

        self.wait_for(lambda: self.adopted(self.conn),
            'the deferred checkpoint was not adopted after the prepared transaction resolved')
        self.assertEqual(self.read_kvs_at(self.uri, 40), self.expected(True))
        self.assertEqual(self.read_kvs_at(self.uri, 35), self.expected(False))

        session_b.close()
        self.demote(conn_b)
        conn_b.close('debug=(skip_checkpoint=true)')

        self.promote()
        self.assertEqual(self.read_kvs_at(self.uri, 40), self.expected(True))
        self.assertEqual(self.read_kvs_at(self.uri, 35), self.expected(False))
        self.checkpoint_at(40)

        conn_c = self.open_node('fresh', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 40, session_c), self.expected(True))
        session_c.close()
        conn_c.close()
