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
#
# test_layered_position01.py
#   WT_CURSOR.set_position and get_position on layered tables. Positions are defined over the
#   stable constituent when the table has one, otherwise over the ingest constituent.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_position01(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = 'disaggregated=(role="leader")'
    uri = f'layered:{test_name}'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)
    conn_follow = None
    session_follow = None

    nrows = 3000
    nsamples = 25

    def key(self, i):
        return 'k%06d' % i

    def value(self, key):
        return 'value-' + key + '-' * 100

    def stable_keys(self):
        return [self.key(i) for i in range(self.nrows)]

    # Interleave one ingest key after every seventh stable key.
    def ingest_keys(self):
        return [self.key(i) + 'x' for i in range(0, self.nrows, 7)]

    def deleted_keys(self):
        return [self.key(i) for i in range(self.nrows // 3, 2 * self.nrows // 3)]

    def write(self, session, keys, ts):
        c = session.open_cursor(self.uri)
        session.begin_transaction()
        for key in keys:
            c[key] = self.value(key)
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c.close()

    def remove(self, session, keys, ts):
        c = session.open_cursor(self.uri)
        session.begin_transaction()
        for key in keys:
            c.set_key(key)
            self.assertEqual(c.remove(), 0)
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c.close()

    def checkpoint(self, ts):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts))
        self.session.checkpoint()

    def open_follower(self):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,statistics=(all),disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def check_record(self, c, keys):
        key = c.get_key()
        self.assertIn(key, keys)
        self.assertEqual(c.get_value(), self.value(key))
        return key

    # Position at evenly spaced fractions: keys arrive in order, from the position space.
    def check_sampling(self, c, position_keys, visible_keys):
        p = wiredtiger.Position()
        prev = None
        for i in range(self.nsamples + 1):
            p.pos = i / self.nsamples
            self.assertEqual(c.set_position(p), 0)
            key = self.check_record(c, visible_keys)
            if prev is not None:
                self.assertGreaterEqual(key, prev)
            prev = key
        p.pos = -1.0
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(self.check_record(c, visible_keys), visible_keys[0])

    # After positioning, next and prev step through the merged visible key set.
    def check_next_prev(self, c, visible_keys):
        p = wiredtiger.Position()
        for pos in (0.0, 0.3, 0.5, 0.8, 1.0):
            p.pos = pos
            self.assertEqual(c.set_position(p), 0)
            idx = visible_keys.index(self.check_record(c, visible_keys))
            if idx + 1 < len(visible_keys):
                self.assertEqual(c.next(), 0)
                self.assertEqual(self.check_record(c, visible_keys), visible_keys[idx + 1])
                self.assertEqual(c.prev(), 0)
                self.assertEqual(self.check_record(c, visible_keys), visible_keys[idx])
            if idx > 0:
                self.assertEqual(c.prev(), 0)
                self.assertEqual(self.check_record(c, visible_keys), visible_keys[idx - 1])

    # Walking forwards, positions never decrease.
    def check_get_position_walk(self, c, visible_keys):
        c.reset()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_position(), '/requires a positioned cursor/')
        prev = -1.0
        count = 0
        while c.next() == 0:
            pos = c.get_position()
            self.assertGreaterEqual(pos, 0.0)
            self.assertLessEqual(pos, 1.0)
            self.assertGreaterEqual(pos, prev)
            prev = pos
            count += 1
        self.assertEqual(count, len(visible_keys))

    def check_round_trip(self, c, position_keys):
        p = wiredtiger.Position()
        for key in position_keys[::101]:
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            p.pos = c.get_position()
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(c.get_key(), key)

    def check_key_only(self, c, visible_keys):
        p = wiredtiger.Position()
        p.pos = 0.5
        p.flags = wiredtiger.WT_POSITION_KEY_ONLY
        self.assertEqual(c.set_position(p), 0)
        boundary = c.get_key()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_value(), '/requires value be set/')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_position(), '/requires a positioned cursor/')

        exact = c.search_near()
        found = self.check_record(c, visible_keys)
        if exact >= 0:
            self.assertGreaterEqual(found, boundary)
        else:
            self.assertLess(found, boundary)

        # The leftmost page has the empty key as its boundary, which sorts before every record.
        p.pos = 0.0
        self.assertEqual(c.set_position(p), 0)
        self.assertEqual(c.get_key(), '')
        self.assertGreater(c.search_near(), 0)
        self.assertEqual(self.check_record(c, visible_keys), visible_keys[0])
        c.reset()

    # Round trips are only exact for records written to a page; a record still on an insert list
    # maps to the nearest on-disk slot, so they are checked only over checkpointed data.
    def check_all(self, c, position_keys, visible_keys, round_trip=True):
        self.check_sampling(c, position_keys, visible_keys)
        self.check_next_prev(c, visible_keys)
        self.check_get_position_walk(c, visible_keys)
        if round_trip:
            self.check_round_trip(c, position_keys)
        self.check_key_only(c, visible_keys)

    def test_leader(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        keys = self.stable_keys()
        self.write(self.session, keys, ts=1)
        self.checkpoint(ts=1)

        c = self.session.open_cursor(self.uri)
        self.check_all(c, keys, keys)
        c.close()

        # A cursor configured for random retrieval cannot be positioned.
        p = wiredtiger.Position()
        p.pos = 0.5
        rc = self.session.open_cursor(self.uri, None, 'next_random=true')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: rc.set_position(p), '/not supported by next_random cursors/')
        rc.close()

    def test_follower_stable_only(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        keys = self.stable_keys()
        self.write(self.session, keys, ts=1)
        self.checkpoint(ts=1)
        self.open_follower()
        self.disagg_advance_checkpoint_and_wait(self.conn_follow)

        c = self.session_follow.open_cursor(self.uri)
        self.check_all(c, keys, keys)
        c.close()

    def test_follower_stable_and_ingest(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        stable = self.stable_keys()
        self.write(self.session, stable, ts=1)
        self.checkpoint(ts=1)
        self.open_follower()
        self.disagg_advance_checkpoint_and_wait(self.conn_follow)

        ingest = self.ingest_keys()
        deleted = set(self.deleted_keys())
        self.write(self.session_follow, ingest, ts=2)
        self.remove(self.session_follow, sorted(deleted), ts=3)
        visible = sorted((set(stable) - deleted) | set(ingest))
        position_keys = [k for k in stable if k not in deleted]

        c = self.session_follow.open_cursor(self.uri)
        self.check_all(c, position_keys, visible)

        # Positioned on an ingest-only key, the position comes from the stable constituent and
        # the cursor keeps its place.
        for key in ingest[::37]:
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            pos = c.get_position()
            self.assertGreaterEqual(pos, 0.0)
            self.assertLessEqual(pos, 1.0)
            self.assertEqual(self.check_record(c, visible), key)
            idx = visible.index(key)
            if idx + 1 < len(visible):
                self.assertEqual(c.next(), 0)
                self.assertEqual(self.check_record(c, visible), visible[idx + 1])
        c.close()

        # A fresh cursor whose first operation is an exact search of an ingest-only key still
        # reports the stable-space position: the one bracketed by the neighboring stable keys.
        ingest_key = ingest[1]
        lo, hi = self.key(7), self.key(8)
        ref = self.session_follow.open_cursor(self.uri)
        ref.set_key(lo)
        self.assertEqual(ref.search(), 0)
        pos_lo = ref.get_position()
        ref.set_key(hi)
        self.assertEqual(ref.search(), 0)
        pos_hi = ref.get_position()
        ref.close()
        fresh = self.session_follow.open_cursor(self.uri)
        fresh.set_key(ingest_key)
        self.assertEqual(fresh.search(), 0)
        pos = fresh.get_position()
        self.assertGreaterEqual(pos, pos_lo)
        self.assertLessEqual(pos, pos_hi)
        self.assertEqual(self.check_record(fresh, visible), ingest_key)
        fresh.close()

    # A stable record deleted through the ingest constituent is not returned by set_position: the
    # cursor lands on its nearest visible neighbor, whichever direction was requested.
    def test_follower_deleted_record_resolves(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        stable = self.stable_keys()
        self.write(self.session, stable, ts=1)
        self.checkpoint(ts=1)
        self.open_follower()
        self.disagg_advance_checkpoint_and_wait(self.conn_follow)

        c = self.session_follow.open_cursor(self.uri)
        targets = [self.key(i) for i in (self.nrows // 4, self.nrows // 2, 3 * self.nrows // 4)]
        positions = {}
        for key in targets:
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            positions[key] = c.get_position()
        c.reset()
        self.remove(self.session_follow, targets, ts=2)
        visible = [k for k in stable if k not in targets]

        p = wiredtiger.Position()
        for key in targets:
            idx = visible.index(self.key(int(key[1:]) + 1))
            neighbors = (visible[idx - 1], visible[idx])
            for flags in (0, wiredtiger.WT_POSITION_PREV):
                p.pos = positions[key]
                p.flags = flags
                self.assertEqual(c.set_position(p), 0)
                found = self.check_record(c, visible)
                self.assertNotEqual(found, key)
                self.assertIn(found, neighbors)
        c.close()

    def test_follower_empty_stable(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.checkpoint(ts=1)
        self.open_follower()
        self.disagg_advance_checkpoint_and_wait(self.conn_follow)

        ingest = self.ingest_keys()
        self.write(self.session_follow, ingest, ts=2)

        c = self.session_follow.open_cursor(self.uri)
        p = wiredtiger.Position()
        p.pos = 0.5
        self.assertEqual(c.set_position(p), wiredtiger.WT_NOTFOUND)

        c.set_key(ingest[len(ingest) // 2])
        self.assertEqual(c.search(), 0)
        self.assertRaisesHavingMessage(wiredtiger.WiredTigerError,
            lambda: c.get_position(), '/WT_NOTFOUND/')
        c.close()

    # A follower with no checkpoint has no stable constituent: positions are over ingest.
    def test_follower_no_stable(self):
        self.open_follower()
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')
        keys = self.stable_keys()
        self.write(self.session_follow, keys, ts=1)

        c = self.session_follow.open_cursor(self.uri)
        self.check_all(c, keys, keys, round_trip=False)
        c.close()
