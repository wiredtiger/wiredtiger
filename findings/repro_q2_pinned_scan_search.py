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
from wtscenario import make_scenarios

# test_repro_pinned_scan_search.py
#   On a disaggregated FOLLOWER, while a read-committed snapshot is pinned by a held
#   open cursor (session->ncursors > 0), a forward scan disagrees with a point search
#   of the same key on the same state.
#
#   Mechanism (__clayered_can_advance_stable in src/cursor/cur_layered.c): the
#   follower's stable constituent only advances to a newer checkpoint when the
#   session's snapshot is released. While the snapshot is pinned, iteration
#   (iteration=true) can never advance, so a scan keeps reading the OLD stable
#   checkpoint and returns a since-deleted key as live; a point search
#   (iteration=false) consults the ingest tombstone first and returns WT_NOTFOUND.
@disagg_test_class
class test_repro_pinned_scan_search(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),'
    uri = 'layered:test_repro_pinned_scan_search'
    ingest_file = 'file:test_repro_pinned_scan_search.wt_ingest'

    disagg_storages = gen_disagg_storages('test_repro_pinned_scan_search', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    # Mirror one committed op to the leader (stable source) and the follower (ingest).
    def mirror(self, leader_s, leader_c, follow_s, follow_c, key, value, ts):
        for s, c in ((leader_s, leader_c), (follow_s, follow_c)):
            s.begin_transaction()
            if value is None:
                c.set_key(key)
                c.remove()
            else:
                c[key] = value
            s.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')

    # Set stable, checkpoint on the leader, and let the follower pick up the checkpoint.
    def advance(self, ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    # Evict the follower ingest so the inserted keys land in the STABLE constituent
    # and a follower scan reads them from the stable checkpoint.
    def evict_ingest(self, keys):
        ec = self.session_follow.open_cursor(self.ingest_file, None, "debug=(release_evict)")
        for k in keys:
            ec.set_key(k)
            ec.search()
            ec.reset()
        ec.close()

    def test_pinned_scan_vs_search(self):
        cfg = "key_format=i,value_format=S"
        self.session.create(self.uri, cfg)
        self.conn_follow = self.wiredtiger_open(
            'follower', self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        # One session writes the follower ingest; another holds the open scan cursor.
        self.session_follow = self.conn_follow.open_session('')
        self.session_follow.create(self.uri, cfg)
        read_session = self.conn_follow.open_session('')

        keys = [100, 110, 120, 130]
        victim = 110
        lc = self.session.open_cursor(self.uri)
        fc = self.session_follow.open_cursor(self.uri)

        # Insert all keys, mirrored to leader stable and follower ingest.
        for ts, k in enumerate(keys, start=1):
            self.mirror(self.session, lc, self.session_follow, fc, k, f'v{k}', ts)

        # First checkpoint advance, then evict the ingest so the keys live in the
        # follower's STABLE checkpoint (a scan will read them from there).
        self.advance(len(keys))
        self.evict_ingest(keys)

        #
        # PINNED case. Open a scan cursor and position it on the OLD stable checkpoint,
        # opening its stable constituent there. Keeping this cursor open and positioned
        # keeps session->ncursors > 0, which pins the read-committed snapshot across the
        # delete + advance below, so the stable cursor cannot move to the new checkpoint
        # during iteration (__clayered_can_advance_stable returns false when iterating).
        #
        scan = read_session.open_cursor(self.uri)
        self.assertEqual(scan.next(), 0)          # position on first key, opens stable cursor
        self.assertEqual(scan.get_key(), keys[0])

        # Delete key 110 on both. The follower writes an ingest tombstone.
        del_ts = len(keys) + 1
        self.mirror(self.session, lc, self.session_follow, fc, victim, None, del_ts)
        lc.close()
        fc.close()

        # Second checkpoint advance: the delete is folded into the NEW stable
        # checkpoint and the follower picks it up. But the held cursor pins the snapshot,
        # so the already-open stable cursor cannot move to the new checkpoint while iterating.
        self.advance(del_ts)

        # Continue the forward scan with the cursor still held open.
        scanned = [keys[0]]
        while scan.next() == 0:
            scanned.append(scan.get_key())

        # Point search of the deleted key, in the SAME session while the scan cursor is held.
        search = read_session.open_cursor(self.uri)
        search.set_key(victim)
        search_ret = search.search()

        self.pr(f'PINNED  scan -> {scanned}')
        self.pr(f'PINNED  search({victim}) -> ' +
                ('WT_NOTFOUND' if search_ret == wiredtiger.WT_NOTFOUND else f'found {search.get_value()}'))

        # The divergence: scan reports the deleted key as live while search hides it.
        self.assertIn(victim, scanned,
            'expected pinned scan to still see the deleted key (stale stable checkpoint)')
        self.assertEqual(search_ret, wiredtiger.WT_NOTFOUND,
            'expected pinned point search to hide the deleted key (ingest tombstone)')

        scan.close()
        search.close()

        #
        # UNPINNED case: a fresh session with nothing held open. Open a fresh scan cursor
        # and a fresh search cursor. Both reflect the new checkpoint, so they agree: the
        # deleted key is gone from both.
        #
        free_session = self.conn_follow.open_session('')
        scan2 = free_session.open_cursor(self.uri)
        scanned2 = []
        while scan2.next() == 0:
            scanned2.append(scan2.get_key())
        scan2.close()

        search2 = free_session.open_cursor(self.uri)
        search2.set_key(victim)
        search2_ret = search2.search()
        search2.close()

        self.pr(f'UNPINNED scan -> {scanned2}')
        self.pr(f'UNPINNED search({victim}) -> ' +
                ('WT_NOTFOUND' if search2_ret == wiredtiger.WT_NOTFOUND else 'found'))

        self.assertNotIn(victim, scanned2, 'unpinned scan should not see the deleted key')
        self.assertEqual(search2_ret, wiredtiger.WT_NOTFOUND, 'unpinned search should hide the deleted key')
