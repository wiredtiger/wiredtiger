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
# repro_prepare_iterate_layered_vs_plain.py
#
#   DOCUMENTS (for human confirmation that it is by-design) a behavioral
#   difference between a LAYERED cursor and a PLAIN (ordinary, non-layered)
#   cursor during FORWARD ITERATION when a prepared transaction has removed a
#   key the walk has not yet reached.
#
#   Setup (both tables): committed base keys 100, 110, 120; a *separate*
#   transaction prepares a remove of the MIDDLE key (110); a reader iterates
#   forward with read_timestamp > prepare_timestamp.
#
#   THE DIFFERENCE
#   --------------
#   * LAYERED cursor: the VERY FIRST next() returns WT_PREPARE_CONFLICT, even
#     though key 100 (< 110, committed) sorts first and would normally be
#     returned. The follower's layered read merges two constituents (ingest +
#     stable). At the start of a fresh walk the merge must position the ingest
#     constituent, and the ingest btree immediately hits the prepared remove
#     of 110 -> WT_PREPARE_CONFLICT before key 100 from stable can be handed
#     back. So a prepared key ANYWHERE in the ingest blocks the layered walk
#     from the very first step.
#
#   * PLAIN cursor: the first next() returns key 100 (0); only the SECOND
#     next() -- which actually reaches 110 -- returns WT_PREPARE_CONFLICT.
#     A plain table conflicts only when iteration reaches the prepared key.
#
#   * After the prepared txn is rolled back, BOTH iterate cleanly and return
#     all three base keys in order.
#
#   WHY THIS LOOKS BY-DESIGN (presented for Ivan to confirm)
#   --------------------------------------------------------
#   1. The shipped regression test_layered_prepare01.py asserts exactly this
#      for its 'middle' scenario (stable_keys=['1','2','3'], prepared='2'):
#      the FIRST next() returns WT_PREPARE_CONFLICT even though '1' < '2'
#      is committed. That test treats first-next-conflict as the expected,
#      correct layered behavior for a prepared key that is not first.
#
#   2. src/cursor/cur_layered.c __clayered_iterate_constituents() (~1156-1167)
#      documents the mechanism. On a fresh start it iterates the ingest
#      constituent first (line ~1159); the comment at ~1164-1167 states:
#      "If WT_CURSTD_KEY_INT is set, the current cursor is expected to be
#       positioned as well. If there is no current cursor, the cursor walk
#       must be blocked by a prepared conflict on the ingest cursor."
#      i.e. a prepared conflict on the ingest constituent is expected to
#      block the whole layered walk, and the err path at ~1225-1230 resets
#      the ingest cursor so a retry (after the prepare resolves) restarts
#      cleanly. This is intrinsic to merging an ingest btree that holds the
#      prepared update against a stable btree that does not.
#
#   The plain-cursor behavior (conflict only on reaching the key) is the
#   ordinary single-btree prepare semantics. The contrast is therefore the
#   expected consequence of the follower's two-constituent merge, not a
#   correctness bug -- but it is surfaced here for human judgement.

import wiredtiger, wttest
from helper_disagg import disagg_test_class

@disagg_test_class
class repro_prepare_iterate_layered_vs_plain(wttest.WiredTigerTestCase):

    conn_base_config = 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    layered_uri = 'table:repro_prepare_layered'
    plain_uri = 'table:repro_prepare_plain'
    base_keys = ['100', '110', '120']
    victim = '110'                  # the middle key that the prepared txn removes
    fmt = 'key_format=S,value_format=S'

    def safe_next(self, cursor):
        '''Call cursor.next(), mapping a raised WT_PREPARE_CONFLICT to the code.'''
        try:
            return cursor.next()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def prepare_remove_victim(self, conn, uri):
        '''In a separate session, prepare a remove of the middle key. Returns the session.'''
        prep_session = conn.open_session('')
        prep_cursor = prep_session.open_cursor(uri)
        prep_session.begin_transaction()
        prep_cursor.set_key(self.victim)
        prep_cursor.remove()
        prep_cursor.close()
        prep_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(300)}'
            + f',prepared_id={self.prepared_id_str(1)}')
        return prep_session

    def drain_forward(self, cursor):
        '''Iterate to the end, returning the list of keys seen.'''
        got = []
        ret = cursor.next()
        while ret == 0:
            got.append(cursor.get_key())
            ret = cursor.next()
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        return got

    def test_prepare_iterate_layered_vs_plain(self):
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')

        # --- LAYERED table: leader writes base, checkpoints, follower picks it up. ---
        self.session.create(self.layered_uri,
                            self.fmt + ',block_manager=disagg,type=layered')
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(self.layered_uri)
            for k in self.base_keys:
                c[k] = k
            c.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(200)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # --- PLAIN table: ordinary table created directly on the follower, base
        # written there with the same commit timestamp. A plain table does not
        # replicate via the leader checkpoint; the point is only to contrast a
        # single-btree cursor against the layered (two-constituent) cursor under
        # the same prepared remove. ---
        plain_session = conn_follow.open_session('')
        plain_session.create(self.plain_uri, self.fmt)
        plain_session.begin_transaction()
        pc = plain_session.open_cursor(self.plain_uri)
        for k in self.base_keys:
            pc[k] = k
        pc.close()
        plain_session.commit_transaction(f'commit_timestamp={self.timestamp_str(100)}')

        # --- Prepare a remove of the middle key (110) in a separate txn on each. ---
        layered_prep = self.prepare_remove_victim(conn_follow, self.layered_uri)
        plain_prep = self.prepare_remove_victim(conn_follow, self.plain_uri)

        # --- Readers with read_timestamp > prepare_timestamp. ---
        layered_read = conn_follow.open_session('')
        layered_read.begin_transaction(f'read_timestamp={self.timestamp_str(400)}')
        layered_cursor = layered_read.open_cursor(self.layered_uri)

        plain_read = conn_follow.open_session('')
        plain_read.begin_transaction(f'read_timestamp={self.timestamp_str(400)}')
        plain_cursor = plain_read.open_cursor(self.plain_uri)

        #
        # DIFFERENCE #1: the LAYERED cursor conflicts on the FIRST next(), even
        # though key 100 (< the removed 110) is committed and sorts first. The
        # merge positions the ingest constituent at the start of the walk and
        # immediately hits the prepared remove of 110.
        #
        first = self.safe_next(layered_cursor)
        self.pr(f'LAYERED first next() -> ' +
                ('WT_PREPARE_CONFLICT' if first == wiredtiger.WT_PREPARE_CONFLICT
                 else f'key {layered_cursor.get_key()}'))
        self.assertEqual(first, wiredtiger.WT_PREPARE_CONFLICT,
            'expected layered first next() to conflict on the prepared ingest key')

        #
        # DIFFERENCE #2: the PLAIN cursor returns key 100 on the first next(),
        # and only conflicts on the SECOND next() when iteration reaches 110.
        #
        first_plain = self.safe_next(plain_cursor)
        self.assertEqual(first_plain, 0,
            'expected plain first next() to return the committed first key')
        self.assertEqual(plain_cursor.get_key(), self.base_keys[0])
        second_plain = self.safe_next(plain_cursor)
        self.pr(f'PLAIN first next() -> key {self.base_keys[0]}; '
                f'second next() -> ' +
                ('WT_PREPARE_CONFLICT' if second_plain == wiredtiger.WT_PREPARE_CONFLICT
                 else f'key {plain_cursor.get_key()}'))
        self.assertEqual(second_plain, wiredtiger.WT_PREPARE_CONFLICT,
            'expected plain second next() to conflict only on reaching the prepared key')

        #
        # After rolling back BOTH prepared txns, BOTH cursors iterate cleanly to
        # the end. Each resumes from its own surviving position -- a corollary of
        # the difference above:
        #
        #   * The LAYERED conflict happened on a fresh walk, so the err path in
        #     cur_layered.c (~1225-1230) reset the ingest cursor. The cursor is
        #     unpositioned, so draining forward yields ALL three base keys.
        #
        #   * The PLAIN cursor already returned key 100 (first next()) before the
        #     conflict, and a plain prepare conflict leaves the cursor positioned
        #     on that last good key. Draining forward therefore resumes AFTER 100
        #     and yields the remaining keys 110, 120.
        #
        # In both cases every base key is accounted for and 110 is now visible.
        #
        layered_prep.rollback_transaction()
        plain_prep.rollback_transaction()

        self.assertEqual(self.drain_forward(layered_cursor), self.base_keys,
            'layered iteration after rollback should return all base keys')
        self.assertEqual(self.drain_forward(plain_cursor), self.base_keys[1:],
            'plain iteration after rollback should resume after the already-returned first key')

        layered_cursor.close()
        plain_cursor.close()
        layered_read.rollback_transaction()
        plain_read.rollback_transaction()
        conn_follow.close()
