#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# repro_prepare_iterate_worst_case.py  --  BUG-REVIEW reproducer.
#
#   Worst case of the layered-vs-plain prepare-iteration difference: a prepared
#   key that sorts AFTER every committed key STILL blocks the entire forward
#   scan on the layered follower cursor.
#
#   stable  = {1, 2, 3}  committed (leader checkpoint the follower picked up)
#   ingest  = {9: prepared insert}  (a second follower session)
#
#   Forward scan at read_timestamp > prepare_timestamp:
#     * LAYERED: the FIRST next() returns WT_PREPARE_CONFLICT. None of 1, 2, 3
#       are returned -- even though they are committed, sort before 9, and are
#       entirely unaffected by the prepared 9. A single pending prepare in the
#       ingest blocks ALL forward iteration of the table.
#     * PLAIN: returns 1, 2, 3 and only conflicts on the 4th next() reaching 9.
#
#   The mechanism is the same as the minimal (middle-key) case: a fresh forward
#   walk positions the ingest constituent first, and positioning a cursor onto
#   the prepared 9 is itself the conflict, so the merge never gets to hand back
#   the stable keys. This file makes the IMPACT vivid (committed, unaffected,
#   earlier-sorting keys are blocked) for the bug review.

import wiredtiger, wttest
from helper_disagg import disagg_test_class

@disagg_test_class
class repro_prepare_iterate_worst_case(wttest.WiredTigerTestCase):

    conn_base_config = 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    base = ['1', '2', '3']      # committed, sort before the prepared key
    victim = '9'                # prepared, sorts AFTER all committed keys

    def safe_next(self, c):
        try:
            return c.next()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def prepare_victim(self, conn, uri):
        s = conn.open_session('')
        s.begin_transaction()
        c = s.open_cursor(uri); c[self.victim] = self.victim; c.close()
        s.prepare_transaction('prepare_timestamp=' + self.timestamp_str(300) +
                              ',prepared_id=' + self.prepared_id_str(1))
        return s

    def test_layered_blocks_all_earlier_keys(self):
        follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        uri = 'table:wc_layered'
        self.session.create(uri, 'key_format=S,value_format=S,block_manager=disagg,type=layered')
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(uri)
            for k in self.base:
                c[k] = k
            c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(follow)      # {1,2,3} in follower stable; ingest empty
        prep = self.prepare_victim(follow, uri)     # prepared {9} -> follower ingest

        reader = follow.open_session('')
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cursor = reader.open_cursor(uri)
        # The first next() conflicts -- 1, 2, 3 (committed, sorting before 9) are not returned.
        self.assertEqual(self.safe_next(cursor), wiredtiger.WT_PREPARE_CONFLICT)

        prep.rollback_transaction()
        follow.close()

    def test_plain_returns_earlier_keys(self):
        uri = 'table:wc_plain'
        self.session.create(uri, 'key_format=S,value_format=S')
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(uri)
            for k in self.base:
                c[k] = k
            c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        prep = self.prepare_victim(self.conn, uri)

        reader = self.conn.open_session('')
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cursor = reader.open_cursor(uri)
        # The committed keys are returned in order; only reaching 9 conflicts.
        for k in self.base:
            self.assertEqual(self.safe_next(cursor), 0)
            self.assertEqual(cursor.get_key(), k)
        self.assertEqual(self.safe_next(cursor), wiredtiger.WT_PREPARE_CONFLICT)

        prep.rollback_transaction()
