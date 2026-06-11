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
# test_layered_prepare_iterate_diff.py
#
#   A LAYERED follower cursor and a PLAIN table differ on forward iteration
#   when a prepared transaction is pending -- a by-design consequence of the
#   follower's stable+ingest merge. Two keys throughout:
#     '1' -- committed (and sorts first).
#     '2' -- prepared (uncommitted) by a SEPARATE session.
#
#   test_layered: on the layered table, '1' is in STABLE (leader checkpoint the
#     follower picked up) and the prepared '2' is in the follower INGEST. The
#     FIRST next() returns WT_PREPARE_CONFLICT -- it never returns '1'. To
#     merge-scan, the layered cursor must position its ingest constituent to
#     compare against stable; the ingest's first entry is the prepared '2', and
#     positioning a cursor onto a prepared update IS the conflict (WT will not
#     expose a key/value it might have to retract), so it cannot find that
#     stable's '1' sorts first.
#
#   test_plain: one btree holds both '1' (committed) and '2' (prepared). The
#     first next() returns '1'; only the SECOND next(), reaching '2', conflicts.
#
#   By-design: a merge join must consult both inputs up front, and WT forbids
#   reading a prepared entry's position. Consistent with test_layered_prepare01
#   ('middle' scenario asserts a first-next() conflict) and the cur_layered.c
#   iterate-constituents logic (the walk is blocked by a prepared conflict on
#   the ingest constituent).

import wiredtiger, wttest
from helper_disagg import disagg_test_class

@disagg_test_class
class test_layered_prepare_iterate_diff(wttest.WiredTigerTestCase):

    conn_base_config = 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    def safe_next(self, c):
        '''cursor.next(), mapping a raised WT_PREPARE_CONFLICT to its code.'''
        try:
            return c.next()
        except wiredtiger.WiredTigerError as e:
            if 'WT_PREPARE_CONFLICT' in str(e):
                return wiredtiger.WT_PREPARE_CONFLICT
            raise

    def prepare_insert_2(self, conn, uri):
        '''In a fresh session, prepare an insert of key '2'. Return the session.'''
        s = conn.open_session('')
        s.begin_transaction()
        c = s.open_cursor(uri); c['2'] = '2'; c.close()
        s.prepare_transaction('prepare_timestamp=' + self.timestamp_str(300) +
                              ',prepared_id=' + self.prepared_id_str(1))
        return s

    def test_layered(self):
        # '1' committed on the leader and checkpointed -> follower STABLE; '2' prepared by a
        # second follower session -> follower INGEST.
        follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        uri = 'table:lpid_layered'
        self.session.create(uri, 'key_format=S,value_format=S,block_manager=disagg,type=layered')

        # Put key=1 to the stable table
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(uri); c['1'] = '1'; c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))

        # Pickup the stable table on the follower
        self.session.checkpoint()
        self.disagg_advance_checkpoint(follow)

        # Put prepare txn for the key = 2
        prep = self.prepare_insert_2(follow, uri)

        reader = follow.open_session('')
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cursor = reader.open_cursor(uri)

        # THE VERY FIRST NEXT CALL ALREADY RETURNS PREPARE CONFLICT
        self.assertEqual(self.safe_next(cursor), wiredtiger.WT_PREPARE_CONFLICT)

        prep.rollback_transaction()
        follow.close()

    def test_plain(self):
        # One ordinary (non-layered) btree holds both keys: '1' committed, '2' prepared by a
        # second session.
        uri = 'table:lpid_plain'
        self.session.create(uri, 'key_format=S,value_format=S')
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(uri); c['1'] = '1'; c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))

        # Put prepare txn for the key = 2
        prep = self.prepare_insert_2(self.conn, uri)

        reader = self.conn.open_session('')
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cursor = reader.open_cursor(uri)

        # Walked in order: '1' is returned, and only the next() that reaches '2' conflicts.
        self.assertEqual(self.safe_next(cursor), 0)
        self.assertEqual(cursor.get_key(), '1')
        self.assertEqual(self.safe_next(cursor), wiredtiger.WT_PREPARE_CONFLICT)

        prep.rollback_transaction()
