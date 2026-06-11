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
#   Minimal (2-key) demonstration that a LAYERED follower cursor and a PLAIN
#   table differ on forward iteration when a prepared transaction is pending --
#   a by-design consequence of the follower's stable+ingest merge.
#
#   Two keys:
#     '1' -- committed. On the layered table it lives in STABLE (the leader
#            wrote it and checkpointed; the follower picked it up).
#     '2' -- prepared (uncommitted) by a SEPARATE session. On the layered
#            table it lives in the follower INGEST.
#
#   Forward scan at a read timestamp above the prepare:
#     * LAYERED: the FIRST next() returns WT_PREPARE_CONFLICT -- it never gets
#       to return '1'. To merge-scan, the layered cursor must position its
#       ingest constituent to compare it against stable; the ingest's first
#       entry is the prepared '2', and positioning a cursor onto a prepared
#       update IS the conflict (WT will not expose a key/value it might have to
#       retract). So it cannot discover that stable's '1' sorts first.
#     * PLAIN: one btree, walked in order -> returns '1', then conflicts only
#       when the SECOND next() reaches '2'.
#
#   By-design: a merge join must consult both inputs up front, and WT forbids
#   reading a prepared entry's position. Matches the existing regression
#   test_layered_prepare01 ('middle' scenario asserts a first-next() conflict)
#   and cur_layered.c (the cursor walk is blocked by a prepared conflict on the
#   ingest constituent).

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

    def prepare_insert_2(self, conn, uri, pid):
        '''In a fresh session, prepare an insert of key '2'. Return the session.'''
        s = conn.open_session('')
        s.begin_transaction()
        c = s.open_cursor(uri); c['2'] = '2'; c.close()
        s.prepare_transaction('prepare_timestamp=' + self.timestamp_str(300) +
                              ',prepared_id=' + self.prepared_id_str(pid))
        return s

    def test_layered_vs_plain_prepare_iterate(self):
        follow = self.wiredtiger_open('follower', self.extensionsConfig() +
                ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')

        # LAYERED: key '1' committed on the leader and checkpointed -> follower STABLE.
        lay = 'table:lpid_layered'
        self.session.create(lay, 'key_format=S,value_format=S,block_manager=disagg,type=layered')
        with self.transaction(session=self.session, commit_timestamp=100):
            c = self.session.open_cursor(lay); c['1'] = '1'; c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(200))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(follow)      # '1' now in follower stable; ingest empty
        # Key '2' prepared into the follower INGEST by a second session.
        lay_prep = self.prepare_insert_2(follow, lay, 1)

        # PLAIN table on the follower: '1' committed, '2' prepared -- both in one btree.
        plain = 'table:lpid_plain'
        ps = follow.open_session('')
        ps.create(plain, 'key_format=S,value_format=S')
        ps.begin_transaction()
        pc = ps.open_cursor(plain); pc['1'] = '1'; pc.close()
        ps.commit_transaction('commit_timestamp=' + self.timestamp_str(100))
        plain_prep = self.prepare_insert_2(follow, plain, 2)

        # Readers at read_timestamp (400) > prepare_timestamp (300).
        rl = follow.open_session(''); rl.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cl = rl.open_cursor(lay)
        rp = follow.open_session(''); rp.begin_transaction('read_timestamp=' + self.timestamp_str(400))
        cp = rp.open_cursor(plain)

        # LAYERED: first next() conflicts -- cannot return stable's '1' ahead of the prepared
        # ingest '2', because consulting the ingest constituent is itself the conflict.
        self.assertEqual(self.safe_next(cl), wiredtiger.WT_PREPARE_CONFLICT)

        # PLAIN: returns '1', conflicts only when the second next() reaches '2'.
        self.assertEqual(self.safe_next(cp), 0)
        self.assertEqual(cp.get_key(), '1')
        self.assertEqual(self.safe_next(cp), wiredtiger.WT_PREPARE_CONFLICT)

        cl.close(); cp.close()
        rl.rollback_transaction(); rp.rollback_transaction()
        lay_prep.rollback_transaction(); plain_prep.rollback_transaction()
        follow.close()
