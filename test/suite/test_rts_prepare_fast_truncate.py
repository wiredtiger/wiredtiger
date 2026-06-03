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

# test_rts_prepare_fast_truncate.py
#   Verify that rollback to stable (RTS) is compatible with prepared fast
#   truncate operations.
#
#   test_rts_rollback_prepared_fast_truncate: A prepared-but-uncommitted fast
#   truncate is rolled back, then RTS is called. The data committed before
#   stable_timestamp must remain visible throughout.
#
#   test_rts_rollback_committed_fast_truncate: A committed fast truncate at a
#   timestamp above stable_timestamp is rolled back by RTS. This verifies
#   that the existing committed-fast-truncate RTS path continues to work
#   correctly after the code changes for prepared fast truncation.
#
#   FIXME-WT-17663: txn_rts_prep_trunc_rollback cannot be asserted in
#   test_rts_rollback_prepared_fast_truncate: RTS returns EBUSY when any prepared
#   transaction is active, so the prepared fast truncate is rolled back via the session
#   before RTS runs and the new code path is never reached. Once we write prepared
#   fast truncates to disk, crash-recovery simulation can exercise the path where
#   prepared transaction is gone but on-disk WT_REF_DELETED/INPROGRESS survives.
#   Add the stat assertion at that point.

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

class test_rts_prepare_fast_truncate(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all)'
    session_config = 'isolation=snapshot'

    format_values = [
        ('integer_row', dict(key_format='i')),
        ('column',      dict(key_format='r')),
    ]

    scenarios = make_scenarios(format_values)

    def _create_and_populate(self, uri, nrows):
        self.session.create(uri,
            'allocation_size=512,leaf_page_max=512,'
            'log=(enabled=false),'
            'key_format={},value_format=S'.format(self.key_format))
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[i] = 'value' + str(i)
        self.session.commit_transaction('commit_timestamp=10')
        cursor.close()
        self.conn.set_timestamp('oldest_timestamp=10,stable_timestamp=10')
        self.session.checkpoint()

    def _count_rows(self, uri):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        count = 0
        while cursor.next() == 0:
            count += 1
        self.session.commit_transaction()
        cursor.close()
        return count

    def _truncate_all(self, session, uri, nrows):
        c1 = session.open_cursor(uri)
        c2 = session.open_cursor(uri)
        c1.set_key(1)
        c2.set_key(nrows)
        session.truncate(None, c1, c2, None)
        c1.close()
        c2.close()

    def test_rts_rollback_prepared_fast_truncate(self):
        """
        Prepare a fast truncate, roll it back explicitly, then call RTS.
        The data committed at stable_timestamp must remain fully visible.
        """
        uri = 'table:test_rts_prepare_fast_truncate_prep'
        nrows = 10000

        self._create_and_populate(uri, nrows)

        # Reopen so all leaf pages are in WT_REF_DISK state on disk.
        self.reopen_conn()

        # Prepare a fast truncate. This creates WT_REF_DELETED entries with
        # prepare_state == INPROGRESS.  Do NOT commit.
        session2 = self.conn.open_session('isolation=snapshot')
        session2.begin_transaction()
        self._truncate_all(session2, uri, nrows)
        session2.prepare_transaction('prepare_timestamp=20')

        # Roll back the prepared transaction via the session (not via RTS).
        # This restores WT_REF_DELETED refs to WT_REF_DISK.
        session2.rollback_transaction()
        session2.close()

        # Now RTS can be called, no active transactions remain.
        self.conn.rollback_to_stable()

        # All rows must be visible: the prepared truncation was never stable.
        count = self._count_rows(uri)
        self.assertEqual(count, nrows,
            'expected {} rows after RTS, got {}'.format(nrows, count))

    def test_rts_rollback_committed_fast_truncate(self):
        """
        Commit a fast truncate at timestamp 20, then roll it back via RTS
        (stable_timestamp=10).  The data committed at ts=10 must be restored.
        """
        uri = 'table:test_rts_prepare_fast_truncate_comm'
        nrows = 10000

        self._create_and_populate(uri, nrows)

        # Reopen so all leaf pages are in WT_REF_DISK state on disk.
        self.reopen_conn()

        # Commit a fast truncate at timestamp 20 (above stable_timestamp=10).
        session2 = self.conn.open_session('isolation=snapshot')
        session2.begin_transaction()
        self._truncate_all(session2, uri, nrows)
        session2.commit_transaction('commit_timestamp=20')
        session2.close()

        # Confirm the truncation made fast-deleted pages.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        fast_del = stat_cursor[stat.conn.rec_page_delete_fast][2]
        stat_cursor.close()
        self.assertGreater(fast_del, 0, 'expected fast-deleted pages after truncate')

        # Checkpoint so RTS can find the committed (but unstable) truncate
        # on disk as WT_REF_DELETED entries.
        self.session.checkpoint()

        # RTS with stable_timestamp=10 must roll back the truncation.
        self.conn.rollback_to_stable()

        # All rows must be visible again.
        count = self._count_rows(uri)
        self.assertEqual(count, nrows,
            'expected {} rows after RTS rolled back committed fast truncation, '
            'got {}'.format(nrows, count))

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        rts_calls = stat_cursor[stat.conn.txn_rts][2]
        stat_cursor.close()
        self.assertGreater(rts_calls, 0, 'expected RTS to have run')
