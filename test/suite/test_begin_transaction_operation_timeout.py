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

import time
import wttest
from wiredtiger import stat

# A transaction is pulled into eviction assist while it is being created, when the cache is over
# its clean trigger and nothing can be evicted. The operation timeout configured for that
# transaction must bound the assist, as it does for every other call on the transaction: a caller
# that asked not to wait indefinitely is entitled to rely on it here too. Without a timeout the
# assist is intentionally unbounded, so the cache wait budget is set as a safety net and the
# assertion is that the caller was released by its own timeout well before that budget.
class test_begin_transaction_operation_timeout(wttest.WiredTigerTestCase):
    uri = 'table:test_begin_transaction_operation_timeout'
    cache_bytes = 10 * 1024 * 1024
    operation_timeout_ms = 200
    cache_max_wait_ms = 2000

    conn_config = 'cache_size=10MB,statistics=(all),eviction=(threads_max=1)'

    def _pin_dirty_content(self, sessions_and_cursors):
        # Hold uncommitted dirty content over the clean trigger. Reconciliation cannot drop these
        # updates, so eviction cannot bring the cache back under its trigger while the pin
        # transactions stay open.
        value = 'a' * 4096
        rows_per_txn = 400
        for txn_num, (pin_session, pin_cursor) in enumerate(sessions_and_cursors):
            pin_session.begin_transaction()
            base = txn_num * rows_per_txn
            for i in range(rows_per_txn):
                pin_cursor[base + i] = value

    def test_operation_timeout_at_begin_transaction(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        stat_session = None
        session = None
        cursor = None
        pin_sessions_and_cursors = []
        pin_txns_active = False
        txn_active = False

        try:
            # Pin sessions ignore the cache size, so they are never pulled into the assist they
            # are about to create.
            stat_session = self.conn.open_session('ignore_cache_size=true')
            for _ in range(8):
                pin_session = self.conn.open_session('ignore_cache_size=true')
                pin_sessions_and_cursors.append(
                    (pin_session, pin_session.open_cursor(self.uri)))
            self._pin_dirty_content(pin_sessions_and_cursors)
            pin_txns_active = True

            # The assist is only entered once the cache is over its clean trigger, and the wait
            # has to last for eviction to have a chance to relieve the pressure.
            inuse = self.get_stat(stat.conn.cache_bytes_inuse, session=stat_session)
            self.assertGreater(inuse, self.cache_bytes)

            session = self.conn.open_session('cache_max_wait_ms=%d' % self.cache_max_wait_ms)
            cursor = session.open_cursor(self.uri)

            start = time.monotonic()
            session.begin_transaction(
                'operation_timeout_ms=%d' % self.operation_timeout_ms)
            txn_active = True
            elapsed = time.monotonic() - start

            # A begin that did not wait at all never reached the assist, so the test is not
            # exercising anything: require a material fraction of the timeout to have elapsed.
            min_elapsed = self.operation_timeout_ms / 1000.0 * 0.5
            self.assertGreaterEqual(elapsed, min_elapsed,
                'begin_transaction returned in %.3f seconds, too fast to have been in the '
                'eviction assist' % elapsed)
            self.assertLess(elapsed, 1.0,
                'begin_transaction took %.3f seconds, far beyond operation_timeout_ms=%d' %
                (elapsed, self.operation_timeout_ms))
        finally:
            if txn_active:
                session.rollback_transaction()
            if pin_txns_active:
                for pin_session, _ in pin_sessions_and_cursors:
                    pin_session.rollback_transaction()
            if cursor is not None:
                cursor.close()
            if session is not None:
                session.close()
            for pin_session, pin_cursor in pin_sessions_and_cursors:
                pin_cursor.close()
                pin_session.close()
            if stat_session is not None:
                stat_session.close()

if __name__ == '__main__':
    wttest.run()
