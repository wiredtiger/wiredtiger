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


import wttest
from wiredtiger import stat

# Test that a thread resolving a transaction is released from the eviction assist at its bounded
# wait. The assist normally spins until the cache drops below its triggers, which never happens when
# the dirty content cannot be reconciled away - here because another session is holding it
# uncommitted. The resolving thread pins no transaction state, so nothing can roll it back to
# relieve the pressure and it must be released on its own.
class test_eviction06(wttest.WiredTigerTestCase):
    uri = 'table:test_eviction06'
    cache_bytes = 50 * 1024 * 1024
    dirty_trigger_pct = 5

    conn_config = 'cache_size=50MB,statistics=(all),' \
        'eviction_dirty_target=1,eviction_dirty_trigger=5,eviction=(threads_max=1)'

    def test_bounded_assist_at_transaction_resolution(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        value = 'a' * 4096

        # Reading statistics is a cursor operation that can itself be pulled into an eviction
        # assist, so read them from a session that never waits for the cache.
        stat_session = self.conn.open_session('cache_max_wait_ms=1')

        # Hold dirty content above the trigger in an uncommitted transaction. Reconciliation has to
        # restore these updates to the page, so no amount of eviction can reclaim them.
        pin_session = self.conn.open_session()
        pin_cursor = pin_session.open_cursor(self.uri)
        pin_session.begin_transaction()
        for i in range(1500):
            pin_cursor[i] = value

        dirty = self.get_stat(stat.conn.cache_bytes_dirty, session=stat_session)
        self.assertGreater(dirty, self.cache_bytes * self.dirty_trigger_pct // 100)

        # Each resolution below enters the assist and cannot bring the cache back under the trigger.
        cursor = self.session.open_cursor(self.uri)
        for i in range(100000, 100500):
            self.session.begin_transaction()
            cursor[i] = value
            self.session.commit_transaction()
            if i % 25 == 0 and self.get_stat(
                stat.conn.eviction_app_bounded_wait_exceeded, session=stat_session) > 0:
                break

        self.assertGreater(self.get_stat(
            stat.conn.eviction_app_bounded_wait_exceeded, session=stat_session), 0)

        # The pressure must still be there, otherwise the assist stopped because the cache drained.
        dirty = self.get_stat(stat.conn.cache_bytes_dirty, session=stat_session)
        self.assertGreater(dirty, self.cache_bytes * self.dirty_trigger_pct // 100)

        pin_session.rollback_transaction()
        cursor.close()
        pin_cursor.close()
        pin_session.close()
        stat_session.close()

if __name__ == '__main__':
    wttest.run()
