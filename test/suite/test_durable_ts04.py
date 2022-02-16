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

from helper import simulate_crash_restart
import wiredtiger, wttest
from wiredtiger import WT_NOTFOUND
from wtscenario import make_scenarios

# test_durable_ts04.py
#
# Prepared transactions can have a durable timestamp that's greater than the commit
# timestamp. This means that the committed data becomes visible before it's durable,
# which opens a window for someone to read it and then commit and become durable
# before it, which in turn leads to inconsistency because a crash will preserve the
# second transaction but not the first.
#
# To avoid this problem, we prohibit reads between the commit time and the durable
# time, until the global timestamp reaches the durable time.
#
# This test makes sure that mechanism works as intended.
#    1. Reading nondurable data produces WT_PREPARE_CONFLICT.
#    2. Advancing stable to the durable timestamp eliminates this behavior.
#    3. The behavior applies to both updates and removes.
#    4. The behavior applies to pages that have been evicted as well as to in-memory updates.
#    5. Rollback-to-stable can successfully roll back the transaction before it becomes stable.
#    6. ignore_prepare=true disables the check.
#
# Additionally there are two ways to set up the scenario; one involves committing
# before stable. (This is explicitly permitted if stable has moved up since prepare, as
# long as the prepare happend after stable.)

class test_durable_ts04(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=10MB'

    format_values = [
        ('integer-row', dict(key_format='i', value_format='S')),
        ('column', dict(key_format='r', value_format='S')),
        ('column-fix', dict(key_format='r', value_format='8t')),
    ]
    commit_values = [
        ('commit_before_stable', dict(commit_before_stable=True)),
        ('commit_after_stable', dict(commit_before_stable=False)),
    ]
    remove_values = [
        ('update', dict(do_remove=False)),
        ('remove', dict(do_remove=True)),
    ]
    evict_values = [
        ('noevict', dict(do_evict=False)),
        ('evict', dict(do_evict=True)),
    ]
    checkpoint_values = [
        ('nocheckpoint', dict(do_checkpoint=False)),
        ('checkpoint', dict(do_checkpoint=True)),
    ]
    op_values = [
        ('crash', dict(op='crash')),
        ('rollback', dict(op='rollback')),
        ('stabilize', dict(op='stabilize')),
    ]
    ignoreprepare_values = [
        ('no_ignore_prepare', dict(ignore_prepare=False)),
        ('ignore_prepare', dict(ignore_prepare=True)),
    ]
    scenarios = make_scenarios(format_values,
        commit_values, remove_values, evict_values, checkpoint_values, op_values,
        ignoreprepare_values)

    Deleted = 1234567  # Choose this to be different from any legal value.

    def check_range(self, uri, lo, hi, value, read_ts):
        cursor = self.session.open_cursor(uri)
        if value is None:
            for i in range(lo, hi):
                self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: cursor[i], '/committed but non-durable value/')
                self.session.rollback_transaction()
        else:
            ign = ',ignore_prepare=true' if self.ignore_prepare else ''
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts) + ign)
            for i in range(lo, hi):
                if value == self.Deleted:
                    cursor.set_key(i)
                    self.assertEqual(cursor.search(), WT_NOTFOUND)
                else:
                    self.assertEqual(cursor[i], value)
            self.session.rollback_transaction()
        cursor.close()

    def check(self, uri, nrows, low_value, high_value, read_ts):
        self.check_range(uri, 1, nrows // 2, low_value, read_ts)
        self.check_range(uri, nrows // 2, nrows + 1, high_value, read_ts)

    def evict(self, uri, lo, hi, value, read_ts):
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        for k in range(lo, hi):
            v = evict_cursor[k]
            self.assertEqual(v, value)
            self.assertEqual(evict_cursor.reset(), 0)
        self.session.rollback_transaction()

    def test_durable_ts04(self):
        # Create a table.
        uri = 'table:test_durable_ts04'
        nrows = 10
        self.session.create(
            uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        if self.value_format == '8t':
            value_a = 97
            value_b = 0 if self.do_remove else 98
        else:
            value_a = "aaaaa" * 100
            value_b = self.Deleted if self.do_remove else "bbbbb" * 100

        cursor = self.session.open_cursor(uri)

        # Write some initial baseline data at time 5; make it stable and checkpoint it.
        self.session.begin_transaction()
        for k in range(1, nrows + 1):
            cursor[k] = value_a
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(5) + \
                                ',oldest_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()

        # Prepare a transaction and commit it with a later durable time.
        # Prepare at 10, commit at 20, durable will be 50. Move stable to 30, either
        # before or after the commit.
        self.session.begin_transaction()
        for k in range(1, nrows // 2):
            cursor.set_key(k)
            if self.do_remove:
                self.assertEqual(cursor.remove(), 0)
            else:
                cursor.set_value(value_b)
                self.assertEqual(cursor.update(), 0)
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(10))
        if self.commit_before_stable:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20) +
            ',durable_timestamp=' + self.timestamp_str(50))
        if not self.commit_before_stable:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        cursor.close()

        # Optionally evict. Have the eviction cursor read at 10 to avoid touching the
        # nondurable data.
        if self.do_evict:
            self.evict(uri, 1, nrows, value_a, 10)

        # Now try reading at 25 and 40. This should fail in all variants.
        # (25 is after commit, before stable, and before durable; 40 is after commit
        # and stable, and before durable.)
        # If ignore_prepare is set, we should see the transaction anyway and get value_b.
        self.check(uri, nrows, value_b if self.ignore_prepare else None, value_a, 25)
        self.check(uri, nrows, value_b if self.ignore_prepare else None, value_a, 40)

        # We should be able to read at 50.
        self.check(uri, nrows, value_b, value_a, 50)

        # Optionally checkpoint.
        if self.do_checkpoint:
            self.session.checkpoint()

        # Now either crash, roll back explicitly, or move stable forward.
        if self.op == 'crash' or self.op == 'rollback':
            if self.op == 'crash':
                simulate_crash_restart(self, ".", "RESTART")
            else:
                self.conn.rollback_to_stable()

            # The transaction should disappear, because it isn't stable yet, and it
            # should do so without any noise or failures caused by the nondurable checks.
            self.check(uri, nrows, value_a, value_a, 15)
            self.check(uri, nrows, value_a, value_a, 25)
            self.check(uri, nrows, value_a, value_a, 40)
            self.check(uri, nrows, value_a, value_a, 50)
        else:
            # First, move stable to 49 and check we still can't read the nondurable values.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(49))
            self.check(uri, nrows, value_b if self.ignore_prepare else None, value_a, 25)
            self.check(uri, nrows, value_b if self.ignore_prepare else None, value_a, 40)
            self.check(uri, nrows, value_b, value_a, 50)

            # Now, set it to 50 and we should be able to read the previously nondurable values.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
            self.check(uri, nrows, value_a, value_a, 15)
            self.check(uri, nrows, value_b, value_a, 25)
            self.check(uri, nrows, value_b, value_a, 30)
            self.check(uri, nrows, value_b, value_a, 40)
            self.check(uri, nrows, value_b, value_a, 50)

if __name__ == '__main__':
    wttest.run()
