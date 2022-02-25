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
from wtscenario import filter_scenarios, make_scenarios

# test_durable_ts04.py
#
# Prepared transactions can have a durable timestamp that's greater than the commit
# timestamp. This means that the committed data becomes visible before it's durable,
# which opens a window for someone to read it and then commit and become durable
# before it, which in turn leads to inconsistency because a crash will preserve the
# second transaction but not the first.
#
# To avoid this problem, we track the durable timestamp of everything a transaction
# reads, and reject attempts to commit with durable timestamp (or commit timestamp
# if not prepared) less than that.
#
# This test makes sure that mechanism works as intended, by setting up a transaction
# with nondurable data and then handling it in a second transaction.
#
# In the first transaction,
#    - The operation may be update or remove; durable timestamps on tombstones also matter.
#    - There are two ways to set up the scenario; one involves committing before stable by
#      preparing and moving stable up, which is explicitly allowed. These scenarios are not
#      really different, but both should get checked.
#    - After commiting we can evict and/or checkpoint; on-disk durable timestamps also matter,
#      including durable stop times for deleted values.
#    - It isn't clear that both evict and checkpoint is useful; might make sense to remove
#      that combination if looking to prune the number of scenarios.
#
# In the second transaction:
#    - Reading nondurable data is always permitted.
#    - The operation can be read, remove, update, insert, or truncate; the write operations
#      can also read (which should trigger the restriction) or not ("blind write"), which
#      should not.
#    - The read can have a timestamp that is in the nondurable range, or no timestamp, which
#      also produces a nondurable read because stable hasn't been moved up.
#    - The commit can be either plain (no timestamp), timestamped, or prepared.
#    - We can try to hit the restriction (expect failure) or not:
#      - a plain commit will fail if the durable timestamp of something it read is beyond
#        stable as of when it commits, and won't if everything it read is stable.
#      - a timestamped commit will fail if its durable timestamp is before the durable
#        timestamp of something it read, and won't if its durable timestamp is the same
#        or later.
#      - note however that this only actually fails if the operation triggers the restriction.
#
# Note that we filter out the case where both the first transaction and the second
# transaction do removes (including truncates) because this doesn't work -- as of this
# writing the second remove is ignored, but this is expected to change so it fails, and
# either way nothing interesting happens.
#
# We also filter out the expected failure scenario for preparing the second transaction
# because that causes a panic. Fortunately the protection scheme can be tested with an
# ordinary timestamped commit. (The prepared scenario does work correctly as of when it
# was written, but because it can't be enabled by default it doesn't help much in
# production.) It would be nice to have a debug switch to turn that panic into a Python
# exception.
#
# We do not for the moment try to create multiple unstable values in history at once.
# That should probably be tested sometime.

class test_durable_ts04(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=10MB'

    format_values = [
        ('integer-row', dict(key_format='i', value_format='S')),
        ('column', dict(key_format='r', value_format='S')),
        ('column-fix', dict(key_format='r', value_format='8t')),
    ]
    # Options for the setup.
    first_remove_values = [
        ('update', dict(first_remove=False)),
        ('remove', dict(first_remove=True)),
    ]
    first_commit_values = [
        ('commit_before_stable', dict(first_commit_before_stable=True)),
        ('commit_after_stable', dict(first_commit_before_stable=False)),
    ]
    evict_values = [
        ('noevict', dict(do_evict=False)),
        ('evict', dict(do_evict=True)),
    ]
    checkpoint_values = [
        ('nocheckpoint', dict(do_checkpoint=False)),
        ('checkpoint', dict(do_checkpoint=True)),
    ]
    # Options for the subsequent transaction.
    second_readts_values = [
        ('readts', dict(second_read_ts=True)),
        ('noreadts', dict(second_read_ts=False)),
    ]
    second_op_values = [
        ('read', dict(second_op='nop', second_read=True, op_xfail=False)),
        ('remove', dict(second_op='remove', second_read=False, op_xfail=False)),
        ('read_remove', dict(second_op='remove', second_read=True, op_xfail=True)),
        ('update', dict(second_op='update', second_read=False, op_xfail=False)),
        ('read_update', dict(second_op='update', second_read=True, op_xfail=True)),
        ('insert', dict(second_op='insert', second_read=False, op_xfail=False)),
        ('read_insert', dict(second_op='insert', second_read=True, op_xfail=True)),
        # While truncate is a read-modify-write, it does not fail here without a
        # read because we operate on disjoint chunks of the database.
        ('truncate', dict(second_op='truncate', second_read=False, op_xfail=False)),
        ('read_truncate', dict(second_op='truncate', second_read=True, op_xfail=True)),
    ]
    second_commit_values = [
        ('commit-plain', dict(second_commit='plain')),
        ('commit-ts', dict(second_commit='ts')),
        ('commit-prepare', dict(second_commit='prepare')),
    ]
    failure_values = [
        ('bad', dict(commit_xfail=True)),
        ('ok', dict(commit_xfail=False)),
    ]

    def keep_scenario(name, vals):
        # Doing remove in the first operation and remove (or truncate) in the second does not
        # do anything interesting.
        first_remove = vals['first_remove']
        second_op = vals['second_op']
        if first_remove and (second_op == 'remove' or 'second_op' == 'truncate'):
            return False
        # Cannot test the prepare.bad case because it panics.
        # FUTURE: we should have a debug flag to turn the prepared failure panic into a
        # Python exception for testing.
        second_commit = vals['second_commit']
        commit_xfail = vals['commit_xfail']
        if second_commit == 'prepare' and commit_xfail:
            return False
        return True
    scenarios = filter_scenarios(
        make_scenarios(format_values,
            first_commit_values, first_remove_values, evict_values, checkpoint_values,
            second_readts_values, second_op_values, second_commit_values, failure_values),
        keep_scenario)

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
            value_b = 0 if self.first_remove else 98
            value_c = 99
        else:
            value_a = "aaaaa" * 100
            value_b = None if self.first_remove else "bbbbb" * 100
            value_c = "ccccc" * 100

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
            if self.first_remove:
                self.assertEqual(cursor.remove(), 0)
            else:
                cursor.set_value(value_b)
                self.assertEqual(cursor.update(), 0)
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(10))
        if self.first_commit_before_stable:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20) +
            ',durable_timestamp=' + self.timestamp_str(50))
        if not self.first_commit_before_stable:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        cursor.reset()

        # Optionally evict. Have the eviction cursor read at 10 to avoid touching the
        # nondurable data.
        if self.do_evict:
            self.evict(uri, 1, nrows, value_a, 10)

        # Optionally checkpoint.
        if self.do_checkpoint:
            self.session.checkpoint()

        # Now do another transaction on top. Read at 40, in the nondurable range
        # (that is, after stable, before the transaction's durable timestamp), or
        # with no timestamp at all (which will read the latest data). We should be
        # able to read the nondurable data freely, but doing so should constrain
        # when we can commit.

        if self.second_read_ts:        
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        else:
            self.session.begin_transaction()

        if self.second_read:
            for k in range(1, nrows // 2):
                if self.first_remove and self.value_format != '8t':
                    cursor.set_key(k)
                    self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
                else:
                    self.assertEqual(cursor[k], value_b)

        if self.second_op == 'truncate':
            lo = self.session.open_cursor(uri)
            hi = self.session.open_cursor(uri)
            lo.set_key(nrows // 2 + 1)
            hi.set_key(nrows)
            self.assertEqual(self.session.truncate(None, lo, hi, None), 0)
            lo.close()
            hi.close()
        else:
            for k in range(nrows // 2 + 1, nrows):
                cursor.set_key(k)
                if self.second_op == 'remove':
                    self.assertEqual(cursor.remove(), 0)
                else:
                    cursor.set_value(value_c)
                    if self.second_op == 'update':
                        self.assertEqual(cursor.update(), 0)
                    elif self.second_op == 'insert':
                        self.assertEqual(cursor.insert(), 0)

        # Set the commit properties.
        if self.second_commit == 'plain':
            if not self.commit_xfail:
                self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(60))
        else:
            # Commit at either 45 or 60. 45 is after stable but before the durable timestamp we
            # read and should fail. 60 is after and should succeed. Note that we could here also
            # prepare before stable, move stable up, and commit before stable, but that doesn't
            # seem useful as it's the handling of durable timestamps we're checking up on.
            tsstr = self.timestamp_str(45 if self.commit_xfail else 60)
            if self.second_commit == 'prepare':
                self.session.prepare_transaction('prepare_timestamp=' + tsstr)
                self.session.timestamp_transaction('commit_timestamp=' + tsstr)
                self.session.timestamp_transaction('durable_timestamp=' + tsstr)
            else:
                self.session.timestamp_transaction('commit_timestamp=' + tsstr)

        if self.op_xfail and self.commit_xfail:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/before the durable timestamp/')
        else:
            self.session.commit_transaction()

if __name__ == '__main__':
    wttest.run()
