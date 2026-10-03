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

from test_verbose01 import test_verbose_base
import wttest
from helper import WiredTigerCursor
from wiredtiger import stat
import re, time

# Verify that a slow checkpoint reconciliation is reported with the file it was reconciling and its
# image build and history store wrapup times, and that a fast one is not reported.
class test_verbose06(test_verbose_base):

    test_name = __qualname__
    uri = f'table:{test_name}'

    # Under disaggregated storage, checkpoint reconciles the table's stable component, whose file
    # name carries a _stable suffix.
    file_name = rf'file:{test_name}\.wt(?:_stable)?'

    # A cache-resident table keeps all its updates in memory until checkpoint reconciles them.
    create_config = 'key_format=Q,value_format=S,cache_resident=true'
    conn_config = 'cache_size=1GB,statistics=(all),verbose=[reconcile:0]'

    # Each round rewrites every key. The pinned oldest timestamp keeps every older version, so
    # checkpoint moves all but the newest to the history store. Under timing stress, history store
    # wrapup sleeps a millisecond per key, so the checkpoint reconciliation takes at least nkeys
    # milliseconds, several times the one second warning threshold the stress option sets.
    nkeys = 3000
    nrounds = 10
    value_size = 1000

    slow_pattern = re.compile(
        rf'WT_VERB_RECONCILE.*Checkpoint took more than 1 minute \((\d+)us\) reconciling '
        rf'{file_name}\. Building disk image took (\d+)us\. History store wrapup took (\d+)us\.')

    def populate(self):
        self.session.create(self.uri, self.create_config)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        ts = 2
        with WiredTigerCursor(self.session, self.uri) as cursor:
            for r in range(self.nrounds):
                self.session.begin_transaction()
                value = chr(ord('a') + r) * self.value_size
                for k in range(self.nkeys):
                    cursor[k] = value
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
                ts += 1
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts - 1))

    def checkpoint_output(self):
        start = time.monotonic()
        self.session.checkpoint()
        return self.readStdout(1000000), time.monotonic() - start

    def reconcile_stats(self):
        # Context for a failure: whether one reconciliation was slow, or the work was spread out.
        names = ['rec_maximum_milliseconds', 'rec_maximum_hs_wrapup_milliseconds',
            'rec_maximum_image_build_milliseconds', 'checkpoint_pages_reconciled', 'cache_hs_insert']
        cursor = self.session.open_cursor('statistics:', None, None)
        values = ', '.join('{}={}'.format(n, cursor[getattr(stat.conn, n)][2]) for n in names)
        cursor.close()
        return values

    def finish_and_clean_output(self):
        self.cleanStdout()
        self.conn.reconfigure('verbose=[]')

    def test_fast_reconcile(self):
        self.populate()
        output, _ = self.checkpoint_output()

        self.assertEqual(len(self.slow_pattern.findall(output)), 0,
            "A fast checkpoint reconciliation was reported as slow:\n" + output)
        self.finish_and_clean_output()

    def test_slow_reconcile(self):
        self.populate()
        self.conn.reconfigure('timing_stress_for_test=[checkpoint_hs_wrapup_slow]')
        output, elapsed = self.checkpoint_output()
        self.conn.reconfigure('timing_stress_for_test=[]')

        self.assertGreaterEqual(elapsed, self.nkeys / 1000,
            "Timing stress didn't slow down checkpoint: {:.1f} seconds".format(elapsed))
        slow = self.slow_pattern.findall(output)
        self.assertEqual(len(slow), 1,
            "Expected one slow checkpoint reconciliation message after {:.1f} seconds ({}):\n{}"
            .format(elapsed, self.reconcile_stats(), output))
        total_us, build_us, hs_wrapup_us = (int(v) for v in slow[0])
        self.assertLessEqual(build_us + hs_wrapup_us, total_us)
        self.assertGreater(hs_wrapup_us, build_us,
            "Timing stress slows history store wrapup, which should dominate:\n" + output)
        self.finish_and_clean_output()

if __name__ == '__main__':
    wttest.run()
