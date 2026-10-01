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
import re, time

# Checkpoint progress logging for a single large dirty page whose history store wrapup dominates the
# checkpoint.
class test_verbose06(test_verbose_base):

    test_name = __qualname__
    hot_uri = f'table:{test_name}_hot'
    small_uri = f'table:{test_name}_small'

    # Under disaggregated storage, checkpoint reconciles the table's stable component, whose file
    # name carries a _stable suffix.
    hot_file = rf'file:{test_name}_hot\.wt(?:_stable)?'

    # A cache-resident table is never evicted or split in memory, so its single leaf page grows far
    # past the small maximum in-memory page size that makes a page hot.
    hot_page_max_mb = 1
    hot_create_config = 'key_format=Q,value_format=S,cache_resident=true,' \
        f'memory_page_max={hot_page_max_mb}MB'
    small_create_config = 'key_format=Q,value_format=S'
    conn_config = 'cache_size=1GB,statistics=(all),eviction_dirty_trigger=95,' \
        'eviction_dirty_target=90,eviction_updates_trigger=95,eviction_updates_target=90,' \
        'verbose=[checkpoint_progress:0]'

    # Each round rewrites every key. The pinned oldest timestamp keeps every older version, so
    # checkpoint writes the newest and moves the rest to the history store. The page ends up about
    # 30 times its maximum in-memory size.
    nkeys = 3000
    nrounds = 10
    value_size = 1000

    start_pattern = re.compile(
        rf'WT_VERB_CHECKPOINT_PROGRESS.*Checkpoint reconciling hot page on {hot_file} '
        r'\(row-store leaf, (\d+)MB, \d+ mods\)')
    small_pattern = re.compile(rf'hot page on file:{test_name}_small')
    done_pattern = re.compile(
        rf'WT_VERB_CHECKPOINT_PROGRESS.*Checkpoint reconciled hot page on {hot_file} in \d+ms '
        r'\(row-store leaf, \d+MB, \d+ mods, \d+ blocks, image build \d+ms, HS wrapup \d+ms\)')
    heartbeat_pattern = re.compile(
        rf'WT_VERB_CHECKPOINT_PROGRESS.*Checkpoint reconciling hot page on {hot_file}: '
        r'HS wrapup (\d+)% \((\d+)/(\d+) keys, \d+ updates written, \d+s elapsed\)')

    def populate(self):
        self.session.create(self.hot_uri, self.hot_create_config)
        self.session.create(self.small_uri, self.small_create_config)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        with WiredTigerCursor(self.session, self.small_uri) as cursor:
            self.session.begin_transaction()
            for k in range(100):
                cursor[k] = 's' * 100
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))

        ts = 3
        with WiredTigerCursor(self.session, self.hot_uri) as cursor:
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

    def finish_and_clean_output(self):
        # Checkpoint always logs its own start and end, and closing the connection runs another
        # checkpoint; this test only checks the hot page lines.
        self.ignoreStdoutPattern(r'WT_VERB_CHECKPOINT_PROGRESS')
        self.cleanStdout()
        self.conn.reconfigure('verbose=[]')

    def test_hot_page_start(self):
        self.populate()
        output, _ = self.checkpoint_output()

        start = self.start_pattern.findall(output)
        self.assertEqual(len(start), 1, "Expected one hot page start message:\n" + output)
        self.assertGreaterEqual(int(start[0]), self.hot_page_max_mb)
        self.assertEqual(len(self.small_pattern.findall(output)), 0,
            "A small page was reported as hot:\n" + output)
        self.finish_and_clean_output()

    def test_hot_page_done(self):
        self.populate()
        output, _ = self.checkpoint_output()

        self.assertEqual(len(self.done_pattern.findall(output)), 1,
            "Expected one hot page completion message:\n" + output)
        self.finish_and_clean_output()
    def test_slow_hs_wrapup(self):
        self.populate()
        self.conn.reconfigure('timing_stress_for_test=[checkpoint_hs_wrapup_slow]')
        output, elapsed = self.checkpoint_output()
        self.conn.reconfigure('timing_stress_for_test=[]')

        # The stress delays history store wrapup by a millisecond per key of the hot page.
        self.assertGreaterEqual(elapsed, self.nkeys / 1000,
            "Timing stress didn't slow down checkpoint: {:.1f} seconds".format(elapsed))

        heartbeats = self.heartbeat_pattern.findall(output)
        self.assertGreaterEqual(len(heartbeats), 1,
            "No history store wrapup progress for a slow hot page:\n" + output)
        for pct, done, total in heartbeats:
            self.assertLessEqual(int(done), int(total))
            self.assertLessEqual(int(pct), 100)
        # Progress is monotonic within the page.
        done = [int(h[1]) for h in heartbeats]
        self.assertEqual(done, sorted(done))
        self.finish_and_clean_output()

if __name__ == '__main__':
    wttest.run()
