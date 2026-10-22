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

import threading
import time

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered_eviction09.py
#    A clean disaggregated leaf holding a checkpoint's multiblock reconciliation result (no
#    retained disk image) is evicted while the materialization frontier is unset, so the
#    eviction is allowed. If a frontier that trails the leaf's block LSNs is published between
#    the eviction review and the split, the children are pushed to WT_REF_DISK while their
#    blocks are not readable yet. The engine must refuse the eviction (leave the page in
#    cache) rather than abort on the __split_multi invariant.
#
#    The eviction is stalled after the review with the split_7 timing stress, which gives the
#    test a window to publish the frontier. Without the fix the process aborts; with the fix
#    the eviction returns EBUSY and the page stays in cache.
#
#    That stress sleeps a random, usually very short time, and the frontier is monotonic, so a
#    publish only opens the window once per connection. Repeat the race across connection
#    reopens to make the test trip reliably.
@disagg_test_class
class test_layered_eviction09(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = ('statistics=(all),disaggregated=(role="leader"),'
                   'timing_stress_for_test=[split_7]')

    uri = f'layered:{test_name}'

    # Small pages make a checkpoint reconciliation split each leaf into many blocks.
    create_session_config = 'key_format=i,value_format=S,leaf_page_max=4KB,internal_page_max=4KB'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    nkeys = 20000
    lag = 700
    delay = 0.01
    rounds = 8

    def get_stat(self, stat_id):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        value = stat_cursor[stat_id][2]
        stat_cursor.close()
        return value

    def test_layered_eviction09(self):
        # Evictions that raced the frontier publication are read back ahead of it: expected.
        self.ignoreStdoutPattern('LSN frontier warning')
        stable_uri = 'file:' + self.uri.split(':', 1)[1] + '.wt_stable'
        timestamp = 0

        for rnd in range(self.rounds):
            # A new connection resets the frontier, so the race can trip again.
            if rnd == 0:
                self.session.create(self.uri, self.create_session_config)
            else:
                self.reopen_conn()
            page_log = self.conn.get_page_log(self.vars.page_log)

            cursor = self.session.open_cursor(self.uri)
            for i in range(self.nkeys):
                self.session.begin_transaction()
                cursor[i] = 'a' * 200
                self.session.commit_transaction(
                    'commit_timestamp=' + self.timestamp_str(timestamp + i + 1))
            cursor.close()
            timestamp += self.nkeys + 2

            # Checkpoint while the materialization frontier is unset. The reconciliation splits
            # the large leaves into multiple blocks and retains no disk image for them; the
            # pages are clean, flagged to be evicted soon.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(timestamp))
            self.session.checkpoint()
            self.assertGreater(self.get_stat(stat.conn.rec_multiblock_leaf), 0)

            (ret, last_lsn) = page_log.pl_get_last_lsn(self.session)
            self.assertEqual(ret, 0)
            target = last_lsn - self.lag
            self.assertGreater(target, 0)

            nthreads = 8
            barrier = threading.Barrier(nthreads + 1)
            errors = []

            def evict(thread_id):
                try:
                    session = self.conn.open_session('debug=(release_evict_page=true)')
                    session.begin_transaction('ignore_prepare=true')
                    evict_cursor = session.open_cursor(stable_uri)
                    barrier.wait()
                    start = thread_id * (self.nkeys // nthreads)
                    for key in range(start, self.nkeys, nthreads):
                        evict_cursor.set_key(key)
                        if evict_cursor.search() == 0:
                            evict_cursor.reset()
                    evict_cursor.close()
                    session.rollback_transaction()
                    session.close()
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

            threads = [threading.Thread(target=evict, args=(i,)) for i in range(nthreads)]
            for thread in threads:
                thread.start()
            barrier.wait()

            # Publish a frontier that trails the block LSNs written by the checkpoint while the
            # evictions are between their review and their split.
            time.sleep(self.delay)
            self.conn.set_context_uint(
                wiredtiger.WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN, target)

            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])

            # Move the frontier past everything the run wrote before reading anything back: evictions
            # that completed while the frontier was unset are free to discard their pages, and the
            # trailing frontier must cover them.
            for _ in range(5):
                self.session.checkpoint()
                (ret, current_lsn) = page_log.pl_get_last_lsn(self.session)
                self.assertEqual(ret, 0)
                page_log.pl_set_last_materialized_lsn(self.session, current_lsn)
                self.conn.set_context_uint(
                    wiredtiger.WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN, current_lsn)
                time.sleep(0.1)

        # Reads must still be served: the pages the eviction refused to discard stay in cache.
        read_ahead_before = self.get_stat(stat.conn.disagg_block_read_ahead_frontier)
        read_cursor = self.session.open_cursor(self.uri)
        for key in range(self.nkeys):
            read_cursor.set_key(key)
            self.assertEqual(read_cursor.search(), 0)
            self.assertEqual(read_cursor.get_value(), 'a' * 200)
        read_cursor.close()
        self.assertEqual(
            self.get_stat(stat.conn.disagg_block_read_ahead_frontier), read_ahead_before)
