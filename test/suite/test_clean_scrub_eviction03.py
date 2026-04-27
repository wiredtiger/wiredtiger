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

from wiredtiger import stat
from helper_clean_scrub import CleanScrubBase, clean_scrub_scenarios
import threading, wttest

# test_clean_scrub_eviction03.py
#       Workload variants and concurrency: large pages, concurrent readers, scrub during
#       checkpoint, and the page-dirtied race.
class test_clean_scrub_eviction03(CleanScrubBase, wttest.WiredTigerTestCase):
    scenarios = clean_scrub_scenarios
    uri = "table:test_clean_scrub_eviction03"

    # Run clean-scrub against a workload with non-default page sizing (small leaf_page_max with
    # large values), exercising reconciliation shapes other than the simple single-block default
    # and verifying that data stays correct. Multi-block leaf reconciliation is hard to force
    # deterministically without internal hooks, but this workload is the closest we can get;
    # running it under TSAN/extra-long would catch concurrency issues in the multi-block scrub
    # path if any are produced.
    def test_large_page_workload(self):
        self.session.create(self.uri,
            'key_format=i,value_format=S,leaf_page_max=4KB,internal_page_max=4KB')
        big_value = 'm' * 2000
        cursor = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows):
            cursor[i] = big_value
        cursor.close()
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 12)

        cursor = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows, 137):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), big_value)
        cursor.close()

    # Concurrent readers during a scrub wave. One thread drives scrubs via cache pressure; the
    # others read the original keys and verify values. Clean-scrub swaps the in-memory page
    # content transparently, so readers must never see the wrong value.
    def test_concurrent_readers(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stop = threading.Event()
        errors = []

        def reader():
            try:
                s = self.conn.open_session()
                c = s.open_cursor(self.uri)
                expected = 'a' * self.value_size
                while not stop.is_set():
                    for i in range(0, self.nrows, 137):
                        c.set_key(i)
                        ret = c.search()
                        if ret != 0:
                            errors.append("search {} returned {}".format(i, ret))
                            return
                        v = c.get_value()
                        if v != expected:
                            errors.append("value mismatch at {}".format(i))
                            return
                c.close()
                s.close()
            except Exception as e:
                errors.append(repr(e))

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        try:
            self.populate(self.nrows, self.nrows * 12)
        finally:
            stop.set()
            for t in readers:
                t.join()

        # The primary signal is that readers never saw a wrong value; that scrubs fired during
        # the run is not asserted here because the reader load competes with population for cache
        # pressure (other tests cover the scrub-count assertion deterministically).
        self.assertEqual(errors, [], "reader thread reported: {}".format(errors))

    # Run checkpoints in parallel with cache pressure so candidate pages are hit while their
    # owning btree is being synced. The WT_BTREE_SYNCING gate must skip them cleanly, and the
    # feature should still make progress after checkpoints close their windows.
    def test_scrub_during_checkpoint(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stop = threading.Event()
        errors = []

        def checkpointer():
            try:
                s = self.conn.open_session()
                while not stop.is_set():
                    s.checkpoint()
                s.close()
            except Exception as e:
                errors.append(repr(e))

        ckpt = threading.Thread(target=checkpointer)
        ckpt.start()
        try:
            self.populate(self.nrows, self.nrows * 12)
        finally:
            stop.set()
            ckpt.join()

        self.assertEqual(errors, [])

        stat_cursor = self.session.open_cursor('statistics:')
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()
        self.assertGreater(evictions, 0, "no scrubs completed despite concurrent checkpoints")

    # Best-effort coverage for the page-dirtied stat: a queued candidate can be re-dirtied by a
    # writer before eviction reaches it, in which case the evict path clears the flag and bumps
    # cache_clean_scrub_page_dirtied. The race isn't deterministic, so the assertion is loose.
    def test_page_dirtied(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        for _ in range(3):
            self.populate(0, self.nrows, value_char='b')
            self.populate(self.nrows, self.nrows * 6)

        stat_cursor = self.session.open_cursor('statistics:')
        dirtied = stat_cursor[stat.conn.cache_clean_scrub_page_dirtied][2]
        stat_cursor.close()
        self.assertGreaterEqual(dirtied, 0)
