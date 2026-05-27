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

# test_wt17638.py
#   Stress test for the race between a deepening parent split and concurrent eviction.
#
#   During a deepening parent split (__split_deepen / __split_ref_prepare) a new WT_REF
#   is zero-initialized (home=NULL) and its addr field is atomically swapped off-page
#   before home is written.  An eviction thread reading ref->home with a relaxed atomic
#   load observed NULL and passed it to __wt_off_page, which dereferences page->dsk
#   unconditionally, causing a SIGSEGV (or an ASSERT_ALWAYS in __rec_root_write under
#   TSAN when __wt_ref_is_root returned true for a leaf).
#
#   The fix upgrades both reads to acquire loads and adds a NULL guard before
#   __wt_off_page in __wt_ref_addr_copy.
#
#   This test creates conditions that widen the race window:
#     - Small pages (4 KB leaf/internal) to force frequent splits.
#     - Small cache (10 MB) to keep eviction pressure high.
#     - Timing stress on split_1 and split_5 to pause the split thread just after
#       __split_ref_prepare runs (addr CAS done, home write imminent).
#     - Concurrent eviction threads scanning refs while the split pauses.
#
#   On unfixed code with TSAN or on weakly-ordered hardware this reproduces the crash.
#   On fixed code the test passes cleanly.

import threading
import wttest

class test_wt17638(wttest.WiredTigerTestCase):
    uri = 'table:test_wt17638'

    # Small cache + split timing stress to maximize races between eviction and splits.
    conn_config = (
        'cache_size=10MB,'
        'timing_stress_for_test=[split_1,split_5],'
        'eviction=(threads_min=2,threads_max=4)'
    )

    # Tiny pages so splits happen frequently.
    table_config = (
        'key_format=i,value_format=S,'
        'allocation_size=4KB,'
        'leaf_page_max=4KB,'
        'internal_page_max=4KB,'
        'split_pct=75'
    )

    NUM_WRITER_THREADS = 4
    ROWS_PER_THREAD = 2000
    VALUE = 'x' * 128  # 128-byte value to fill pages quickly

    def _writer(self, thread_id, errors):
        """Insert rows in a dedicated session to drive splits."""
        session = self.conn.open_session()
        cursor = session.open_cursor(self.uri)
        base = thread_id * self.ROWS_PER_THREAD
        try:
            for i in range(self.ROWS_PER_THREAD):
                cursor[base + i] = self.VALUE
        except Exception as e:
            errors.append(str(e))
        finally:
            cursor.close()
            session.close()

    def _evict_scanner(self, stop_event, errors):
        """Repeatedly scan the table with release_evict to trigger __wt_ref_addr_copy."""
        session = self.conn.open_session()
        evict_cursor = session.open_cursor(self.uri, None, 'debug=(release_evict)')
        try:
            while not stop_event.is_set():
                session.begin_transaction('read_timestamp=' + self.timestamp_str(1))
                try:
                    evict_cursor.reset()
                    while evict_cursor.next() == 0:
                        evict_cursor.reset()
                        break  # one page per pass keeps throughput high
                except Exception:
                    pass
                finally:
                    session.rollback_transaction()
        except Exception as e:
            errors.append(str(e))
        finally:
            try:
                evict_cursor.close()
            except Exception:
                pass
            session.close()

    def test_wt17638_split_eviction_race(self):
        """Drive concurrent splits and eviction to expose the NULL-home race."""
        self.session.create(self.uri, self.table_config)

        # Pin oldest timestamp so the evict scanner can open a read txn.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        errors = []
        stop_event = threading.Event()

        # Start eviction scanner threads before writers so eviction is already
        # running when the first splits fire.
        evict_threads = []
        for _ in range(2):
            t = threading.Thread(target=self._evict_scanner, args=(stop_event, errors))
            t.start()
            evict_threads.append(t)

        # Run writer threads concurrently so splits happen across all key ranges.
        writer_threads = []
        for tid in range(self.NUM_WRITER_THREADS):
            t = threading.Thread(target=self._writer, args=(tid, errors))
            t.start()
            writer_threads.append(t)

        for t in writer_threads:
            t.join()

        # Let eviction threads drain any in-flight scans.
        stop_event.set()
        for t in evict_threads:
            t.join()

        self.assertEqual(errors, [],
            'Unexpected errors during split/eviction stress: ' + '; '.join(errors))

        # Verify all rows are readable after the stress run.
        cursor = self.session.open_cursor(self.uri)
        count = sum(1 for _ in cursor)
        cursor.close()
        self.assertEqual(count, self.NUM_WRITER_THREADS * self.ROWS_PER_THREAD)
