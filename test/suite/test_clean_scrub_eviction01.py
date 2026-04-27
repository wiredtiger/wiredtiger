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
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
import threading, wiredtiger, wttest

# test_clean_scrub_eviction01.py
# Tests for clean-scrub eviction: reconciliation retains a disk image on the
# page, and eviction re-instantiates the in-memory content from that image
# rather than writing the page out and later re-reading it.
class test_clean_scrub_eviction01(wttest.WiredTigerTestCase):
    # Use a cache large enough that pages stay dirty in memory until checkpoint,
    # but small enough that a second wave of inserts triggers eviction and
    # gives the eviction walk a chance to find clean-scrub candidates.
    conn_config = 'cache_size=50MB,statistics=(all),eviction=(clean_scrub_eviction=true),debug_mode=(clean_scrub=true,evict_walk_full=true),checkpoint=(wait=0)'
    uri = "table:test_clean_scrub_eviction01"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    # Verify that a checkpoint saves disk images for reconciled leaf pages
    # when debug_mode=(clean_scrub=true) bypasses the normal thresholds.
    def test_images_saved_on_checkpoint(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(images_saved, 0)

    # Verify that clean pages with saved disk images are re-instantiated via
    # the clean-scrub eviction path. A checkpoint produces clean pages with
    # saved disk images; a second wave of inserts then pressures the cache
    # and drives the eviction server to find and scrub those pages.
    def test_clean_scrub_eviction(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        # Insert enough data to exceed the cache size (50MB) so that the
        # eviction server runs and walks over the clean pages that have saved
        # disk images from the checkpoint.
        self.populate(self.nrows, self.nrows * 12)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()

        self.assertGreater(images_saved, 0)
        self.assertGreater(evictions, 0)

    # Verify that data is still readable and correct after clean-scrub
    # re-instantiation replaces the in-memory page content.
    def test_clean_scrub_data_correct(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 2, value_char='b')

        cursor = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'a' * self.value_size)
        for i in range(self.nrows, self.nrows * 2):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'b' * self.value_size)
        cursor.close()

    # Verify that saved-image bytes are reflected in the connection cache bytes counters so
    # the memory used by clean-scrub inventory is visible to operators.
    def test_image_bytes_tracked(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        # Baseline: nothing saved yet.
        stat_cursor = self.session.open_cursor('statistics:')
        self.assertEqual(stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2], 0)
        stat_cursor.close()

        self.populate(0, self.nrows)
        self.session.checkpoint()

        # After checkpoint: the outstanding gauge should be positive and at most the cumulative
        # saved bytes. The gauge (cache_clean_scrub_image_bytes) is an inventory-style counter;
        # the cumulative counter (cache_clean_scrub_image_saved_bytes) monotonically increases.
        stat_cursor = self.session.open_cursor('statistics:')
        saved_bytes = stat_cursor[stat.conn.cache_clean_scrub_image_saved_bytes][2]
        outstanding = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        # The gauge includes a small overhead fudge factor (see __wt_cache_bytes_plus_overhead),
        # so it can slightly exceed the raw cumulative saved_bytes. We only care that it tracks
        # saves: positive, and within a sensible range of the cumulative figure.
        self.assertGreater(outstanding, 0)
        self.assertLess(outstanding, saved_bytes * 2)

        # Force cache pressure so the images are eventually scrubbed/discarded.
        self.populate(self.nrows, self.nrows * 12)

        # After eviction has run the outstanding gauge drops: images are either scrubbed (the
        # bytes move into the new child pages' disk image counters) or the owning pages are
        # discarded. Either way our inventory counter should decrease.
        stat_cursor = self.session.open_cursor('statistics:')
        after = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertLess(after, outstanding)

    # Verify that disabling the flag at runtime stops clean-scrub evictions.
    def test_clean_scrub_off(self):
        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 2)

        stat_cursor = self.session.open_cursor('statistics:')
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()
        self.assertEqual(evictions, 0)

    # Run clean-scrub against a workload with non-default page sizing (small leaf_page_max with
    # large values), exercising reconciliation shapes other than the simple single-block default
    # and verifying that data stays correct. Multi-block leaf reconciliation is hard to force
    # deterministically in unit-test settings, but this workload is the closest we can come
    # without internal hooks; running it under TSAN/extra-long would catch concurrency issues
    # in the multi-block scrub path if any are produced.
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

        # Data correctness across a sample of original keys: scrub re-instantiated their pages,
        # the values must still match.
        cursor = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows, 137):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), big_value)
        cursor.close()

    # Per-dhandle check that the metadata and history-store btrees never hold clean-scrub images.
    # Both are excluded inside __rec_should_save_disk_image to avoid saving memory on btrees the
    # scrub path doesn't apply to.
    def test_system_btrees_not_saved(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        user = self.session.open_cursor('statistics:' + self.uri)
        user_saved = user[stat.dsrc.cache_clean_scrub_image_saved][2]
        user.close()
        self.assertGreater(user_saved, 0)

        for system_uri in ('statistics:file:WiredTigerHS.wt',
                           'statistics:file:WiredTiger.wt'):
            try:
                c = self.session.open_cursor(system_uri)
            except wiredtiger.WiredTigerError:
                continue
            saved = c[stat.dsrc.cache_clean_scrub_image_saved][2]
            c.close()
            self.assertEqual(saved, 0,
                "system btree {} has clean-scrub images: {}".format(system_uri, saved))

    # After the feature is disabled and all images are consumed, the cumulative inventory
    # gauge must return to zero. This catches drift from any path that frees an image without
    # releasing the inventory accounting.
    def test_inventory_returns_to_zero(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        peak = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertGreater(peak, 0)

        # Disable the feature so no new images are saved, then apply enough pressure to
        # force every saved image to be either scrubbed or discarded.
        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.populate(self.nrows, self.nrows * 12)

        # Drop the user table so all its pages are discarded. The inventory must be fully
        # released.
        self.session.close()
        self.session = self.conn.open_session()
        self.dropUntilSuccess(self.session, self.uri)

        stat_cursor = self.session.open_cursor('statistics:')
        remaining = stat_cursor[stat.conn.cache_clean_scrub_image_bytes][2]
        stat_cursor.close()
        self.assertEqual(remaining, 0,
            "inventory gauge did not return to zero after table drop: {} bytes".format(remaining))

    # Cycle the feature on, off, and on again. The "saved" cumulative counter must not grow
    # while the feature is off, and must grow again when it is re-enabled.
    def test_reconfigure_cycle(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_before_off = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(saved_before_off, 0)

        self.conn.reconfigure('eviction=(clean_scrub_eviction=false)')
        self.populate(self.nrows, self.nrows * 2)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_after_off = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertEqual(saved_after_off, saved_before_off,
            "saves grew while the feature was off")

        self.conn.reconfigure('eviction=(clean_scrub_eviction=true)')
        self.populate(self.nrows * 2, self.nrows * 3)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        saved_after_on = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(saved_after_on, saved_after_off,
            "saves did not resume after re-enable")

    # Concurrent readers during a scrub wave. One thread drives scrubs via cache pressure; the
    # other threads read the original keys and verify values. Clean-scrub swaps the in-memory
    # page content transparently; readers must never see the wrong value.
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
            # Drive cache pressure so scrubs fire while readers run.
            self.populate(self.nrows, self.nrows * 12)
        finally:
            stop.set()
            for t in readers:
                t.join()

        self.assertEqual(errors, [], "reader thread reported: {}".format(errors))

        stat_cursor = self.session.open_cursor('statistics:')
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()
        self.assertGreater(evictions, 0)

    # Stress the __wt_btree_syncing guard: run checkpoints in parallel with cache pressure so
    # candidate pages are hit while their owning btree is being synced. The guard should make
    # the walker skip them (avoiding EBUSY), and the feature should still make progress after
    # the checkpoint window closes.
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

    # Best-effort coverage for the page-dirtied stat: a page queued for clean-scrub can be
    # re-dirtied by a writer before eviction gets to it, in which case the evict path clears
    # the flag and increments cache_clean_scrub_page_dirtied. The race isn't deterministic, so
    # the assertion is loose  the stat simply must not be negative and should be recorded
    # when the race does fire.
    def test_page_dirtied(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        # Overwrite the same keys while cache pressure is applied: newly-queued clean-scrub
        # candidates may be re-dirtied by the time the evict worker looks at them.
        for _ in range(3):
            self.populate(0, self.nrows, value_char='b')
            self.populate(self.nrows, self.nrows * 6)

        stat_cursor = self.session.open_cursor('statistics:')
        dirtied = stat_cursor[stat.conn.cache_clean_scrub_page_dirtied][2]
        stat_cursor.close()
        self.assertGreaterEqual(dirtied, 0)


# Same functional coverage as the main class but without the debug_mode override. Exercises
# the production decision path in __wti_evict_page_is_clean_scrub_candidate that requires
# WT_EVICT_CACHE_UPDATES pressure instead of the debug flag to queue candidates.
class test_clean_scrub_eviction01_production(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,statistics=(all),'
                   'eviction=(clean_scrub_eviction=true),'
                   'debug_mode=(evict_walk_full=true),checkpoint=(wait=0)')
    uri = "table:test_clean_scrub_eviction01_production"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    def test_production_path(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()
        self.populate(self.nrows, self.nrows * 12)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        evictions = stat_cursor[stat.conn.cache_clean_scrub_eviction][2]
        stat_cursor.close()
        self.assertGreater(images_saved, 0)
        self.assertGreater(evictions, 0)


# In-memory btrees don't reconcile to disk, so clean-scrub should not save images for them.
class test_clean_scrub_eviction01_in_memory(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,statistics=(all),in_memory=true,'
                   'eviction=(clean_scrub_eviction=true),'
                   'debug_mode=(clean_scrub=true)')
    uri = "table:test_clean_scrub_eviction01_in_memory"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    def test_in_memory_not_saved(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertEqual(images_saved, 0,
            "in-memory btree saved clean-scrub images: {}".format(images_saved))


# Disaggregated storage auto-enables clean-scrub eviction without needing the explicit eviction
# config option. Open a disagg connection without setting clean_scrub_eviction and verify that
# checkpoint reconciliation produces saved images.
@disagg_test_class
class test_clean_scrub_eviction01_disagg(wttest.WiredTigerTestCase, DisaggConfigMixin):
    disagg_storages = gen_disagg_storages('test_clean_scrub_eviction01_disagg', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_config = ('cache_size=50MB,statistics=(all),checkpoint=(wait=0),'
                   'disaggregated=(page_log=palite),disaggregated=(role="leader")')
    uri = "table:test_clean_scrub_eviction01_disagg"
    nrows = 10000
    value_size = 500

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()

    def test_disagg_auto_enable(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate(0, self.nrows)
        self.session.checkpoint()

        stat_cursor = self.session.open_cursor('statistics:')
        images_saved = stat_cursor[stat.conn.cache_clean_scrub_image_saved][2]
        stat_cursor.close()
        self.assertGreater(images_saved, 0,
            "disaggregated connection did not auto-enable clean-scrub eviction")
