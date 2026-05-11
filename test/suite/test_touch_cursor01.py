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
#
# [TEST_TAGS]
# cursors
# search
# [END_TAGS]

# test_touch_cursor01.py
# Functional + performance characterization of the skunk_94 touch cursor.
#
# The touch cursor walks the btree using only internal pages and forwards a
# fire-and-forget warmup hint to the page log layer (PALI) via
# plh_get(flags=WT_PAGE_LOG_WARMUP); cursor->search returns WT_NOTFOUND.
#
# This test:
#   1. Verifies the cursor open succeeds and search() always returns WT_NOTFOUND.
#   2. Runs a scaled-down version of the case-94 workload (UUID -> JSON payload
#      table plus a birthday index) with a deliberately small cache, so the
#      working set never fits in cache and every iteration is forced through
#      palite. With palite's touch_sim_enabled=true latencies engaged, the
#      touch-warmup path should be measurably faster than the no-touch path
#      over multiple repeats.
#
# The simulation knobs live entirely in palite; they default to disabled so
# every other palite-backed test in the suite sees zero behavior change.

import json, random, time, uuid, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


@disagg_test_class
class test_touch_cursor01(wttest.WiredTigerTestCase, DisaggConfigMixin):

    # Latency knobs in milliseconds. Picked to be small enough to keep the
    # whole test under ~3 minutes on Evergreen, large enough that wall-clock
    # signal dominates Python/SQLite overhead. Cold:warm = 5:1 leaves enough
    # headroom that the assertion below is robust to ~25% timing jitter.
    cold_ms = 5
    warm_ms = 1
    warmup_ms = 1

    # Working-set sizing. nitems x value_bytes >> cache_size_mb so the touch
    # benefit shows up on every iteration, not just iter 0.
    nitems = 20_000
    value_bytes = 256
    repeats = 3
    age_low = 40
    age_high = 60
    age_ref_year = 2026

    # Make scenarios for different page-log implementations. Only palite is
    # built locally, so this is effectively a single-scenario test in CI.
    disagg_storages = gen_disagg_storages('test_touch_cursor01', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # palite extension config: enable the touch-cursor latency simulation.
    # Because the simulation is opt-in (touch_sim_enabled=true), other palite
    # tests are unaffected.
    disagg_config = (
        f'touch_sim_enabled=true,'
        f'touch_sim_cold_ms={cold_ms},'
        f'touch_sim_warm_ms={warm_ms},'
        f'touch_sim_warmup_ms={warmup_ms}'
    )

    # Two cache sizes. We populate with the bigger one so 20k inserts don't fight
    # eviction; we measure with the tight one so every leaf read has to spill
    # back through palite, which is what the case-94 cold-storage scenario is
    # about.
    populate_cache_mb = 64
    measure_cache_mb = 1

    conn_base_config = (
        'transaction_sync=(enabled,method=fsync),'
        'statistics=(all),'
        'disaggregated=(page_log=palite),'
    )

    def conn_config(self):
        return self.conn_base_config + (
            f'cache_size={self.populate_cache_mb}MB,'
            'disaggregated=(role="leader"),'
        )

    def _tighten_cache(self):
        """Switch the connection to the measurement cache size."""
        self.conn.reconfigure(f'cache_size={self.measure_cache_mb}MB')

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    # ---- helpers -------------------------------------------------------

    def _populate(self, idx_uri, main_uri, seed=0xC0FFEE):
        idx = self.session.open_cursor(idx_uri, None, None)
        main = self.session.open_cursor(main_uri, None, None)
        rng = random.Random(seed)
        pad = 'x' * max(0, self.value_bytes - 80)
        for _ in range(self.nitems):
            year = rng.randint(1900, 2020)
            month = rng.randint(1, 12)
            day = rng.randint(1, 28)
            bday = f'{year:04d}{month:02d}{day:02d}'
            u = uuid.UUID(int=rng.getrandbits(128)).hex
            houses = rng.randint(0, 5)
            payload = json.dumps(
                {'year': year, 'houses': houses, 'name': f'p_{u[:8]}', 'pad': pad}
            )
            main[u] = payload
            idx[f'{bday}|{u}'] = u
        main.close()
        idx.close()
        self.session.checkpoint()

    def _collect_uuids(self, idx_uri):
        min_year = self.age_ref_year - self.age_high
        max_year = self.age_ref_year - self.age_low
        lo = f'{min_year:04d}0101'
        hi = f'{max_year:04d}1231|\xff'
        c = self.session.open_cursor(idx_uri, None, None)
        out = []
        try:
            c.set_key(lo)
            sn = c.search_near()
            if sn == wiredtiger.WT_NOTFOUND:
                return out
            while c.get_key() < lo:
                if c.next() == wiredtiger.WT_NOTFOUND:
                    return out
            while c.get_key() <= hi:
                out.append(c.get_value())
                if c.next() == wiredtiger.WT_NOTFOUND:
                    break
            return out
        finally:
            c.close()

    def _warmup(self, main_uri, uuids):
        t = self.session.open_cursor(
            main_uri, None, 'touch=(enabled=true,action=warmup)'
        )
        try:
            for u in uuids:
                t.set_key(u)
                rc = t.search()
                self.assertEqual(rc, wiredtiger.WT_NOTFOUND,
                                 f'touch.search must return WT_NOTFOUND, got {rc}')
        finally:
            t.close()

    def _sum_houses(self, main_uri, uuids):
        c = self.session.open_cursor(main_uri, None, None)
        total = 0
        try:
            for u in uuids:
                c.set_key(u)
                if c.search() == 0:
                    total += json.loads(c.get_value())['houses']
            return total
        finally:
            c.close()

    def _reopen(self):
        """Close+reopen the connection to drop the WT cache between repeats.

        We reopen with the measurement cache size; populate is the one phase
        that needs the bigger cache and that happens before any _reopen.
        """
        config = self.conn_base_config + (
            f'cache_size={self.measure_cache_mb}MB,'
            'disaggregated=(role="leader"),'
        )
        # Step down to avoid shutdown checkpoint, drop conn, reopen.
        meta = self.disagg_get_complete_checkpoint_meta()
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.close_conn()
        self.open_conn(
            ".",
            config + f'disaggregated=(checkpoint_meta="{meta}"),',
        )

    # ---- tests ---------------------------------------------------------

    def test_touch_cursor_returns_notfound(self):
        """Functional: touch.search returns WT_NOTFOUND on hit, miss and partial keys."""
        uri = 'file:test_touch_func.wt'
        self.session.create(uri,
                            'key_format=S,value_format=S,block_manager=disagg,'
                            'allocation_size=512,leaf_page_max=512,'
                            'internal_page_max=512')
        c = self.session.open_cursor(uri, None, None)
        for i in range(500):
            c[f'k{i:05d}'] = f'v{i:05d}'
        c.close()
        self.session.checkpoint()
        self._reopen()

        # Hit key
        t = self.session.open_cursor(uri, None,
                                     'touch=(enabled=true,action=warmup)')
        for k in ('k00007', 'k00250', 'k00499', 'NOTAKEY', ''):
            t.set_key(k if k else 'x')  # WT rejects empty string keys.
            self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND)
        t.close()

        # Normal cursor still works (no state leak from the touch cursor).
        c = self.session.open_cursor(uri, None, None)
        c.set_key('k00250')
        self.assertEqual(c.search(), 0)
        self.assertEqual(c.get_value(), 'v00250')
        c.close()

    def test_touch_cursor_perf(self):
        """Performance: touch path is measurably faster across multiple repeats."""
        idx_uri = 'file:test_touch_idx.wt'
        main_uri = 'file:test_touch_main.wt'
        common = (
            'key_format=S,value_format=S,block_manager=disagg,'
            'allocation_size=512,leaf_page_max=1KB,internal_page_max=512'
        )
        self.session.create(idx_uri, common)
        self.session.create(main_uri, common)

        self.pr(f'populating {self.nitems} rows ...')
        t0 = time.perf_counter()
        self._populate(idx_uri, main_uri)
        self.pr(f'  populate took {time.perf_counter() - t0:.1f}s')

        # Establish the working set once -- both paths use the exact same
        # uuid list (drawn from the same seed), so per-iteration comparison is
        # apples-to-apples.
        self._reopen()
        uuids = self._collect_uuids(idx_uri)
        self.assertGreater(len(uuids), 100,
                           'age window matched too few uuids; bump nitems')
        self.pr(f'  matched {len(uuids)} uuids in age {self.age_low}..{self.age_high}')

        # Path A: no touch. Each iteration reopens to drop the WT cache, so
        # every iteration is forced back through palite at cold latency.
        a_times = []
        for i in range(self.repeats):
            self._reopen()
            t0 = time.perf_counter()
            total_a = self._sum_houses(main_uri, uuids)
            dt = time.perf_counter() - t0
            a_times.append(dt)
            self.pr(f'  no-touch  iter {i}: {dt*1000:7.1f} ms  sum={total_a}')

        # Path B: touch once, then run the same repeats. palite's warm-set is
        # process-static keyed by db_home, so the warm bits survive the
        # reopen() between iterations.
        self._reopen()
        t0 = time.perf_counter()
        self._warmup(main_uri, uuids)
        warmup_s = time.perf_counter() - t0
        self.pr(f'  warmup pass: {warmup_s*1000:.1f} ms')

        b_times = []
        for i in range(self.repeats):
            self._reopen()
            t0 = time.perf_counter()
            total_b = self._sum_houses(main_uri, uuids)
            dt = time.perf_counter() - t0
            b_times.append(dt)
            self.pr(f'  touched   iter {i}: {dt*1000:7.1f} ms  sum={total_b}')

        self.assertEqual(total_a, total_b,
                         'touch and no-touch paths must compute the same sum')

        avg_a = sum(a_times) / len(a_times)
        avg_b = sum(b_times) / len(b_times)
        speedup = avg_a / avg_b if avg_b > 0 else float('inf')
        self.pr(f'== summary: avg_no_touch={avg_a*1000:.1f} ms, '
                f'avg_touch={avg_b*1000:.1f} ms, speedup={speedup:.2f}x ==')

        # Functional invariant: touch path must be measurably faster on average
        # over the configured repeats. Threshold picked conservatively for
        # noisy CI machines (Evergreen variance can be 30%+); the local laptop
        # number with this config is in the 2.5x-3.5x range.
        self.assertGreater(
            speedup, 1.3,
            f'expected >= 1.3x speedup, got {speedup:.2f}x '
            f'(avg_no_touch={avg_a*1000:.1f} ms, avg_touch={avg_b*1000:.1f} ms)'
        )
