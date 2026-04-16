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

# test_eviction_queue_scale.py
#
# Test that the eviction queue working set (evict_walk_base) and per-walk budget
# (evict_walk_incr) scale correctly with the configured cache size when the
# eviction.scale_queue_to_cache_size option is enabled.
#
# Scaling formula (computed at connection open in __wt_evict_create when enabled):
#
#   evict_walk_base = clamp(cache_gb * 100, WTI_EVICT_WALK_BASE, WTI_EVICT_WALK_BASE_MAX)
#   evict_walk_incr = evict_walk_base / 3
#   evict_slots     = evict_walk_base + evict_walk_incr
#
# Without the flag: compile-time defaults (base=300, incr=100, slots=400) are used.
# With the flag and a 4 GB cache: base=400, incr=133, slots=533.
#
# The per-btree page target in __evict_get_target_pages is capped at remaining_slots,
# which equals evict_walk_incr when visiting the first btree in a pass.  This has a
# direct effect on the target-page histogram stats:
#
#   flag=false (100 MB or 4 GB): evict_walk_incr=100 -> target capped at 100 -> lt128
#   flag=true  (4 GB):           evict_walk_incr=133 -> target capped at 133 -> ge128

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios


class test_eviction_queue_scale(wttest.WiredTigerTestCase):
    """
    Verify that:
      - Without the flag, the compile-time defaults are used regardless of cache size.
      - With the flag enabled, the queue scales with cache size and the observable
        target-page histogram shifts accordingly.
      - Eviction remains functionally correct in all cases.

    Scenarios:
      legacy_small  flag=false, 100 MB -> incr=100, target always lt128
      legacy_large  flag=false, 4 GB  -> incr=100, target always lt128 (flag off)
      scaled_large  flag=true,  4 GB  -> incr=133, target can reach ge128
    """

    scenarios = make_scenarios([
        ('legacy_small', dict(
            cache_size='100MB',
            scale_queue=False,
            # target=20, trigger=30: eviction starts at 30 MB dirty out of 100 MB.
            # 80 K rows * 1 KB = ~80 MB exceeds the trigger.
            dirty_target=20,
            dirty_trigger=30,
            nrows=80000,
            # flag=false -> evict_walk_incr=100, target capped at 100 < 128.
            expect_ge128=False,
        )),
        ('legacy_large', dict(
            cache_size='4GB',
            scale_queue=False,
            # target=1, trigger=2: eviction starts at ~80 MB dirty out of 4 GB.
            # 100 K rows * 1 KB = ~100 MB exceeds the trigger.
            dirty_target=1,
            dirty_trigger=2,
            nrows=100000,
            # flag=false -> evict_walk_incr still 100 even with 4 GB cache.
            expect_ge128=False,
        )),
        ('scaled_large', dict(
            cache_size='4GB',
            scale_queue=True,
            dirty_target=1,
            dirty_trigger=2,
            nrows=100000,
            # flag=true, cache_gb=4 -> evict_walk_base=400, evict_walk_incr=133 >= 128.
            expect_ge128=True,
        )),
    ])

    def conn_config(self):
        scale_flag = 'true' if self.scale_queue else 'false'
        return (
            f'cache_size={self.cache_size},'
            f'eviction=(scale_queue_to_cache_size={scale_flag}),'
            f'eviction_dirty_target={self.dirty_target},'
            f'eviction_dirty_trigger={self.dirty_trigger},'
            f'statistics=(all)'
        )

    def get_stat(self, stat_key):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat_key][2]
        stat_cursor.close()
        return val

    def test_evict_queue_scales_with_cache_size(self):
        """
        Insert data to exceed the dirty-eviction trigger, then verify:

          1. All inserted rows survive eviction and remain readable — confirms no
             data corruption in either the minimum or the scaled code path.

          2. Dirty evictions actually occurred — confirms the eviction server ran
             and the queue was populated during the writes.

          3. The target-page histogram reflects the evict_walk_incr value:
               sub_gb_cache (incr=100): ge128 bucket must stay zero, lt128 must
                 fire (target pages are always capped at 100 < 128).
               4gb_cache (incr=133): ge128 bucket must be non-zero (target pages
                 are capped at 133 >= 128 on the first btree visit each pass).
        """
        uri = 'table:test_evict_queue_scale'
        self.session.create(uri, 'key_format=i,value_format=S')

        value = 'x' * 1024  # 1 KB per row
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = value
        cursor.close()

        # Checkpoint to ensure the eviction server has had an opportunity to run.
        self.session.checkpoint()

        # All rows must survive eviction and remain readable.
        cursor = self.session.open_cursor(uri)
        count = sum(1 for _ in cursor)
        self.assertEqual(count, self.nrows,
            f'Expected all {self.nrows} rows readable after eviction '
            f'with cache_size={self.cache_size}')
        cursor.close()

        # Dirty evictions must have occurred: the dirty trigger was exceeded and
        # the eviction server should have processed dirty pages.
        dirty_evicted = self.get_stat(stat.conn.cache_eviction_dirty)
        self.assertGreater(dirty_evicted, 0,
            f'Expected dirty evictions with cache_size={self.cache_size} '
            f'(dirty_trigger={self.dirty_trigger}%)')

        target_ge128 = self.get_stat(stat.conn.cache_eviction_target_page_ge128)
        target_lt128 = self.get_stat(stat.conn.cache_eviction_target_page_lt128)
        target_lt256 = self.get_stat(stat.conn.cache_eviction_target_page_lt256)
        target_lt512 = self.get_stat(stat.conn.cache_eviction_target_page_lt512)
        target_ge512 = self.get_stat(stat.conn.cache_eviction_target_page_ge512)

        if self.expect_ge128:
            # Scaled path (4 GB, evict_walk_incr=133): the per-btree target is
            # capped at 133 on the first btree visit each pass.
            #
            # ge128 (total >= 128):    must be non-zero.
            # lt256 (sub-bucket 128-255): must be non-zero since 133 falls here.
            # lt512 and ge512:          must be zero (133 < 256).
            self.assertGreater(target_ge128, 0,
                'Expected ge128 eviction targets with 4 GB scaled cache '
                '(evict_walk_incr=133 >= 128)')
            self.assertGreater(target_lt256, 0,
                'Expected lt256 sub-bucket hits with 4 GB scaled cache '
                '(target capped at 133, which falls in [128, 256))')
            self.assertEqual(target_lt512, 0,
                'Expected no lt512 hits (target capped at 133 < 256)')
            self.assertEqual(target_ge512, 0,
                'Expected no ge512 hits (target capped at 133 < 512)')
            # ge128 must equal the sum of the three sub-buckets.
            self.assertEqual(target_ge128, target_lt256 + target_lt512 + target_ge512,
                'ge128 must equal the sum of the lt256 + lt512 + ge512 sub-buckets')
        else:
            # Minimum path (evict_walk_incr=100): the per-btree target is always
            # capped at 100 < 128, so the ge128 bucket and all sub-buckets must
            # be zero.
            self.assertEqual(target_ge128, 0,
                'Expected no ge128 eviction targets '
                '(evict_walk_incr=100 < 128)')
            self.assertEqual(target_lt256, 0,
                'Expected no lt256 sub-bucket hits (evict_walk_incr=100 < 128)')
            self.assertEqual(target_lt512, 0,
                'Expected no lt512 sub-bucket hits (evict_walk_incr=100 < 128)')
            self.assertEqual(target_ge512, 0,
                'Expected no ge512 sub-bucket hits (evict_walk_incr=100 < 128)')
            self.assertGreater(target_lt128, 0,
                'Expected lt128 eviction targets '
                '(target capped at evict_walk_incr=100)')


if __name__ == '__main__':
    wttest.run()
