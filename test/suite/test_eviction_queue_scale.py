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
# Test that enabling eviction.scale_queue_to_cache_size on a large cache does not
# break correctness and still produces dirty evictions under write pressure.
#
# At connection open (__wt_evict_create), evict_target_slots is set to
# clamp(cache_gb * 100, WTI_EVICT_WALK_BASE, WTI_EVICT_WALK_BASE_MAX) when the
# flag is enabled -- otherwise it stays at WTI_EVICT_WALK_BASE + WTI_EVICT_WALK_INCR.
# evict_target_slots sizes the LRU queue allocation and also acts as the
# pressure-graduated denominator in __evict_walk_target.
#
# The per-pass walker budget (WTI_EVICT_WALK_INCR) is constant in both paths, so
# the per-btree target_pages histogram bucket distribution is similar in both
# configurations. This test therefore only asserts correctness (no data loss)
# and that dirty evictions occur under pressure.

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios


class test_eviction_queue_scale(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('legacy_small', dict(
            cache_size='100MB',
            scale_queue=False,
            dirty_target=20,
            dirty_trigger=30,
            nrows=80000,
        )),
        ('legacy_large', dict(
            cache_size='4GB',
            scale_queue=False,
            dirty_target=1,
            dirty_trigger=2,
            nrows=100000,
        )),
        ('scaled_large', dict(
            cache_size='4GB',
            scale_queue=True,
            dirty_target=1,
            dirty_trigger=2,
            nrows=100000,
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
        uri = 'table:test_evict_queue_scale'
        self.session.create(uri, 'key_format=i,value_format=S')

        value = 'x' * 1024  # 1 KB per row
        cursor = self.session.open_cursor(uri)
        for i in range(self.nrows):
            cursor[i] = value
        cursor.close()

        self.session.checkpoint()

        cursor = self.session.open_cursor(uri)
        count = sum(1 for _ in cursor)
        self.assertEqual(count, self.nrows,
            f'Expected all {self.nrows} rows readable after eviction '
            f'with cache_size={self.cache_size}')
        cursor.close()

        dirty_evicted = self.get_stat(stat.conn.cache_eviction_dirty)
        self.assertGreater(dirty_evicted, 0,
            f'Expected dirty evictions with cache_size={self.cache_size} '
            f'(dirty_trigger={self.dirty_trigger}%)')


if __name__ == '__main__':
    wttest.run()
