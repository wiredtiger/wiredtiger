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
# test_btree_usage_internal_key.py
#   WT-17717 internal separator-key-size sampling.
#   Verify the insert-path spine walk populates the internal (above-leaf) key-size
#   levels for a tree built entirely in memory (no disk reads). A small
#   memory_page_max forces enough in-memory splits to build a 3+ level tree so
#   level 2 is exercised as well as level 1.
import re, wttest

class test_btree_usage_internal_key(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(fast),' + \
        'file_manager=(close_scan_interval=1,close_idle_time=100,close_handle_minimum=250)'

    def detail(self):
        det = {}
        c = self.session.open_cursor('statistics:', None, None)
        try:
            while c.next() == 0:
                d, _vs, v = c.get_value()
                m = re.match(r'^usage_\(id=\d+\)_ik[^:]*: (.*)$', d)
                if m is not None:
                    det[m.group(1)] = v
        finally:
            c.close()
        return det

    def test_internal_key(self):
        # Small leaf + memory_page_max => many in-memory splits => deep tree, no disk reads.
        self.session.create('table:ik',
            'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=8KB,memory_page_max=256KB')
        c = self.session.open_cursor('table:ik')
        N_L1 = 'sampled key-size observation count one level above the leaf'
        S_L1 = 'sampled key-size byte sum one level above the leaf'
        N_L2 = 'sampled key-size observation count two levels above the leaf'
        max_l1 = max_l2 = l1_sum_at_max = 0
        key = 0
        for _ in range(30):
            for _ in range(40000):
                c[('%016d' % key)] = 'v' * 20
                key += 1
            det = self.detail()
            if det.get(N_L1, 0) > max_l1:
                max_l1 = det[N_L1]; l1_sum_at_max = det.get(S_L1, 0)
            max_l2 = max(max_l2, det.get(N_L2, 0))
            if max_l1 > 0 and max_l2 > 0:
                break
        c.close()
        self.pr('INTKEY  L1_n=%d L1_mean=%s  L2_n=%d' %
                (max_l1, (l1_sum_at_max // max_l1 if max_l1 else 0), max_l2))
        # Internal key sizes must be sampled from the in-memory spine (no disk reads happened).
        self.assertGreater(max_l1, 0, 'expected level-1 internal key sizes to be sampled')
        self.assertGreater(max_l2, 0, 'expected level-2 internal key sizes (multi-level ascend)')
