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
# test_btree_usage_overwrite.py
#   WT-17717 insert-overwrite classification: a new-key insert and an insert
#   that overwrites an existing key must land in separate op buckets.
import re, wttest

class test_btree_usage_overwrite(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(fast),' + \
        'file_manager=(close_scan_interval=1,close_idle_time=100,close_handle_minimum=250)'

    def sums(self):
        # Return (sum of INSERT counts, sum of INSERT_OVERWRITE counts) across positions for our btree.
        ins = ovr = 0
        c = self.session.open_cursor('statistics:', None, None)
        try:
            while c.next() == 0:
                d, _vs, v = c.get_value()
                m = re.match(r'^usage_\(id=\d+\)_ow[^:]*: (.*)$', d)
                if m is None:
                    continue
                f = m.group(1)
                if re.match(r'number of sampled insert-overwrites on|near ', f):
                    ovr += v
                elif re.match(r'number of sampled inserts on|near ', f):
                    ins += v
        finally:
            c.close()
        return ins, ovr

    def test_overwrite(self):
        self.session.create('table:ow', 'key_format=S,value_format=S')
        cur = self.session.open_cursor('table:ow')
        val = 'v' * 40
        # Phase 1: distinct new keys -> INSERT (compare != 0, new key).
        for k in range(200000):
            cur['%012d' % k] = val
        ins_after_new, ovr_after_new = self.sums()
        # Phase 2: hammer one existing key -> INSERT_OVERWRITE (compare == 0).
        hot = '%012d' % 100000
        max_ovr = 0
        for _ in range(20):
            for _ in range(40000):
                cur[hot] = val
            _ins, ovr = self.sums()
            max_ovr = max(max_ovr, ovr)
            if max_ovr > 0:
                break
        cur.close()
        self.pr('OVERWRITE  inserts(after new-key phase)=%d  max insert-overwrites=%d' %
                (ins_after_new, max_ovr))
        # New keys were classified as inserts ...
        self.assertGreater(ins_after_new, 0, 'expected new-key inserts to be sampled')
        # ... and re-setting an existing key is classified separately as insert-overwrite.
        self.assertGreater(max_ovr, 0, 'expected insert-overwrites to be sampled separately')
