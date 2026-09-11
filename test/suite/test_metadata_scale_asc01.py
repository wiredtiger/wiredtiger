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

# Creating many table: URIs in classic (ASC) WiredTiger: metadata file size vs
# ident count. Pair with test_disagg_metadata_scale01 for vs_asc.

import wttest
from metadata_scale import (
    ASC_KEYS_PER_IDENT, SCALE_RESULTS, above_baseline, asc_keys, fmt_bytes,
    ident_name, measure_local, scan_metadata)

class test_metadata_scale_asc01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=1GB,statistics=(all)'

    table_config = 'key_format=q,value_format=u'
    scales = (1000, 5000, 10000)
    extrapolate = (1_000_000, 10_000_000)

    def _report_checkpoint_impact(self, name, pre_file, pre_file_len):
        post = measure_local(self.session)
        file_key = f'file:{name}.wt'
        post_file_len = len(post['local'][file_key])
        self.prhead('checkpoint impact on ident 0:')
        self.prhead(
            f'  WiredTiger.wt {fmt_bytes(pre_file)} -> {fmt_bytes(post["wt_file"])} '
            f'({fmt_bytes(post["wt_file"] - pre_file)})')
        self.prhead(f'  {file_key} value {pre_file_len} -> {post_file_len} bytes')

    def _row(self, n, keys, wt, payload):
        self.prhead(
            f'{n:10d} {keys:10d}'
            f' {fmt_bytes(wt):>12}'
            f' {fmt_bytes(payload):>12}')

    def _report(self, baseline, snaps):
        size_keys = ('wt_file', 'payload')
        self.prhead(
            'ASC metadata size vs ident count. Rows beyond the measured '
            'scales use per-ident cost at the largest measured N. '
            'WiredTiger.wt per-ident falls with N as allocation slack shrinks.')
        header = f'{"idents":>10} {"asc keys":>10} {"asc_wt":>12} {"payload":>12}'
        self.prhead(header)
        self._row(0, 0, baseline['wt_file'], baseline['payload'])
        for n, snap in snaps:
            self._row(n, snap['user_keys'], snap['wt_file'], snap['payload'])

        n, snap = snaps[-1]
        self.prhead(f'per-ident cost at {n} idents (above baseline):')
        per = {}
        for key in size_keys:
            per[key] = above_baseline(snap, baseline, key) / n
            self.prhead(f'  {key}: {per[key]:.1f} B/ident')

        for target in self.extrapolate:
            self._row(
                target, target * ASC_KEYS_PER_IDENT,
                baseline['wt_file'] + per['wt_file'] * target,
                baseline['payload'] + per['payload'] * target)

        if len(snaps) >= 2:
            n0, s0 = snaps[-2]
            n1, s1 = snaps[-1]
            self.prhead(f'linearity {n0} vs {n1} (per-ident, above baseline):')
            for key in size_keys:
                a = above_baseline(s0, baseline, key) / n0
                b = above_baseline(s1, baseline, key) / n1
                rel = abs(a - b) / max(a, b) if max(a, b) else 0
                self.prhead(
                    f'  {key}: {a:.1f} vs {b:.1f} B/ident '
                    f'({rel * 100:.1f}% apart)')

    def test_table_ident_metadata_scale(self):
        self.session.checkpoint()
        baseline = measure_local(self.session)
        self.assertEqual(baseline['user_keys'], 0)

        name0 = ident_name(0)
        self.session.create(f'table:{name0}', self.table_config)
        local = scan_metadata(self.session)
        for key in asc_keys(name0):
            self.assertIn(key, local, f'missing ASC metadata key {key}')

        self.prhead(
            f'ASC create config {len(self.table_config)} bytes; stored value lengths:')
        for key in asc_keys(name0):
            self.prhead(f'  {key}: {len(local[key])} bytes')
            self.pr(local[key])

        pre = measure_local(self.session)
        pre_file_len = len(local[f'file:{name0}.wt'])
        self.session.checkpoint()
        self._report_checkpoint_impact(name0, pre['wt_file'], pre_file_len)
        self.assertEqual(
            measure_local(self.session)['user_keys'], ASC_KEYS_PER_IDENT)

        created = 1
        snaps = []
        for n in self.scales:
            for i in range(created, n):
                self.session.create(f'table:{ident_name(i)}', self.table_config)
                if (i + 1) % 1000 == 0:
                    self.prhead(f'created {i + 1} tables')
            created = n
            self.session.checkpoint()
            snap = measure_local(self.session)
            self.assertEqual(snap['user_keys'], n * ASC_KEYS_PER_IDENT)
            for key in ('wt_file', 'payload', 'user_payload'):
                self.assertGreater(snap[key], baseline[key])
            snaps.append((n, snap))

        SCALE_RESULTS['asc'] = (baseline, snaps)
        self._report(baseline, snaps)

        n0, s0 = snaps[-2]
        n1, s1 = snaps[-1]
        a = above_baseline(s0, baseline, 'user_payload') / n0
        b = above_baseline(s1, baseline, 'user_payload') / n1
        rel = abs(a - b) / max(a, b)
        self.assertLess(
            rel, 0.15,
            f'user_payload is not linear: {a:.1f} vs {b:.1f} B/ident')
