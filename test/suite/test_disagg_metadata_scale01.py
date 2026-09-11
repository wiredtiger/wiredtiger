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

# Creating many table: URIs with type=layered: metadata file size vs ident
# count, whether growth is linear, and the size regression versus ASC when
# test_metadata_scale_asc01 has already run in this process.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from metadata_scale import (
    DSC_KEYS_PER_IDENT, SCALE_RESULTS, SHARED_KEYS_PER_IDENT, SHARED_META,
    above_baseline, dsc_keys, fmt_bytes, fmt_ratio, ident_name, measure_dsc,
    scan_metadata, shared_keys, user_entries)
from wtscenario import make_scenarios

@disagg_test_class
class test_disagg_metadata_scale01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=1GB,statistics=(all),' \
        + 'disaggregated=(role="leader",lose_all_my_data=true)'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    table_config = 'key_format=q,value_format=u,type=layered,block_manager=disagg'
    scales = (1000, 5000, 10000)
    extrapolate = (1_000_000, 10_000_000)

    def _report_checkpoint_impact(self, name, pre_file, pre_shared, pre_stable_len):
        post = measure_dsc(self.session)
        stable_key = f'file:{name}.wt_stable'
        post_stable_len = len(post['local'][stable_key])
        self.prhead('checkpoint impact on ident 0:')
        self.prhead(
            f'  WiredTiger.wt {fmt_bytes(pre_file)} -> {fmt_bytes(post["wt_file"])} '
            f'({fmt_bytes(post["wt_file"] - pre_file)})')
        self.prhead(f'  shared user keys {pre_shared} -> {post["shared_keys"]}')
        self.prhead(
            f'  {stable_key} value {pre_stable_len} -> {post_stable_len} bytes')

    def _asc_snap_at(self, n):
        asc = SCALE_RESULTS.get('asc')
        if not asc:
            return None, None
        baseline, snaps = asc
        for count, snap in snaps:
            if count == n:
                return baseline, snap
        return None, None

    def _vs_asc(self, n, snap, baseline):
        asc_base, asc_snap = self._asc_snap_at(n)
        if not asc_snap:
            return None
        dsc = above_baseline(snap, baseline, 'wt_file') + \
            above_baseline(snap, baseline, 'shared_ckpt')
        asc = above_baseline(asc_snap, asc_base, 'wt_file')
        return dsc, asc

    def _row(self, n, keys, shared, wt, sh, vs_s):
        self.prhead(
            f'{n:10d} {keys:10d} {shared:10d}'
            f' {fmt_bytes(wt):>12}'
            f' {fmt_bytes(sh):>12}'
            f' {fmt_bytes(wt + sh):>12}'
            f' {vs_s:>8}')

    def _report(self, baseline, snaps):
        size_keys = ('wt_file', 'shared_ckpt', 'payload', 'shared_payload')
        self.prhead(
            'DSC metadata size vs ident count; vs_asc is '
            '(WiredTiger.wt + shared ckpt) / ASC WiredTiger.wt, '
            'above empty-metadata baseline (run test_metadata_scale_asc01 first)')
        self.prhead(
            'vs_asc is small at 100/1k because ASC WiredTiger.wt still has '
            'allocation slack; shared payload is linear. Rows beyond the '
            'measured scales use per-ident cost at the largest measured N.')
        header = (
            f'{"idents":>10} {"dsc keys":>10} {"shared":>10}'
            f' {"dsc_wt":>12} {"dsc_shared":>12} {"dsc_total":>12} {"vs_asc":>8}')
        self.prhead(header)
        self._row(0, 0, 0, baseline['wt_file'], baseline['shared_ckpt'], 'n/a')
        for n, snap in snaps:
            vs = self._vs_asc(n, snap, baseline)
            self._row(
                n, snap['user_keys'], snap['shared_keys'],
                snap['wt_file'], snap['shared_ckpt'],
                fmt_ratio(*vs) if vs else 'n/a')

        n, snap = snaps[-1]
        self.prhead(f'per-ident cost at {n} idents (above baseline):')
        per = {}
        for key in size_keys:
            per[key] = above_baseline(snap, baseline, key) / n
            self.prhead(f'  {key}: {per[key]:.1f} B/ident')
        vs_est = self._vs_asc(n, snap, baseline)
        if vs_est:
            self.prhead(f'  vs_asc: {fmt_ratio(*vs_est)}')

        vs_s = fmt_ratio(*vs_est) if vs_est else 'n/a'
        for target in self.extrapolate:
            self._row(
                target, target * DSC_KEYS_PER_IDENT, target * SHARED_KEYS_PER_IDENT,
                baseline['wt_file'] + per['wt_file'] * target,
                baseline['shared_ckpt'] + per['shared_ckpt'] * target, vs_s)

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
        baseline = measure_dsc(self.session)
        self.assertEqual(baseline['user_keys'], 0)
        self.assertEqual(baseline['shared_keys'], 0)

        name0 = ident_name(0)
        self.session.create(f'table:{name0}', self.table_config)
        local = scan_metadata(self.session)
        for key in dsc_keys(name0):
            self.assertIn(key, local, f'missing local metadata key {key}')
        self.assertNotIn(f'file:{name0}.wt_ingest', scan_metadata(self.session, SHARED_META))

        self.prhead(
            f'DSC create config {len(self.table_config)} bytes; stored value lengths:')
        for key in dsc_keys(name0):
            self.prhead(f'  {key}: {len(local[key])} bytes')
            self.pr(local[key])

        pre = measure_dsc(self.session)
        pre_shared = len(user_entries(scan_metadata(self.session, SHARED_META)))
        pre_stable_len = len(local[f'file:{name0}.wt_stable'])

        # Shared metadata is queued until checkpoint; file: rows gain a
        # checkpoint=() cookie. Measure both sides of that first checkpoint.
        self.session.checkpoint()
        self._report_checkpoint_impact(name0, pre['wt_file'], pre_shared, pre_stable_len)

        shared = scan_metadata(self.session, SHARED_META)
        for key in shared_keys(name0):
            self.assertIn(key, shared, f'missing shared metadata key {key}')
        self.assertEqual(len(user_entries(shared)), SHARED_KEYS_PER_IDENT)

        created = 1
        snaps = []
        for n in self.scales:
            for i in range(created, n):
                self.session.create(f'table:{ident_name(i)}', self.table_config)
                if (i + 1) % 1000 == 0:
                    self.prhead(f'created {i + 1} tables')
            created = n
            self.session.checkpoint()
            snap = measure_dsc(self.session)
            self.assertEqual(snap['user_keys'], n * DSC_KEYS_PER_IDENT)
            self.assertEqual(snap['shared_keys'], n * SHARED_KEYS_PER_IDENT)
            for key in (
                    'wt_file', 'shared_ckpt', 'payload', 'shared_payload',
                    'user_payload', 'shared_user_payload'):
                self.assertGreater(snap[key], baseline[key])
            snaps.append((n, snap))

        SCALE_RESULTS['dsc'] = (baseline, snaps)
        self._report(baseline, snaps)

        n0, s0 = snaps[-2]
        n1, s1 = snaps[-1]
        for key, limit in (
                ('shared_user_payload', 0.05),
                ('user_payload', 0.15)):
            a = above_baseline(s0, baseline, key) / n0
            b = above_baseline(s1, baseline, key) / n1
            rel = abs(a - b) / max(a, b)
            self.assertLess(
                rel, limit,
                f'{key} is not linear: {a:.1f} vs {b:.1f} B/ident')
