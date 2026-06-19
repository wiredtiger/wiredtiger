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

import json
import os
import re
import subprocess

from helper import WiredTigerCursor
from helper_disagg import DisaggCorruptionMixin
from metadata_helper import get_table_id
from run import wt_builddir
from suite_subprocess import suite_subprocess
import wttest

class test_util_read_corrupt_unified(wttest.WiredTigerTestCase,
                                     DisaggCorruptionMixin,
                                     suite_subprocess):
    uri = 'test_util_read_corrupt'
    nrows = 1000
    value_pad = 'x' * 1000

    def _is_disagg(self):
        return 'disagg' in self.hook_names

    def _effective_uri(self, base=None):
        if base is None:
            base = self.uri
        if self._is_disagg():
            return 'layered:' + base
        return 'table:' + base

    def _stable_uri(self, base=None):
        if base is None:
            base = self.uri
        return 'file:' + base + '.wt_stable'

    def _verify_target(self, base=None):
        if base is None:
            base = self.uri
        if self._is_disagg():
            return self._stable_uri(base)
        return 'file:' + base + '.wt'

    def _wt_path(self):
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        return wtexe

    def _wt_follower_config(self):
        page_log_extension = wttest.WiredTigerTestCase.findExtension(
            'page_log', 'palite')
        if not page_log_extension:
            raise RuntimeError('palite page_log extension not found')
        return ('extensions=["' + page_log_extension[0] + '"],'
                'disaggregated=(role="follower",page_log=palite)')

    def run_wt(self, *args, out='wt.out', err='wt.err'):
        cmd = [self._wt_path()]
        if self._is_disagg():
            cmd.extend(['-C', self._wt_follower_config()])
        cmd.extend(args)
        self.close_conn()
        with open(out, 'w') as o, open(err, 'w') as e:
            rc = subprocess.call(cmd, stdout=o, stderr=e)
        with open(out) as o:
            stdout = o.read()
        with open(err) as e:
            stderr = e.read()
        return rc, stdout, stderr

    def _corrupt_asc_leaf(self, offset, base=None):
        if base is None:
            base = self.uri
        with open(base + '.wt', 'r+b') as f:
            f.seek(offset + 64)
            f.write(b'\xde\xad\xbe\xef' * 16)

    def _corrupt_disagg_leaf(self, table_id, ident):
        page_id, lsn = ident
        sql = (f"UPDATE pages SET page_data = randomblob(length(page_data)) "
               f"WHERE table_id={table_id} AND page_id={page_id} AND lsn={lsn};\n"
               f"SELECT changes();\n")
        rows = self._palite_mutate(table_id, sql)
        self._require_one_change(rows, table_id, page_id, lsn)

    def _live_leaves(self, base=None):
        self.close_conn()
        verify_uri = self._verify_target(base)
        cmd = [self._wt_path()]
        if self._is_disagg():
            cmd.extend(['-C', self._wt_follower_config()])
        cmd.extend(['verify', '-d', 'dump_address', verify_uri])
        out = subprocess.run(cmd, capture_output=True, text=True)
        leaves = []
        for line in out.stdout.splitlines():
            if 'row-store leaf' not in line:
                continue
            if self._is_disagg():
                m = re.search(r'page_id:\s*(\d+),\s*disagg_lsn:\s*(\d+)', line)
                if m:
                    leaves.append((int(m.group(1)), int(m.group(2))))
            else:
                # The root has a different "Root:" / "> addr:" preamble, so this
                # regex (which requires the "address:" prefix) skips it.
                m = re.search(r'address:\s*\[0:\s*(\d+)-\d+', line)
                if m:
                    leaves.append(int(m.group(1)))
        return leaves

    # Return the root's identifier: a file offset (ASC) or (page_id, lsn) (disagg).
    def _root_addr(self, base=None):
        self.close_conn()
        cmd = [self._wt_path()]
        if self._is_disagg():
            cmd.extend(['-C', self._wt_follower_config()])
        cmd.extend(['verify', '-d', 'dump_address', self._verify_target(base)])
        out = subprocess.run(cmd, capture_output=True, text=True)
        lines = out.stdout.splitlines()
        for i, line in enumerate(lines):
            if line.strip() != 'Root:' or i + 1 >= len(lines):
                continue
            addr = lines[i + 1]
            if self._is_disagg():
                m = re.search(r'>\s*addr:\s*\[\s*(\d+),\s*\d+,\s*(\d+)', addr)
                if m:
                    return (int(m.group(1)), int(m.group(2)))
            else:
                m = re.search(r'>\s*addr:\s*\[0:\s*(\d+)-\d+', addr)
                if m:
                    return int(m.group(1))
        self.fail('root address not found in `wt verify -d dump_address` output')

    def corrupt_one_leaf(self, base=None):
        if base is None:
            base = self.uri
        table_id = (get_table_id(self.session, self._stable_uri(base))
                    if self._is_disagg() else None)
        leaves = self._live_leaves(base)
        self.assertTrue(leaves, 'no row-store leaf found in `wt verify -d dump_address` output')
        # Middle leaf so the first/last keys live on either side of the skip.
        chosen = leaves[len(leaves) // 2]
        if self._is_disagg():
            self._corrupt_disagg_leaf(table_id, chosen)
        else:
            self._corrupt_asc_leaf(chosen, base)

    # The _corrupt_*_leaf helpers aren't leaf-specific -- they take any address.
    def corrupt_root(self, base=None):
        if base is None:
            base = self.uri
        table_id = (get_table_id(self.session, self._stable_uri(base))
                    if self._is_disagg() else None)
        root = self._root_addr(base)
        if self._is_disagg():
            self._corrupt_disagg_leaf(table_id, root)
        else:
            self._corrupt_asc_leaf(root, base)

    def populate(self, base=None, value_tag='v'):
        with WiredTigerCursor(self.session, self._effective_uri(base)) as c:
            for i in range(self.nrows):
                c['k%08d' % i] = '%s%08d' % (value_tag, i) + self.value_pad

    def setup_corrupt_leaf_table(self):
        self.session.create(self._effective_uri(), 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_one_leaf()

    def setup_corrupt_root_table(self):
        self.session.create(self._effective_uri(), 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_root()

    # Keys that didn't show up in `wt -q dump`, i.e. the keys on the corrupted
    # leaf. Used to pin read/stat tests to known-unreachable keys.
    def _missing_keys(self):
        rc, stdout, _ = self.run_wt('-q', 'dump', self._effective_uri())
        self.assertEqual(rc, 0, 'helper dump failed: rc=%d' % rc)
        found = set()
        for line in stdout.splitlines():
            if (len(line) == 12 and line.startswith('k') and line.endswith('\\00')
                    and line[1:9].isdigit()):
                found.add(line[:9])
        return [f'k{i:08d}' for i in range(self.nrows) if f'k{i:08d}' not in found]

    # Without -q, corrupt-leaf dump panics.
    def test_dump_without_q(self):
        self.setup_corrupt_leaf_table()
        rc, _, stderr = self.run_wt('dump', self._effective_uri())
        self.assertNotEqual(rc, 0, 'wt dump returned rc=%d; expected non-zero' % rc)
        self.assertIn('WT_PANIC', stderr, 'wt dump stderr missing WT_PANIC despite corruption')

    # With -q, corrupt-leaf dump exits 0 with partial output (first/last keys present).
    def test_dump_with_q_corrupt_page(self):
        self.setup_corrupt_leaf_table()
        rc, stdout, stderr = self.run_wt('-q', 'dump', self._effective_uri())
        self.assertEqual(rc, 0, 'wt -q dump returned rc=%d; expected 0' % rc)
        self.assertNotIn('WT_PANIC', stderr, 'wt -q dump panicked despite -q')

        first_key = 'k%08d\\00\n' % 0
        last_key = 'k%08d\\00\n' % (self.nrows - 1)
        self.assertIn(first_key, stdout, 'wt -q dump stdout missing the first key')
        self.assertIn(last_key, stdout, 'wt -q dump stdout missing the last key')

        record_count = sum(1 for line in stdout.splitlines()
                           if len(line) == 12 and line.endswith('\\00')
                           and line.startswith('k') and line[1:9].isdigit())
        self.assertLess(record_count, self.nrows,
            'wt -q dump emitted all %d records; corruption was not exercised '
            '(count=%d)' % (self.nrows, record_count))
        self.assertGreater(record_count, self.nrows // 2,
            'wt -q dump emitted only %d records' % record_count)

    # With -q, corrupt-root dump emits no records. The exact failure point
    # differs (ASC fails at cursor open; disagg fails at first next()), so we
    # just assert no panic and no records.
    def test_dump_with_q_corrupted_root(self):
        self.setup_corrupt_root_table()
        _, stdout, stderr = self.run_wt('-q', 'dump', self._effective_uri())
        self.assertNotIn('WT_PANIC', stderr, 'wt -q dump panicked despite -q')
        record_count = sum(1 for line in stdout.splitlines()
                           if len(line) == 12 and line.endswith('\\00')
                           and line.startswith('k') and line[1:9].isdigit())
        self.assertEqual(record_count, 0,
            'wt -q dump emitted %d data records with the root corrupted' % record_count)

    # Two tables in one dump (corrupt-leaf + corrupt-root). Distinct value
    # prefixes let us tell records apart. The envelope must round-trip through
    # json.loads -- under -q a skipped URI must not orphan a separator or close
    # an unopened table.
    def test_dump_with_q_and_json(self):
        base_leaf = self.uri + '_leaf'
        base_root = self.uri + '_root'
        self.session.create(self._effective_uri(base_leaf), 'key_format=S,value_format=S')
        self.session.create(self._effective_uri(base_root), 'key_format=S,value_format=S')
        self.populate(base_leaf, value_tag='vleaf')
        self.populate(base_root, value_tag='vroot')
        self.session.checkpoint()

        # Snapshot disagg table ids before the verify subprocess closes the conn.
        if self._is_disagg():
            tid_leaf = get_table_id(self.session, self._stable_uri(base_leaf))
            tid_root = get_table_id(self.session, self._stable_uri(base_root))
        else:
            tid_leaf = tid_root = None

        leaves = self._live_leaves(base_leaf)
        self.assertTrue(leaves, 'no leaf addresses found for leaf table')
        leaf_addr = leaves[len(leaves) // 2]
        root_addr = self._root_addr(base_root)

        if self._is_disagg():
            self._corrupt_disagg_leaf(tid_leaf, leaf_addr)
            self._corrupt_disagg_leaf(tid_root, root_addr)
        else:
            self._corrupt_asc_leaf(leaf_addr, base_leaf)
            self._corrupt_asc_leaf(root_addr, base_root)

        _, stdout, stderr = self.run_wt('-q', 'dump', '-j',
                                        self._effective_uri(base_leaf),
                                        self._effective_uri(base_root))
        self.assertNotIn('WT_PANIC', stderr, 'wt -q dump -j panicked despite -q')

        try:
            json.loads(stdout)
        except ValueError as exc:
            self.fail('wt -q dump -j emitted malformed JSON: %s\noutput:\n%s' % (exc, stdout))

        # Leaf table: partial output with first and last values present.
        self.assertIn('vleaf%08d' % 0, stdout, 'leaf-table first value missing from JSON')
        self.assertIn('vleaf%08d' % (self.nrows - 1), stdout,
            'leaf-table last value missing from JSON')
        leaf_count = stdout.count('"value0" : "vleaf')
        self.assertLess(leaf_count, self.nrows,
            'leaf-table emitted all %d records; corruption not exercised' % self.nrows)
        self.assertGreater(leaf_count, self.nrows // 2,
            'leaf-table emitted only %d records' % leaf_count)

        # Root table: nothing readable.
        root_count = stdout.count('"value0" : "vroot')
        self.assertEqual(root_count, 0,
            'root-table emitted %d records; expected none with corrupted root'
            % root_count)

    # Without -q, reading a corrupt-leaf key panics.
    def test_read_without_q(self):
        self.setup_corrupt_leaf_table()
        missing = self._missing_keys()
        self.assertTrue(missing, 'expected at least one missing key after corruption')
        rc, _, stderr = self.run_wt('read', self._effective_uri(), missing[0])
        self.assertNotEqual(rc, 0, 'wt read returned rc=%d; expected non-zero' % rc)
        self.assertIn('WT_PANIC', stderr, 'wt read stderr missing WT_PANIC despite corruption')

    # With -q, reading clean + corrupt keys: clean values print, no panic.
    def test_read_with_q_corrupt_leaf(self):
        self.setup_corrupt_leaf_table()
        missing = self._missing_keys()
        self.assertTrue(missing, 'expected at least one missing key after corruption')
        first_key = 'k%08d' % 0
        last_key = 'k%08d' % (self.nrows - 1)
        rc, stdout, stderr = self.run_wt(
            '-q', 'read', self._effective_uri(), first_key, missing[0], last_key)
        self.assertNotIn('WT_PANIC', stderr, 'wt -q read panicked despite -q')
        self.assertNotEqual(rc, 0,
            'wt -q read returned rc=%d; expected non-zero' % rc)
        self.assertIn('v%08d' % 0, stdout, 'wt -q read stdout missing first key value')
        self.assertIn('v%08d' % (self.nrows - 1), stdout, 'wt -q read stdout missing last key value')

    # With -q + corrupt root, no key is readable but no panic.
    def test_read_with_q_corrupted_root(self):
        self.setup_corrupt_root_table()
        any_key = 'k%08d' % (self.nrows // 2)
        rc, _, stderr = self.run_wt('-q', 'read', self._effective_uri(), any_key)
        self.assertNotEqual(rc, 0, 'wt -q read returned rc=%d; expected non-zero' % rc)
        self.assertNotIn('WT_PANIC', stderr, 'wt -q read panicked despite -q')

    # Without -q, stat walks the table and panics on the corrupt leaf.
    def test_stat_without_q(self):
        self.setup_corrupt_leaf_table()
        rc, _, stderr = self.run_wt('stat', self._effective_uri())
        self.assertNotEqual(rc, 0, 'wt stat returned rc=%d; expected non-zero' % rc)
        self.assertIn('WT_PANIC', stderr, 'wt stat stderr missing WT_PANIC despite corruption')

    # With -q, stat tolerates the corrupt leaf and still emits stats.
    def test_stat_with_q_corrupt_leaf(self):
        self.setup_corrupt_leaf_table()
        rc, stdout, stderr = self.run_wt('-q', 'stat', self._effective_uri())
        self.assertEqual(rc, 0, 'wt -q stat returned rc=%d; expected 0' % rc)
        self.assertNotIn('WT_PANIC', stderr, 'wt -q stat panicked despite -q')
        self.assertTrue(stdout.strip(), 'wt -q stat produced no output')

if __name__ == '__main__':
    wttest.run()
