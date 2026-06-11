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

import json, os, subprocess
import wttest
from helper_disagg import DisaggConfigMixin, DisaggCorruptionMixin
from metadata_helper import get_table_id
from run import wt_builddir
from suite_subprocess import suite_subprocess

# test_util_read_corrupt_disagg.py
#    Disaggregated-storage counterpart to test_util_read_corrupt.py.
#    Mirrors the dump test matrix against a layered: URI backed by a
#    palite page log. Corruption is injected by DisaggCorruptionMixin
#    against the palite SQLite store.
#
# Each test mirrors the same-named test in the non-disagg file. The two
# files share the contract; only the corruption mechanism differs.
@wttest.skip_for_hook("tiered", "wt does not run under tiered hook")
class test_util_read_corrupt_disagg(wttest.WiredTigerTestCase,
                                    suite_subprocess,
                                    DisaggConfigMixin,
                                    DisaggCorruptionMixin):
    uri = "layered:test_util_read_corrupt_disagg"
    stable_uri = "file:test_util_read_corrupt_disagg.wt_stable"
    nrows = 1000

    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    # The wt subprocess opens its own connection in follower mode against
    # the same on-disk cell. Mirrors test_disagg_util02's pattern.
    def _wt_follower_config(self):
        return self.extensionsConfig() + ',disaggregated=(role="follower")'

    # Returns (rc, stdout, stderr) for a wt invocation in follower mode.
    # Bypasses runWt so we get the exact return code (matches the
    # subprocess.call pattern used in test_util_read_corrupt.py). Closes
    # the test framework's WT connection first so the wt subprocess can
    # acquire the home's file lock; reopens after.
    def _run_wt_follower(self, *args, out='wt.out', err='wt.err'):
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        cmd = [wtexe, '-C', self._wt_follower_config()] + list(args)
        self.close_conn()
        try:
            with open(out, 'w') as o, open(err, 'w') as e:
                rc = subprocess.call(cmd, stdout=o, stderr=e)
        finally:
            self.open_conn()
        with open(out) as o:
            stdout = o.read()
        with open(err) as e:
            stderr = e.read()
        return rc, stdout, stderr

    # Pad values so 1000 rows produce many leaf pages (rather than a
    # single fat leaf). With ~1 KB values and a 32 KB leaf_page_max the
    # table ends up with ~32 leaves and 1 root, giving us room to corrupt
    # one leaf without the whole table becoming unreachable.
    value_pad = 'x' * 1000

    def _populate(self, uri=None):
        uri = uri or self.uri
        self.session.create(uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}" + self.value_pad
        c.close()
        self.session.checkpoint()

    # Find the newest base-image page (full snapshot, not a delta) for
    # the stable file underlying our layered URI. Picks a base image
    # rather than a delta so corruption breaks a real on-disk page and
    # is reachable via the cursor walk. DisaggCorruptionMixin random
    # picker can land on the shared metadata or a system table; we want
    # corruption to land squarely on the user table's data, so we
    # query palite ourselves with the user table's table_id.
    def _find_user_table_base_page(self):
        table_id = get_table_id(self.session, self.stable_uri)
        # Take the modal page-data size: leaves all run at the same
        # split-target size (leaf_page_max with reconciliation overhead),
        # so the mode is reliably a leaf. Internal/root pages have
        # different (and typically smaller) sizes.
        all_rows = self.sqlite_select_json(
            table_id,
            f'SELECT page_id, lsn, length(page_data) AS sz FROM pages '
            f'WHERE table_id={table_id} AND base_lsn=0 AND backlink_lsn=0 '
            f'ORDER BY page_id;')
        self.assertTrue(all_rows,
            f'no base-image rows for table_id={table_id} in palite')
        from collections import Counter
        modal_size = Counter(r['sz'] for r in all_rows).most_common(1)[0][0]
        leaves = [r for r in all_rows if r['sz'] == modal_size]
        # Pick a leaf at ~25% through the page_id range. Avoids both
        # extremes (first leaf may host the smallest keys, last leaf the
        # largest) so iteration crosses readable leaves on both sides.
        chosen = leaves[len(leaves) // 4]
        return table_id, chosen['page_id'], chosen['lsn']

    # Targeted corruption: overwrite the page_data blob with random
    # bytes for the chosen (table_id, page_id, lsn). Uses
    # DisaggCorruptionMixin._palite_mutate for the SQL execution so we
    # share the close-conn / reopen-conn / sqlite-binary plumbing with
    # the shared corruption helper.
    def _corrupt_user_table_base_page(self):
        table_id, page_id, lsn = self._find_user_table_base_page()
        self.close_conn()
        sql = (f"UPDATE pages SET page_data = randomblob(length(page_data)) "
               f"WHERE table_id={table_id} AND page_id={page_id} AND lsn={lsn};\n"
               f"SELECT changes();\n")
        rows = self._palite_mutate(table_id, sql)
        self._require_one_change(rows, table_id, page_id, lsn)
        self.open_conn()

    # setup_corrupt_leaf_table: populate the user table and corrupt one
    # base-image page in palite. Mirrors the helper of the same name in
    # the non-disagg file.
    def setup_corrupt_leaf_table(self):
        self._populate()
        self._corrupt_user_table_base_page()

    def _parse_dumped_keys(self, dump_output):
        """Extract the set of keys from a wt dump (print-format) output.
        Each record is two lines (key + value); keys are 'kNNNNNNNN'
        followed by '\\00' (the print-format null terminator escape)."""
        keys = set()
        for line in dump_output.splitlines():
            if (len(line) == 12 and line.endswith('\\00')
              and line.startswith('k')
              and line[1:9].isdigit()):
                keys.add(line[:9])
        return keys

    # ---------- dump ----------

    def test_dump_without_q_fails_disagg(self):
        # Baseline: dump on corrupt disagg data must panic. Mirrors
        # test_dump_without_q_fails in the non-disagg file. rc semantics
        # differ between debug builds (SIGABRT) and release builds
        # (panic without abort), so the load-bearing check is the
        # WT_PANIC marker on stderr, not the exact rc value.
        self.setup_corrupt_leaf_table()
        rc, _, stderr = self._run_wt_follower(
            'dump', self.uri,
            out='dump_no_q.out', err='dump_no_q.err')
        self.assertNotEqual(rc, 0,
            f'wt dump on corrupt disagg exited 0; -q semantics may '
            f'have leaked into the default path')
        self.assertIn('WT_PANIC', stderr,
            'wt dump on corrupt disagg exited non-zero without panicking')

    def test_dump_with_q_produces_partial_output_disagg(self):
        # Graceful counterpart: -q runs the walker which skips bad pages
        # and continues. Output is partial; rc == 1 to flag the skip.
        # Asserts that records before and after the corrupt page made it
        # through, and that the total is less than nrows (something was
        # skipped) and more than half (didn't give up early).
        self.setup_corrupt_leaf_table()
        rc, stdout, stderr = self._run_wt_follower(
            '-q', 'dump', self.uri,
            out='dump_q.out', err='dump_q.err')
        self.assertEqual(rc, 1,
            f'wt -q dump on corrupt disagg returned rc={rc}; expected 1')
        self.assertNotIn('WT_PANIC', stderr,
            'wt -q dump panicked despite -q on disagg storage')
        self.assertTrue(stdout.startswith('WiredTiger Dump (WiredTiger Version'),
            f'wt -q dump stdout did not begin with the expected header; '
            f'got first 80 chars: {stdout[:80]!r}')
        first_key = 'k%08d\\00\n' % 0
        last_key = 'k%08d\\00\n' % (self.nrows - 1)
        self.assertIn(first_key, stdout,
            'wt -q dump stdout missing the first key; walker may not have '
            'started or may have failed on the very first leaf')
        self.assertIn(last_key, stdout,
            'wt -q dump stdout missing the last key; the walker did not '
            'skip past corruption to reach later leaves')
        record_count = sum(1 for line in stdout.splitlines()
                           if len(line) == 12 and line.endswith('\\00')
                           and line.startswith('k')
                           and line[1:9].isdigit())
        self.assertLess(record_count, self.nrows,
            f'wt -q dump emitted all {self.nrows} records; corruption '
            f'was not exercised (count={record_count})')
        self.assertGreater(record_count, self.nrows // 2,
            f'wt -q dump emitted very few records (count={record_count}); '
            f'walker may have stopped early instead of skipping the corrupt '
            f'leaf')

    def test_dump_with_q_skipped_keys_are_contiguous_disagg(self):
        # Stricter: parse the emitted keys and assert the missing set is
        # a single contiguous range. Catches "walker emits garbage"
        # and "walker drops records outside the corrupt page" regressions.
        self.setup_corrupt_leaf_table()
        rc, stdout, _ = self._run_wt_follower(
            '-q', 'dump', self.uri,
            out='dump_q.out', err='dump_q.err')
        self.assertEqual(rc, 1)
        emitted = self._parse_dumped_keys(stdout)
        expected = {'k%08d' % i for i in range(self.nrows)}
        missing = expected - emitted
        extras = emitted - expected
        self.assertEqual(extras, set(),
            f'walker emitted records that were never in the table: {extras!r}')
        self.assertGreater(len(missing), 0,
            'walker emitted every record; corruption was not exercised')
        self.assertLess(len(missing), self.nrows // 4,
            f'walker dropped too many records ({len(missing)} of '
            f'{self.nrows}); should have skipped one corrupt page')
        missing_idx = sorted(int(k[1:]) for k in missing)
        gaps = [missing_idx[i + 1] - missing_idx[i]
                for i in range(len(missing_idx) - 1)]
        self.assertTrue(all(g == 1 for g in gaps),
            f'missing keys are not contiguous: {missing_idx!r}. The walker '
            f'emitted records past the corrupt page but also dropped some '
            f'non-corrupt ones')

    def _run_dump_q(self, *extra_argv, outname='dump_opt.out',
                    errname='dump_opt.err'):
        return self._run_wt_follower(
            '-q', 'dump', *extra_argv, self.uri,
            out=outname, err=errname)

    def test_dump_q_json_skips_corrupt_disagg(self):
        # JSON dump under -q on disagg storage. Pins: rc == 1, the JSON
        # parses, first and last keys present, count is less than nrows.
        self.setup_corrupt_leaf_table()
        rc, stdout, _ = self._run_dump_q('-j',
                                         outname='dump_q_json.out',
                                         errname='dump_q_json.err')
        self.assertEqual(rc, 1, f'wt -q -j dump rc={rc}, expected 1')
        doc = json.loads(stdout)
        self.assertIn(self.uri, doc, 'dump JSON missing URI key')
        records = doc[self.uri][1]['data']
        keys = [r['key0'] for r in records]
        self.assertIn('k%08d' % 0, keys)
        self.assertIn('k%08d' % (self.nrows - 1), keys)
        self.assertLess(len(keys), self.nrows,
            'JSON dump emitted all records; corruption not exercised')

    def test_dump_q_hex_skips_corrupt_disagg(self):
        # Hex dump under -q on disagg storage. The last key encoded as
        # 'k00000999\0' must appear after the skip; weak "is this hex"
        # assertions would pass even when the cursor stopped early.
        self.setup_corrupt_leaf_table()
        rc, stdout, _ = self._run_dump_q('-x',
                                         outname='dump_q_hex.out',
                                         errname='dump_q_hex.err')
        self.assertEqual(rc, 1)
        lines = [ln for ln in stdout.splitlines() if ln]
        self.assertIn('Data', lines,
            'hex dump missing "Data" section marker')
        data_lines = lines[lines.index('Data') + 1:]
        self.assertTrue(all(all(c in '0123456789abcdef' for c in ln)
                            for ln in data_lines),
            'hex dump produced non-hex output lines')
        last_key_hex = (('k%08d' % (self.nrows - 1)) + '\0').encode().hex()
        self.assertIn(last_key_hex, data_lines,
            'hex dump did not emit the last key after skipping corruption')

    def test_dump_q_reverse_skips_corrupt_disagg(self):
        # Reverse iteration under -q on disagg storage. First emitted key
        # must be the last in the table; last emitted must be the first.
        self.setup_corrupt_leaf_table()
        rc, stdout, _ = self._run_dump_q('-r',
                                         outname='dump_q_rev.out',
                                         errname='dump_q_rev.err')
        self.assertEqual(rc, 1)
        ordered_keys = []
        for line in stdout.splitlines():
            if (len(line) == 12 and line.endswith('\\00')
                    and line.startswith('k')
                    and line[1:9].isdigit()):
                ordered_keys.append(line[:9])
        self.assertGreater(len(ordered_keys), 0,
            'reverse dump emitted nothing')
        self.assertEqual(ordered_keys[0], 'k%08d' % (self.nrows - 1),
            f'reverse dump did not start at the last key '
            f'(got {ordered_keys[0]!r}); iteration not in reverse')
        self.assertEqual(ordered_keys[-1], 'k%08d' % 0,
            f'reverse dump did not reach the first key after the skip '
            f'(last emitted: {ordered_keys[-1]!r})')

    def test_dump_q_bounds_skips_corrupt_disagg(self):
        # Bounded iteration under -q on disagg storage. Bounds span the
        # full table; the walker must honor the bound contract and skip
        # the corrupt page.
        self.setup_corrupt_leaf_table()
        rc, stdout, _ = self._run_dump_q(
            '-l', 'k%08d\\00' % 0,
            '-u', 'k%08d\\00' % (self.nrows - 1),
            outname='dump_q_b.out',
            errname='dump_q_b.err')
        self.assertEqual(rc, 1)
        emitted = self._parse_dumped_keys(stdout)
        self.assertIn('k%08d' % 0, emitted)
        self.assertIn('k%08d' % (self.nrows - 1), emitted)
        self.assertLess(len(emitted), self.nrows)

    # ---------- multi-URI -q -j across corrupt-root tables ----------
    #
    # On ASC we corrupt a specific table's root by overwriting bytes in
    # the .wt file. On disagg the root lives in palite as a row in the
    # pages table. DisaggCorruptionMixin corrupt_random_page_image
    # picks any random row; for a multi-URI continuation test we need
    # the corruption to land on a specific table's root. Until that
    # helper exists, this test is omitted on disagg (the non-disagg
    # variant covers the dispatch contract; the disagg block-manager
    # respect for the session quiet flag is already exercised by the
    # other dump tests above).

if __name__ == '__main__':
    wttest.run()
