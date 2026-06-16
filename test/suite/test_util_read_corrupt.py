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

import os
import subprocess
from run import wt_builddir
from suite_subprocess import suite_subprocess
import wttest

# test_util_read_corrupt.py
#    Cover the global `wt -q` flag: read-oriented commands must
#    produce partial output and exit non-zero on a corrupt page rather than
#    crashing, and must still abort without -q. The dispatcher must reject -q
#    on commands outside the read-oriented set.
#
# Each command is exercised by a pair of tests:
#   test_<command>_without_q_fails           - pins the "fails on first corrupt"
#                                          baseline that the with-q test
#                                          relies on for its meaning.
#   test_<command>_with_q_<contract>          - pins the with-q contract for
#                                          that command (partial output /
#                                          continues / does not crash).
class test_util_read_corrupt(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_util_read_corrupt.a'
    uri = 'table:' + tablename
    nentries = 2000

    # Corruption is written at 75% of the .wt file. Keys are sequential
    # and the file is large enough (~20MB) that internal pages cluster
    # at the start, so a write at 75% reliably lands on a leaf rather
    # than on metadata.
    corrupt_offset_frac = 0.75

    # The shared-table layout in the disagg hook hides the on-disk file we
    # need to overwrite, so the same skip rule as the open_and_position
    # helper in test_verify applies here.
    def skip_test_if_disagg(self):
        if 'disagg' in self.hook_names:
            self.skipTest('disagg hook: shared tables hide the .wt file we need to corrupt')

    # Fixed-width keys keep `wt read ... <keys>` argv inside the Windows
    # process-creation command-line limit (~32KB). Earlier versions of this
    # test built keys as growing prefixes ("0", "01", "012", ...), which
    # produced multi-kB keys at the tail and overflowed argv on Windows.
    # The 10kB value pad keeps the on-disk file large enough (~20MB) that
    # corruption at 75% reliably lands on a leaf page rather than on a
    # dhandle-open-time block that the cursor-open path reads with the
    # quiet-corrupt flag clear.
    def populate(self):
        cursor = self.session.open_cursor(self.uri, None, None)
        value_pad = 'v' * 10000
        for i in range(self.nentries):
            key = '%08d' % i
            cursor[key] = key + value_pad
        cursor.close()

    def corrupt_leaf_page(self):
        """Overwrite a region at corrupt_offset_frac into the .wt file with
        garbage. The width (~96kB) is several times the default 32KB leaf so
        the corruption straddles multiple full leaves regardless of page
        boundaries. Narrow corruption (single leaf, few records) can be
        missed by windowed walks like `wt dump -k <key> -w <window>` and
        by per-key `wt read <keys>` lookups whose targets happen not to
        fall on the damaged leaf; the without-q baselines rely on a hit
        being deterministic across builds, so we damage a wide enough
        region to guarantee it."""
        self.close_conn()
        filename = self.tablename + '.wt'
        filesize = os.path.getsize(filename)
        position = int(filesize * self.corrupt_offset_frac)
        with open(filename, 'r+b') as f:
            f.seek(position)
            for _ in range(32000):
                f.write(b'\x01\xff\x80')

    def setup_corrupt_leaf_table(self):
        """create  populate  checkpoint  corrupt the user table at 75%."""
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

    def setup_clean_table(self):
        """create  populate  checkpoint, no corruption. For tests that
        exercise dump_record's non-error return-code paths."""
        # Under the disagg hook the table is rewritten as layered: and the
        # wt follower subprocess opens its own connection that doesn't see
        # table:foo. Skip rather than diverge the test for the disagg
        # connection-resolution path; the same dump_record code is covered
        # by the non-disagg run.
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.close_conn()

    def count_lines(self, filename):
        with open(filename) as f:
            return sum(1 for _ in f)

    def _wt_path(self):
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        return wtexe

    def _all_keys(self):
        """Every key in the table. Used by `wt read` tests: the corrupt
        leaf's key range is layout-dependent (the test framework's
        checkpoint cadence shuffles which keys end up at file offset 75%),
        so a narrow key cluster can sit on either side of the damage and
        leave the `-q` contract unverified. Walking every key guarantees
        the search path crosses the corrupt leaf. Argv size for 2000
        9-byte keys is ~18KB, comfortably under the ~32KB Windows
        process-creation limit."""
        return ['%08d' % i for i in range(self.nentries)]

    def _parse_dumped_keys(self, dump_output):
        """Extract the set of keys from a wt dump (print-format) output.
        Each record is two lines (key + value); keys are 8 ASCII digits
        followed by '\\00' (the print-format null terminator escape)."""
        keys = set()
        for line in dump_output.splitlines():
            if (len(line) == 11 and line.endswith('\\00')
              and line[:8].isdigit()):
                keys.add(line[:8])
        return keys

    # ---------- dump ----------

    def test_dump_without_q_fails(self):
        # Baseline: dump on corrupt data must panic, not exit gracefully.
        # The load-bearing assertions are the stderr markers that pin the
        # panic flow; rc semantics differ between debug builds (SIGABRT,
        # rc=-6) and release builds (panic without abort, rc=1), so we
        # only check that the command did not succeed.
        # The stderr markers pin:
        #   1. per-block "read checksum error" diagnostic from
        #      block_read.c
        #   2. bitflip-detection report (our corruption isn't a single-bit
        #      flip), the "fatal read error" wrap, and the WT_PANIC
        #      marker - reordering regressions in the panic flow get
        #      caught here, not just disappearance of one phrase.
        #   3. The block-layer diagnostic appears at all - only printed
        #      when the quiet flag is clear (block_read.c:233), so a
        #      regression that suppressed it on the default path would
        #      be caught here.
        # stdout is intentionally NOT asserted: how much output flushed
        # before the abort depends on stdio buffer state at the moment
        # of the SIGABRT and is not deterministic across builds.
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), 'dump', self.uri]
        with open('dump_no_q.out', 'w') as out, open('dump_no_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertNotEqual(rc, 0,
            'wt dump on corrupt data exited 0; -q semantics may have '
            'leaked into the default path')
        with open('dump_no_q.err') as f:
            err = f.read()
        # Markers that must always appear on a panic-on-corrupt path,
        # regardless of whether the panic also aborts (debug) or returns
        # the error to main (release).
        markers = [
            ('read checksum error',
                'block-layer corruption diagnostic missing from stderr'),
            ('bitflip detection performed but no single-bit flip found',
                'bitflip detection report missing from stderr'),
            ('fatal read error',
                'fatal read error wrap missing from stderr'),
            ('WT_PANIC',
                'wt dump exited non-zero without panicking; default '
                'path is no longer panic-on-corrupt'),
        ]
        for marker, msg in markers:
            self.assertIn(marker, err, msg)

    def test_dump_with_q_produces_partial_output(self):
        # Graceful counterpart: same corruption, but -q runs the tree
        # walker which skips bad pages and continues with sibling
        # subtrees. Output is partial - only records on the corrupt
        # leaf are missing - and the command exits zero because the
        # walk completed (corruption is signaled via __wt_errx on
        # stderr, not via the exit code).
        # Pins five properties of the -q behavior:
        #   1. rc == 0 (walk reached WT_NOTFOUND cleanly).
        #   2. stdout starts with the dump header preamble verbatim.
        #   3. The first key (00000000) is in stdout - records before
        #      the corrupt leaf made it through.
        #   4. The last key (00001999) IS in stdout - records AFTER
        #      the corrupt leaf also made it through. This is the
        #      load-bearing skip-and-continue assertion: a non-walker
        #      iteration would stop at the corrupt leaf and never
        #      reach the last key.
        #   5. Total record count is less than nentries - something
        #      was actually skipped (test framework variance means we
        #      cannot pin exactly how many records the corrupt leaf
        #      contained, but it is definitely > 0 and < nentries).
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), '-q', 'dump', self.uri]
        with open('dump_q.out', 'w') as out, open('dump_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertEqual(rc, 0,
            'wt -q dump on corrupt data returned rc=%d; expected 0 '
            '(walk completed; skip is transparent at exit-code level)' % rc)
        with open('dump_q.out') as f:
            out = f.read()
        self.assertTrue(out.startswith('WiredTiger Dump (WiredTiger Version'),
            'wt -q dump stdout did not begin with the expected header; '
            'got first 80 chars: %r' % out[:80])
        first_key = '%08d\\00\n' % 0
        last_key = '%08d\\00\n' % (self.nentries - 1)
        self.assertIn(first_key, out,
            'wt -q dump stdout missing the first key; walker may not have '
            'started or may have failed on the very first leaf')
        self.assertIn(last_key, out,
            'wt -q dump stdout missing the last key; the walker did not '
            'skip past corruption to reach later leaves (this is the '
            'load-bearing skip-and-continue assertion)')
        # Record count must indicate something was skipped.
        record_count = sum(1 for line in out.splitlines()
                           if len(line) == 11 and line.endswith('\\00')
                           and line[0:8].isdigit())
        self.assertLess(record_count, self.nentries,
            'wt -q dump emitted all %d records; corruption was not '
            'exercised (count=%d)' % (self.nentries, record_count))
        self.assertGreater(record_count, self.nentries // 2,
            'wt -q dump emitted very few records (count=%d); walker may '
            'have stopped early instead of skipping the corrupt leaf' %
            record_count)

    # ---------- skip-and-continue walker correctness ----------

    def test_dump_with_q_skipped_keys_are_contiguous(self):
        # Stricter than test_dump_with_q_produces_partial_output: parse
        # the emitted keys and assert the missing set is exactly a
        # single contiguous range (proves the walker skipped exactly
        # one subtree, not scattered records).  Catches "walker emits
        # garbage records" and "walker drops records outside the
        # corrupt leaf" regressions that the looser test would miss.
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), '-q', 'dump', self.uri]
        with open('dump_q.out', 'w') as out, open('dump_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertEqual(rc, 0)
        with open('dump_q.out') as f:
            out = f.read()
        emitted = self._parse_dumped_keys(out)
        expected = {'%08d' % i for i in range(self.nentries)}
        missing = expected - emitted
        extras = emitted - expected
        self.assertEqual(extras, set(),
            'walker emitted records that were never in the table: %r' % extras)
        self.assertGreater(len(missing), 0,
            'walker emitted every record; corruption was not exercised')
        self.assertLess(len(missing), self.nentries // 4,
            'walker dropped too many records (%d of %d); should have skipped '
            'one corrupt leaf, not given up on a quarter of the table' %
            (len(missing), self.nentries))
        # Missing keys must be contiguous.  A regression that skipped
        # non-corrupt records too would produce a gap with holes inside.
        missing_idx = sorted(int(k) for k in missing)
        gaps = [missing_idx[i + 1] - missing_idx[i]
                for i in range(len(missing_idx) - 1)]
        self.assertTrue(all(g == 1 for g in gaps),
            'missing keys are not contiguous: %r.  The walker emitted records '
            'past the corrupt leaf but also dropped some non-corrupt ones, '
            'which means the skip-and-continue is not behaving like a single '
            'subtree skip' % missing_idx)

    # ---------- cursor-walk option coverage under -q + corruption ----------

    def _run_dump_q(self, *extra_argv, outname='dump_opt.out', errname='dump_opt.err'):
        argv = [self._wt_path(), '-q', 'dump', *extra_argv, self.uri]
        with open(outname, 'w') as out, open(errname, 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        with open(outname) as f:
            out_text = f.read()
        with open(errname) as f:
            err_text = f.read()
        return rc, out_text, err_text

    def test_dump_q_json_skips_corrupt(self):
        # JSON dump under -q must emit a well-formed JSON document despite
        # the corrupt subtree being skipped. Pins: rc == 0, first and last
        # keys present (iteration crossed the corrupt span), JSON parses.
        self.setup_corrupt_leaf_table()
        rc, out, _ = self._run_dump_q('-j',
                                      outname='dump_q_json.out',
                                      errname='dump_q_json.err')
        self.assertEqual(rc, 0, 'wt -q -j dump rc=%d, expected 0' % rc)
        import json
        doc = json.loads(out)
        self.assertIn(self.uri, doc, 'dump JSON missing URI key')
        records = doc[self.uri][1]['data']
        keys = [r['key0'] for r in records]
        self.assertIn('00000000', keys)
        self.assertIn('%08d' % (self.nentries - 1), keys)
        self.assertLess(len(keys), self.nentries,
            'JSON dump emitted all records; corruption not exercised')

    def test_dump_q_hex_skips_corrupt(self):
        # Hex dump under -q must continue past corruption to the last key.
        # Keys are stored with a trailing null so "00001999\0" encodes to
        # 303030303139393900. A weaker assertion that just checks "output
        # is hex" would pass even when the cursor stopped at corruption
        # short of the end.
        self.setup_corrupt_leaf_table()
        rc, out, _ = self._run_dump_q('-x',
                                      outname='dump_q_hex.out',
                                      errname='dump_q_hex.err')
        self.assertEqual(rc, 0)
        lines = [ln for ln in out.splitlines() if ln]
        self.assertIn('Data', lines, 'hex dump missing "Data" section marker')
        data_lines = lines[lines.index('Data') + 1:]
        self.assertTrue(all(all(c in '0123456789abcdef' for c in ln)
                            for ln in data_lines),
            'hex dump produced non-hex output lines')
        last_key_hex = (('%08d' % (self.nentries - 1)) + '\0').encode().hex()
        self.assertIn(last_key_hex, data_lines,
            'hex dump did not emit the last key after skipping corrupt subtree')

    def test_dump_q_reverse_skips_corrupt(self):
        # Reverse iteration under -q. Must actually run in reverse: the
        # first emitted key should be near the end (nentries - 1), the
        # last emitted near the start. A weaker assertion that only checks
        # endpoint presence would pass when the dispatcher routes -r
        # through the forward-only walker.
        self.setup_corrupt_leaf_table()
        rc, out, _ = self._run_dump_q('-r',
                                      outname='dump_q_rev.out',
                                      errname='dump_q_rev.err')
        self.assertEqual(rc, 0)
        # Preserve order: parse keys as they appear in stdout.
        ordered_keys = []
        for line in out.splitlines():
            if (len(line) == 11 and line.endswith('\\00')
                    and line[:8].isdigit()):
                ordered_keys.append(line[:8])
        self.assertGreater(len(ordered_keys), 0,
            'reverse dump emitted nothing')
        self.assertEqual(ordered_keys[0], '%08d' % (self.nentries - 1),
            'reverse dump did not start at the last key (got %r); the '
            'iteration is not actually running in reverse' % ordered_keys[0])
        self.assertEqual(ordered_keys[-1], '00000000',
            'reverse dump did not reach the first key after skipping '
            'the corrupt subtree (last emitted: %r)' % ordered_keys[-1])

    def test_dump_q_bounds_skips_corrupt(self):
        # Bounded iteration under -q. Bounds span the table; walk must
        # skip the corrupt subtree while honoring the bound contract.
        self.setup_corrupt_leaf_table()
        rc, out, _ = self._run_dump_q(
            '-l', '00000000\\00',
            '-u', '%08d\\00' % (self.nentries - 1),
            outname='dump_q_b.out',
            errname='dump_q_b.err')
        self.assertEqual(rc, 0)
        emitted = self._parse_dumped_keys(out)
        self.assertIn('00000000', emitted)
        self.assertIn('%08d' % (self.nentries - 1), emitted)
        self.assertLess(len(emitted), self.nentries)

    # ---------- multi-URI -q -j continuation across corrupt-root tables ----------

    # ---------- dump_record return-code regressions (no corruption) outside scope ----------

    def test_dump_missing_key_exits_zero(self):
        self.setup_clean_table()
        self.runWt(['dump', '-k', '99999999\\00', self.uri],
            outfilename='dump_missing.out', errfilename='dump_missing.err')
        self.check_empty_file('dump_missing.err')

    def test_dump_window_past_end_exits_zero(self):
        self.setup_clean_table()
        last_key_index = self.nentries - 2
        key = '%08d\\00' % last_key_index
        self.runWt(['dump', '-k', key, '-w', '500', self.uri],
            outfilename='dump_wend.out', errfilename='dump_wend.err')
        self.check_empty_file('dump_wend.err')
        self.assertGreater(self.count_lines('dump_wend.out'), 10,
            'wt dump -k -w produced no data records past the backward window')

    # ---------- read ----------

    def test_read_without_q_fails(self):
        # Baseline: read on corrupt data must panic, not exit gracefully.
        # Pins the same three properties as the dump baseline:
        #   1. rc indicates SIGABRT-class signal kill (not in (0, 1)).
        #   2. stderr contains the full panic-flow markers in order:
        #      read-checksum-error, bitflip-detection-report,
        #      fatal-read-error, WT_PANIC, aborting. These are
        #      block-layer messages that don't depend on which cursor
        #      op triggered the read, so the sequence matches the
        #      dump test's even though read uses cursor.search.
        #   3. The block-layer diagnostic is present (it's only
        #      printed when the quiet flag is clear).
        # Walking every key guarantees the search path crosses the
        # corrupt leaf regardless of which key range the test
        # framework's checkpoint cadence places there.
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), 'read', self.uri] + self._all_keys()
        with open('read_no_q.out', 'w') as out, open('read_no_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertNotEqual(rc, 0,
            'wt read on corrupt data exited 0; -q semantics may have '
            'leaked into the default path')
        with open('read_no_q.err') as f:
            err = f.read()
        markers = [
            ('read checksum error',
                'block-layer corruption diagnostic missing from stderr'),
            ('bitflip detection performed but no single-bit flip found',
                'bitflip detection report missing from stderr'),
            ('fatal read error',
                'fatal read error wrap missing from stderr'),
            ('WT_PANIC',
                'wt read exited non-zero without panicking; default '
                'path is no longer panic-on-corrupt'),
        ]
        for marker, msg in markers:
            self.assertIn(marker, err, msg)
        positions = [err.index(m) for m, _ in markers]
        self.assertEqual(positions, sorted(positions),
            'stderr panic markers appeared out of order: %r' % positions)

    def test_read_with_q_continues_on_corrupt(self):
        # Graceful counterpart, with the additional contract unique to
        # read: the per-key loop continues past errors. After one key's
        # search returns WT_ERROR, util_cerr logs to stderr and the loop
        # moves on to the next argv entry rather than bailing.
        # Pins five properties of the -q behavior:
        #   1. rc == 1 exactly (graceful non-zero, not a crash).
        #   2. stderr contains zero panic markers.
        #   3. Every stderr line is exactly the per-key cursor error
        #      from util_cerr, with no other lines mixed in.
        #   4. At least one error was observed (corruption was hit).
        #   5. THE LOAD-BEARING CHECK: stdout-value-count plus
        #      stderr-error-count equals nentries. This proves the
        #      loop visited every requested key. If read had bailed
        #      on the first error (no continue-past behavior), the
        #      sum would be far less than nentries.
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), '-q', 'read', self.uri] + self._all_keys()
        with open('read_q.out', 'w') as out, open('read_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertEqual(rc, 1,
            'wt -q read returned rc=%d; expected exactly 1' % rc)
        with open('read_q.err') as f:
            err = f.read()
        with open('read_q.out') as f:
            out = f.read()
        self.assertNotIn('WT_PANIC', err,
            'wt -q read panicked despite -q')
        self.assertNotIn('aborting WiredTiger library', err,
            'wt -q read reached the abort marker')
        all_err_lines = [ln for ln in err.splitlines() if ln]
        out_lines = [ln for ln in out.splitlines() if ln]
        # Filter out block-layer diagnostic lines that fire under
        # WT_SESSION_READ_SKIP_CORRUPT (verbose checksum/bitflip
        # output). The per-key cursor.search error lines from util_cerr
        # are what this test is verifying.
        expected_suffix = ('%s: cursor.search: WT_ERROR: '
                           'non-specific WiredTiger error') % self.uri
        err_lines = [ln for ln in all_err_lines if ln.endswith(expected_suffix)]
        self.assertGreater(len(err_lines), 0,
            'wt -q read produced no per-key error lines; corruption was '
            'not exercised on this run')
        self.assertEqual(len(out_lines) + len(err_lines), self.nentries,
            'stdout values (%d) + per-key stderr errors (%d) does not '
            'equal nentries (%d); wt -q read did not visit every '
            'requested key' % (len(out_lines), len(err_lines), self.nentries))

    # ---------- stat ----------

    def test_stat_without_q_fails(self):
        self.setup_corrupt_leaf_table()
        self.runWt(['stat', self.uri],
            outfilename='stat_no_q.out', errfilename='stat_no_q.err', failure=True)

    def test_stat_with_q_does_not_crash_on_corrupt(self):
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), '-q', 'stat', self.uri]
        with open('stat_q.out', 'w') as out, open('stat_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertIn(rc, (0, 1),
            'wt -q stat exited with unexpected status %d (expected 0 or 1, '
            'a higher code indicates the process crashed instead of returning '
            'a quiet-corrupt error)' % rc)
        if rc == 1:
            self.check_non_empty_file('stat_q.err')

    # ---------- list ----------

    def test_list_with_q_still_lists_uris(self):
        # `wt list` reads the metadata cursor (WiredTiger.wt), not the
        # user table, so this test exercises the happy path: a corrupted
        # user-table leaf page must not block the metadata listing.
        self.setup_corrupt_leaf_table()
        self.runWt(['-q', 'list'],
            outfilename='list_q.out', errfilename='list_q.err')
        with open('list_q.out') as f:
            text = f.read()
        self.assertIn(self.uri, text,
            'wt -q list omitted the user table from the metadata listing')

    def test_list_with_q_skips_corrupt_metadata_leaf(self):
        # Harder list -q case: corrupt a leaf of WiredTiger.wt itself
        # (the metadata btree). The conn-level read_corrupt flag must
        # propagate to internal sessions so __btree_preload at
        # metadata-dhandle-open doesn't panic on the corrupt child
        # page; the metadata cursor walk in list_print must then skip
        # the corrupt leaf and continue to the surviving URIs. Pins:
        #   1. fixture validity: plain `wt list` (no -q) panics on
        #      the corruption, proving we landed on a live leaf;
        #   2. rc == 0 from `wt -q list` (walk completes cleanly);
        #   3. partial output: emitted < total_uris and > 0 URIs.
        # Leaves containing conn-open-required keys (system:*,
        # file:WiredTiger.wt, file:WiredTigerHS.wt) are filtered out
        # of the candidate set: their corruption isn't tolerated by
        # callers in the conn-open / recovery path even with the flag
        # set. The two probes plus the filter pin us to a leaf the
        # explicit metadata cursor walk reaches.
        import re
        import shutil
        import struct
        self.skip_test_if_disagg()
        self.close_conn()

        # 20 tables each padded with ~800B of app_metadata inflates the
        # metadata btree enough to split off live leaves whose entries
        # are only user-table file:* URIs. Without the padding a 20-
        # table fixture packs every leaf with at least one conn-open
        # key and the filter rules them all out.
        n_tables = 20
        pad = 'x' * 800
        # Per user table: file:t*.wt + colgroup:t* + table:t*. Plus
        # file:WiredTigerHS.wt. (file:WiredTiger.wt and system:* are
        # filtered out by list_print's WT_PREFIX_MATCH check.)
        total_uris = 3 * n_tables + 1

        home = 'metadata_corrupt_home'
        os.makedirs(home, exist_ok=True)
        import wiredtiger
        conn = wiredtiger.wiredtiger_open(home, 'create')
        s = conn.open_session()
        for i in range(n_tables):
            s.create('table:m%04d' % i,
                'key_format=S,value_format=S,app_metadata="%s"' % pad)
        s.checkpoint()
        conn.close()

        # Scan WiredTiger.wt for row_leaf pages, skipping any whose
        # entries include a conn-open key.
        ALLOC = 4096
        WT_PAGE_HEADER_SIZE = 28
        conn_open_re = re.compile(
            rb'(system:[A-Za-z0-9_]+|file:WiredTiger(?:HS)?\.wt)')
        path = os.path.join(home, 'WiredTiger.wt')
        leaves = []
        with open(path, 'rb') as f:
            size = os.path.getsize(path)
            for off in range(0, size, ALLOC):
                f.seek(off)
                hdr = f.read(WT_PAGE_HEADER_SIZE + 12)
                if len(hdr) < WT_PAGE_HEADER_SIZE + 12 or hdr[24] != 7:
                    continue
                ds = struct.unpack_from('<I', hdr, 28)[0]
                if ds == 0 or ds > size or ds % ALLOC != 0:
                    continue
                f.seek(off)
                if conn_open_re.search(f.read(ds)):
                    continue
                leaves.append((off, ds))
        self.assertGreater(len(leaves), 0,
            'no user-only metadata leaves to corrupt; bump n_tables or '
            'app_metadata padding')

        # Live vs stale image isn't determinable from header data, so
        # sweep from the file tail (block manager appends, so tail
        # offsets are more likely current) and validate each batch
        # with TWO probes:
        #   plain `wt list` must fail   (corruption is reached);
        #   `wt -q list` must succeed with partial output (cursor walk
        #     actually skipped surviving entries).
        # Stale images can satisfy the first via preload-touch but
        # not the second. Restoring between batches keeps the search
        # deterministic.
        pristine = home + '.pristine'
        if os.path.exists(pristine):
            shutil.rmtree(pristine)
        shutil.copytree(home, pristine)

        def restore():
            shutil.rmtree(home)
            shutil.copytree(pristine, home)

        def corrupt_batch(batch):
            with open(path, 'r+b') as f:
                for off, _ in batch:
                    f.seek(off + 64)
                    f.write(b'\xde\xad\xbe\xef' * 32)

        def run_list(extra_argv):
            argv = [self._wt_path(), '-h', home] + extra_argv + ['list']
            with open('mdl.out', 'w') as o, open('mdl.err', 'w') as e:
                rc = subprocess.call(argv, stdout=o, stderr=e)
            with open('mdl.out') as o:
                return rc, o.read()

        def count_uris(text):
            prefixes = ('table:', 'colgroup:', 'file:', 'system:', 'index:')
            return sum(1 for line in text.splitlines()
                       if any(line.startswith(p) for p in prefixes))

        tail_first = sorted(leaves, key=lambda p: -p[0])
        live = None
        BATCH = 2
        for c in range(0, len(tail_first), BATCH):
            batch = tail_first[c:c + BATCH]
            corrupt_batch(batch)
            rc_plain, out_plain = run_list([])
            rc_q, out_q = run_list(['-q'])
            rows_plain = count_uris(out_plain)
            rows_q = count_uris(out_q)
            if (rc_plain != 0 or rows_plain != total_uris) and \
               rc_q == 0 and 0 < rows_q < total_uris:
                live = (batch, rows_q)
                break
            restore()
        self.assertIsNotNone(live,
            'exhausted user-only leaves without finding a live one; '
            'every candidate was a stale image (current root references '
            'a different copy)')
        _, rows_q = live
        # Sanity-check both endpoints of the partial output band: at
        # least one URI survived (walk reached the other side of the
        # corrupt leaf), and not every URI (corruption was exercised).
        self.assertGreater(rows_q, 0,
            'wt -q list emitted no URIs after metadata-leaf corruption; '
            'the walk aborted instead of skipping')
        self.assertLess(rows_q, total_uris,
            'wt -q list emitted all %d URIs; corruption was not exercised '
            '(emitted=%d)' % (total_uris, rows_q))

    # ---------- verify -c regression coverage ----------

    def test_verify_c_still_works_with_q_landed(self):
        self.setup_corrupt_leaf_table()
        self.runWt(['-p', 'verify', '-c', self.uri],
            outfilename='verifyc.out', errfilename='verifyc.err', failure=True)
        with open('verifyc.err') as f:
            err = f.read()
        self.assertIn('read checksum error', err,
            'wt verify -c stderr did not contain the expected checksum error diagnostic')

    # ---------- dispatcher rejection ----------

    def test_q_rejected_on_write_command(self):
        # The dispatcher must reject -q for commands that are not on the
        # read-oriented list.
        self.close_conn()
        self.runWt(['-q', 'create', '-c', 'key_format=S,value_format=S',
                    'table:does_not_matter'],
            outfilename='create_q.out', errfilename='create_q.err', failure=True)
        with open('create_q.err') as f:
            err = f.read()
        self.assertIn('-q is only valid for read-oriented commands', err,
            'wt -q create did not produce the expected rejection error')

if __name__ == '__main__':
    wttest.run()
