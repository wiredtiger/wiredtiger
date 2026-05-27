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
#    Cover the global `wt -q` flag (WT-17348): read-oriented commands must
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
# Splitting the halves keeps CI failure surfaces specific: a regression in
# the panic-suppression path lights up one named test, not a generic
# "behaves wrong on corruption" check.
class test_util_read_corrupt(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_util_read_corrupt.a'
    uri = 'table:' + tablename
    nentries = 2000

    # Corruption is written at 75% of the .wt file. Keys are sequential and
    # the file is large enough (~20MB) that internal pages cluster at the
    # start, so a write at 75% reliably lands on a leaf. Tests that need a
    # key on (or adjacent to) the corrupt leaf compute it from this fraction.
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

    # ---------- dump (full table walk: dump_all_records path) ----------

    def test_dump_without_q_fails(self):
        # Baseline: with corruption present and without -q, the full-table
        # dump must abort. This pins the "panic on first corrupt" contract
        # that the with-q test's "partial output" claim relies on.
        self.setup_corrupt_leaf_table()
        self.runWt(['dump', self.uri],
            outfilename='dump_no_q.out', errfilename='dump_no_q.err', failure=True)
        self.check_non_empty_file('dump_no_q.err')

    def test_dump_with_q_produces_partial_output(self):
        # With -q the dump walks past the corrupt leaf, emits whatever it
        # could read, and exits non-zero.
        self.setup_corrupt_leaf_table()
        self.runWt(['-q', 'dump', self.uri],
            outfilename='dump_q.out', errfilename='dump_q.err', failure=True)
        self.assertGreater(self.count_lines('dump_q.out'), 0,
            'wt -q dump produced no output on a partially corrupt table')
        self.check_non_empty_file('dump_q.err')

    # ---------- dump -k <key> -w <window> (dump_record path) ----------

    # Intentionally omitted from this commit. The dump_record code path
    # (search_near + windowed traversal) is interesting because this
    # change also touches its return-code handling, but a corruption-
    # driven without-q baseline is hard to make deterministic: the
    # corrupt leaf lands on an unpredictable key index due to
    # test-framework checkpoint cadence, and a narrow window can sit on
    # either side of it. The dump_record fixes in this change (missing
    # key exits 0; window past end of table exits 0) are non-error paths
    # and can be pinned without corruption; follow-up commit.

    # ---------- read ----------

    def test_read_without_q_fails(self):
        # Looking up every key guarantees one search lands on the corrupt
        # leaf, regardless of which key range it ended up holding under
        # this test run's btree layout.
        self.setup_corrupt_leaf_table()
        self.runWt(['read', self.uri] + self._all_keys(),
            outfilename='read_no_q.out', errfilename='read_no_q.err', failure=True)

    def test_read_with_q_continues_on_corrupt(self):
        # With -q, the per-key error path keeps walking the remaining
        # keys. Walking all 2000 keys reliably crosses the corrupt leaf
        # and exits non-zero with output for the readable keys; the
        # readable-key count is the count of stdout lines.
        self.setup_corrupt_leaf_table()
        self.runWt(['-q', 'read', self.uri] + self._all_keys(),
            outfilename='read_q.out', errfilename='read_q.err', failure=True)
        self.assertGreater(self.count_lines('read_q.out'), 0,
            'wt -q read produced no values for any of the requested keys')
        self.check_non_empty_file('read_q.err')

    # ---------- stat ----------

    def test_stat_without_q_fails(self):
        # Baseline: `wt stat` invokes statistics=(all), which walks every
        # page during cursor open. Without -q the walk must panic on the
        # corrupt leaf rather than reporting a graceful error; otherwise
        # the block-layer panic suppression has leaked into the default
        # path.
        self.setup_corrupt_leaf_table()
        self.runWt(['stat', self.uri],
            outfilename='stat_no_q.out', errfilename='stat_no_q.err', failure=True)

    def test_stat_with_q_does_not_crash_on_corrupt(self):
        # The stats walk runs to completion (or error) before any output is
        # produced, so "partial output" isn't a meaningful contract here.
        # The realistic contract for -q on stat is "do not crash"; rc == 1
        # is the graceful WT_ERROR path, rc == 0 means the walk happened
        # not to touch the corrupted offset on this build. Both are
        # acceptable; anything else indicates a crash.
        self.setup_corrupt_leaf_table()
        argv = [self._wt_path(), '-q', 'stat', self.uri]
        with open('stat_q.out', 'w') as out, open('stat_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertIn(rc, (0, 1),
            'wt -q stat exited with unexpected status %d (expected 0 or 1, '
            'a higher code indicates the process crashed instead of returning '
            'a quiet-corrupt error)' % rc)
        # When the walk did hit corruption, stderr must carry the WT_ERROR
        # diagnostic so a future silent regression of the quiet-corrupt
        # error path is caught here rather than slipping through as
        # "rc == 1, no message".
        if rc == 1:
            self.check_non_empty_file('stat_q.err')

    # ---------- list (metadata happy path; metadata-corruption pair TODO) ----------

    def test_list_with_q_still_lists_uris(self):
        # `wt list` reads the metadata cursor (WiredTiger.wt), not the
        # user table, so this test exercises the happy path: a corrupted
        # user-table leaf page must not block the metadata listing. The -q
        # softening of list_print's post-loop error path requires metadata
        # corruption to exercise directly; that pair lands in a follow-up.
        self.setup_corrupt_leaf_table()
        self.runWt(['-q', 'list'],
            outfilename='list_q.out', errfilename='list_q.err')
        with open('list_q.out') as f:
            text = f.read()
        self.assertIn(self.uri, text,
            'wt -q list omitted the user table from the metadata listing')

    # ---------- verify -c regression coverage ----------

    def test_verify_c_still_works_with_q_landed(self):
        # Regression coverage for "confirm `wt verify -c` behavior is
        # unchanged": run it on the same corrupted table and assert the
        # expected checksum-error diagnostic. `wt verify` is intentionally
        # excluded from the -q set; this test exercises the original -c
        # path that this PR must not regress.
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
        # read-oriented list. `create` is a good representative of the
        # rejected set.
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
