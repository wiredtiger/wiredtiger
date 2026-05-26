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
class test_util_read_corrupt(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_util_read_corrupt.a'
    uri = 'table:' + tablename
    nentries = 2000

    # The shared-table layout in the disagg hook hides the on-disk file we need
    # to overwrite, so the same skip rule as the open_and_position helper in
    # test_verify applies here.
    def skip_test_if_disagg(self):
        if 'disagg' in self.hook_names:
            self.skipTest('disagg hook: shared tables hide the .wt file we need to corrupt')

    # Fixed-width keys keep `wt read ... <keys>` argv inside the Windows
    # process-creation command-line limit (~32KB). Earlier versions of this
    # test built keys as growing prefixes ("0", "01", "012", ...), which
    # produced multi-kB keys at the tail of the range and overflowed argv on
    # Windows.
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
        """Overwrite a region ~75% into the .wt file with garbage to break a leaf page."""
        self.close_conn()
        filename = self.tablename + '.wt'
        filesize = os.path.getsize(filename)
        position = (filesize * 75) // 100
        with open(filename, 'r+b') as f:
            f.seek(position)
            for _ in range(100):
                f.write(b'\x01\xff\x80')

    def count_lines(self, filename):
        with open(filename) as f:
            return sum(1 for _ in f)

    def test_dump_with_q_produces_partial_output(self):
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

        # Without -q: wt dump aborts.
        self.runWt(['dump', self.uri],
            outfilename='dump_no_q.out', errfilename='dump_no_q.err', failure=True)

        # With -q: wt dump produces some records and exits non-zero.
        self.runWt(['-q', 'dump', self.uri],
            outfilename='dump_q.out', errfilename='dump_q.err', failure=True)
        self.assertGreater(self.count_lines('dump_q.out'), 0,
            'wt -q dump produced no output on a partially corrupt table')
        self.check_non_empty_file('dump_q.err')

    def test_read_with_q_continues_on_corrupt(self):
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

        # Build a spread of keys across the table so some lookups land on the
        # corrupted region (~75% into the file) and others don't. Keys mirror
        # populate()'s "%08d" layout.
        step = max(1, self.nentries // 20)
        keys = ['%08d' % i for i in range(0, self.nentries, step)]

        # We don't know which keys will hit the corrupt page on a given platform
        # / build. The contract is: must not crash, must produce some output for
        # keys that are readable. Bypass the runWt exit-code assertion and check
        # output directly.
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        argv = [wtexe, '-q', 'read', self.uri] + keys
        with open('read_q.out', 'w') as out, open('read_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertIn(rc, (0, 1),
            'wt -q read exited with unexpected status %d (expected 0 or 1)' % rc)
        self.assertGreater(self.count_lines('read_q.out'), 0,
            'wt -q read produced no values for any of the requested keys')

    def test_stat_with_q_does_not_crash_on_corrupt(self):
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

        # `wt stat` runs with `statistics=(all)` (set by util_main.c) which
        # triggers a full btree walk inside the statistics cursor's open path.
        # Unlike dump/read, the walk runs to completion (or hits an error)
        # before any output is produced, so "partial output on corruption" is
        # not a meaningful contract here. The realistic contract for -q on
        # stat is: do not crash. Exit 1 when the walk hits the corrupt leaf
        # (graceful WT_ERROR via the quiet-corrupt path), or exit 0 when the
        # walk happens not to touch the corrupted offset on this build.
        #
        # Bypass the exit-code assertion in runWt to allow either outcome.
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        argv = [wtexe, '-q', 'stat', self.uri]
        with open('stat_q.out', 'w') as out, open('stat_q.err', 'w') as err:
            rc = subprocess.call(argv, stdout=out, stderr=err)
        self.assertIn(rc, (0, 1),
            'wt -q stat exited with unexpected status %d (expected 0 or 1, '
            'a higher code indicates the process crashed instead of returning '
            'a quiet-corrupt error)' % rc)
        # When the walk did hit corruption, stderr must carry the WT_ERROR
        # diagnostic so a future silent regression of the quiet-corrupt error
        # path is caught here rather than slipping through as "rc == 1, no
        # message".
        if rc == 1:
            self.check_non_empty_file('stat_q.err')

    def test_list_with_q_still_lists_uris(self):
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

        # `wt list` reads the metadata cursor (WiredTiger.wt), not the user
        # table, so this test exercises the happy path: the corrupted user-
        # table leaf page must not block the metadata listing. The -q
        # softening of list_print's post-loop error path requires metadata
        # corruption to exercise directly, which is hard to reproduce safely
        # in a suite test; that path is left to future targeted coverage.
        self.runWt(['-q', 'list'],
            outfilename='list_q.out', errfilename='list_q.err')
        with open('list_q.out') as f:
            text = f.read()
        self.assertIn(self.uri, text,
            'wt -q list omitted the user table from the metadata listing')

    def test_verify_c_still_works_with_q_landed(self):
        # Regression coverage for the ticket's "confirm `wt verify -c`
        # behavior is unchanged" criterion: run `wt verify -c` on the same
        # corrupted table and assert it still produces the expected checksum-
        # error diagnostics rather than crashing. `wt verify` is intentionally
        # excluded from the -q set; this test exercises the original -c path
        # that this PR must not regress.
        self.skip_test_if_disagg()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.populate()
        self.session.checkpoint()
        self.corrupt_leaf_page()

        self.runWt(['-p', 'verify', '-c', self.uri],
            outfilename='verifyc.out', errfilename='verifyc.err', failure=True)
        with open('verifyc.err') as f:
            err = f.read()
        self.assertIn('read checksum error', err,
            'wt verify -c stderr did not contain the expected checksum error diagnostic')

    def test_q_rejected_on_write_command(self):
        # The dispatcher must reject -q for commands that are not on the read-
        # oriented list. `create` is a good representative of the rejected set.
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
