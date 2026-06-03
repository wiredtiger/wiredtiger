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
import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, get_shard_id
from metadata_helper import get_table_id
from run import wt_builddir
from suite_subprocess import suite_subprocess

# test_util_read_corrupt_disagg.py
#    Disaggregated-storage counterpart to test_util_read_corrupt.py. Cover
#    the global `wt -q` flag for read-oriented commands against a
#    layered: URI backed by a palite page log. Without -q the command
#    panics on a corrupt page; with -q the command exits gracefully.
#
# Each command is exercised by a pair of tests with the same shape as
# the non-disagg file:
#   test_<command>_without_q_fails_disagg
#   test_<command>_with_q_<contract>_disagg
#
# The corruption helper follows the pattern proposed in WT-17667 for a
# shared disagg corruption helper: close the connection so the palite
# page log releases its SQLite lock, UPDATE the page_data column to
# all-0xFF bytes, then reopen. When the shared helper lands, this
# inline copy can be replaced with it.
@wttest.skip_for_hook("tiered", "wt does not run under tiered hook")
class test_util_read_corrupt_disagg(wttest.WiredTigerTestCase, suite_subprocess, DisaggConfigMixin):
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
    # subprocess.call pattern used in test_util_read_corrupt.py).
    def _run_wt_follower(self, *args, out='wt.out', err='wt.err'):
        wtexe = os.path.join(wt_builddir, '.libs', 'wt')
        if not os.path.isfile(wtexe):
            wtexe = os.path.join(wt_builddir, 'wt')
        cmd = [wtexe, '-C', self._wt_follower_config()] + list(args)
        with open(out, 'w') as o, open(err, 'w') as e:
            rc = subprocess.call(cmd, stdout=o, stderr=e)
        with open(out) as o:
            stdout = o.read()
        with open(err) as e:
            stderr = e.read()
        return rc, stdout, stderr

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    # Find the newest base-image page (full snapshot, not a delta) for
    # the stable file underlying our layered URI. Uses the sqlite3 binary
    # built alongside palite because the system Python sqlite3 may be too
    # old to parse the palite schema.
    def _find_base_image_page(self):
        table_id = get_table_id(self.session, self.stable_uri)
        db = os.path.join(self.home, 'kv_home',
                          f'pages_{get_shard_id(table_id):02d}.db')
        sql = (f"SELECT page_id, lsn FROM pages "
               f"WHERE table_id={table_id} AND base_lsn=0 AND backlink_lsn=0 "
               f"ORDER BY lsn DESC LIMIT 1;")
        sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
        out = subprocess.run([sqlite_exe, '-json', db, sql],
                             capture_output=True, text=True, check=True).stdout
        rows = json.loads(out) if out.strip() else []
        self.assertTrue(rows,
            f"no base-image rows for table_id={table_id} in palite")
        return table_id, rows[0]['page_id'], rows[0]['lsn']

    # Inline corruption helper. Pattern from the WT-17667 proposal for
    # a shared disagg corruption helper: close the connection to release
    # the palite page log's SQLite lock, overwrite the targeted page's
    # data with garbage, and reopen.
    def _corrupt_one_base_page(self):
        table_id, page_id, lsn = self._find_base_image_page()
        self.close_conn()
        db = os.path.join(self.home, 'kv_home',
                          f'pages_{get_shard_id(table_id):02d}.db')
        # Replace the entire page_data blob with 0xFF bytes (4 KB worth).
        # This breaks the page header's checksum without disturbing the
        # row's primary key, so the page is still discoverable by the
        # block layer (which then fails the checksum check).
        sql = (f"UPDATE pages SET page_data = "
               f"randomblob(length(page_data)) "
               f"WHERE table_id={table_id} AND page_id={page_id} AND lsn={lsn};")
        sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
        subprocess.run([sqlite_exe, db, sql], check=True)
        self.open_conn()

    # ---------- dump ----------

    def test_dump_without_q_fails_disagg(self):
        # Baseline: dump on corrupt disagg data must panic, not exit
        # gracefully. Mirrors test_dump_without_q_fails in the non-disagg
        # file: rc not in (0, 1), stderr carries the panic markers.
        self._populate()
        self._corrupt_one_base_page()
        rc, _, stderr = self._run_wt_follower(
            'dump', self.uri,
            out='dump_no_q.out', err='dump_no_q.err')
        self.assertNotIn(rc, (0, 1),
            f'wt dump on corrupt disagg exited cleanly (rc={rc}); '
            f'-q semantics may have leaked into the default path')
        # The disagg block read path raises the same WT_PANIC marker as
        # the regular block read path when corruption is hit with the
        # quiet flag clear.
        self.assertIn('WT_PANIC', stderr,
            'wt dump on corrupt disagg exited non-zero without panicking')

    def test_dump_with_q_disagg(self):
        # Graceful counterpart. The disagg block read path checks the
        # same WT_SESSION_QUIET_CORRUPT_FILE flag, so -q delivers the
        # same panic-vs-graceful trade as on regular tables.
        self._populate()
        self._corrupt_one_base_page()
        rc, stdout, stderr = self._run_wt_follower(
            '-q', 'dump', self.uri,
            out='dump_q.out', err='dump_q.err')
        self.assertEqual(rc, 1,
            f'wt -q dump on corrupt disagg returned rc={rc}; expected 1')
        self.assertNotIn('WT_PANIC', stderr,
            'wt -q dump panicked despite -q on disagg storage')
        self.assertNotIn('aborting WiredTiger library', stderr,
            'wt -q dump reached the abort marker on disagg storage')

    # ---------- read ----------

    def test_read_without_q_fails_disagg(self):
        self._populate()
        self._corrupt_one_base_page()
        keys = [f"k{i:08}" for i in range(self.nrows)]
        rc, _, stderr = self._run_wt_follower(
            'read', self.uri, *keys,
            out='read_no_q.out', err='read_no_q.err')
        self.assertNotIn(rc, (0, 1),
            f'wt read on corrupt disagg exited cleanly (rc={rc})')
        self.assertIn('WT_PANIC', stderr,
            'wt read on corrupt disagg did not panic')

    def test_read_with_q_continues_on_corrupt_disagg(self):
        # As on non-disagg storage, read's continue-past-bad-key contract
        # rides on each key being an independent search. Walking every
        # key guarantees at least one hits the corrupt base image.
        self._populate()
        self._corrupt_one_base_page()
        keys = [f"k{i:08}" for i in range(self.nrows)]
        rc, stdout, stderr = self._run_wt_follower(
            '-q', 'read', self.uri, *keys,
            out='read_q.out', err='read_q.err')
        self.assertEqual(rc, 1,
            f'wt -q read on corrupt disagg returned rc={rc}; expected 1')
        self.assertNotIn('WT_PANIC', stderr,
            'wt -q read panicked despite -q on disagg storage')
        # Sum check (same as non-disagg pair): every requested key got
        # an outcome (success on stdout or error on stderr). If the loop
        # bailed on first error, the sum would be far less than nrows.
        out_lines = [ln for ln in stdout.splitlines() if ln]
        err_lines = [ln for ln in stderr.splitlines() if ln]
        self.assertEqual(len(out_lines) + len(err_lines), self.nrows,
            f'stdout values ({len(out_lines)}) + stderr errors '
            f'({len(err_lines)}) does not equal nrows ({self.nrows}); '
            f'wt -q read did not visit every requested key')

    # ---------- stat ----------

    def test_stat_without_q_fails_disagg(self):
        self._populate()
        self._corrupt_one_base_page()
        rc, _, stderr = self._run_wt_follower(
            'stat', self.uri,
            out='stat_no_q.out', err='stat_no_q.err')
        self.assertNotIn(rc, (0, 1),
            f'wt stat on corrupt disagg exited cleanly (rc={rc})')
        self.assertIn('WT_PANIC', stderr,
            'wt stat on corrupt disagg did not panic')

    def test_stat_with_q_does_not_crash_on_corrupt_disagg(self):
        # Same caveats as the non-disagg stat test: rc in (0, 1) is the
        # honest contract because the pre-warm hack only covers leaf
        # corruption when root/internal pages are intact. The load-
        # bearing assertion is "no panic markers" - rc=0 means the walk
        # missed the corruption, rc=1 means it hit it and -q caught it.
        self._populate()
        self._corrupt_one_base_page()
        rc, _, stderr = self._run_wt_follower(
            '-q', 'stat', self.uri,
            out='stat_q.out', err='stat_q.err')
        self.assertIn(rc, (0, 1),
            f'wt -q stat on corrupt disagg crashed (rc={rc}); '
            f'pre-warm dhandle caching failed')
        self.assertNotIn('WT_PANIC', stderr,
            'wt -q stat panicked despite -q on disagg storage')

    # ---------- list ----------

    def test_list_with_q_still_lists_uris_disagg(self):
        # list reads metadata, not user-table pages. As on non-disagg
        # storage, corrupting a user-table page does not block listing.
        # This pins "list works under -q on a disagg database even when
        # one of its tables has unreadable pages."
        self._populate()
        self._corrupt_one_base_page()
        rc, stdout, _ = self._run_wt_follower(
            '-q', 'list',
            out='list_q.out', err='list_q.err')
        self.assertEqual(rc, 0,
            f'wt -q list returned rc={rc}; expected 0')
        self.assertIn(self.uri, stdout,
            'wt -q list omitted the user table from the metadata listing')

if __name__ == '__main__':
    wttest.run()
