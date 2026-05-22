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

import os, sqlite3
from typing import NamedTuple
import wttest
from helper_disagg import DisaggConfigMixin, get_shard_id
from metadata_helper import get_table_id
from suite_subprocess import suite_subprocess

class PalitePage(NamedTuple):
    """One row from palite's pages table. Schema in ext/page_log/palite/palite.cpp."""
    page_id: int
    lsn: int
    base_lsn: int
    backlink_lsn: int
    flags: int

# Drive the `wt page` command end-to-end through a disagg cell built with
# the in-tree palite page-log extension. The leader connection writes pages
# (full images on first checkpoint, deltas on subsequent ones); we then run
# `wt page` as a subprocess in follower mode against the same cell.
@wttest.skip_for_hook("tiered", "wt page does not run under tiered hook")
class test_disagg_wt_page(wttest.WiredTigerTestCase, suite_subprocess, DisaggConfigMixin):
    uri = "layered:wt_page_test"
    stable_uri = "file:wt_page_test.wt_stable"
    nrows = 1000

    # palite flag bit indicating a tombstoned page chain entry; mirrors
    # WT_PAGE_LOG_DISCARDED in ext/page_log/palite/palite.cpp.
    PAGE_LOG_DISCARDED = 0x10000

    # extensionsConfig() injects disaggregated=(page_log=palite) from the
    # extension list set up in conn_extensions; we only set the role here.
    conn_config = 'disaggregated=(role="leader")'

    def setUp(self):
        # self.vars is set by the base setUp; call the staticmethod here
        # because we want to skip before super().setUp() opens a connection.
        if self.vars().page_log != 'palite':
            self.skipTest("wt page test requires the palite page_log")
        super().setUp()

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _wt_page_extra_config(self):
        return self.extensionsConfig() + ',disaggregated=(role="follower")'

    # Run `wt -C <cfg> page <args>` via suite_subprocess.runWt, which handles
    # close_conn / reopen_conn, gdb/lldb wrapping, and the libtool wt binary
    # lookup. The palite extension and follower role are injected via -C so
    # wt main can open the connection before dispatching to util_page.
    # Returns (stdout, stderr) text. failure=True asserts a non-zero exit;
    # failure=False asserts a zero exit.
    def _run_wt_page(self, *args, failure=False):
        cmd = ['-C', self._wt_page_extra_config(), 'page'] + list(args)
        self.runWt(cmd, outfilename='wt.out', errfilename='wt.err',
                   failure=failure)
        with open('wt.out') as f:
            stdout = f.read()
        with open('wt.err') as f:
            stderr = f.read()
        return stdout, stderr

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _dirty_and_checkpoint(self):
        # Update a scattered subset of keys (about eight, here) to dirty
        # several leaf pages without rewriting the whole tree, then
        # checkpoint so palite emits delta entries for the dirtied pages.
        c = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows, max(1, self.nrows // 8)):
            c[f"k{i:08}"] = f"V{i:08}"
        c.close()
        self.session.checkpoint()

    def _find_page(self, where_clause, params, description):
        # Find the newest page chain entry matching where_clause.
        table_id = get_table_id(self.session, self.stable_uri)
        db = os.path.join(self.home, 'kv_home',
                          f'pages_{get_shard_id(table_id):02d}.db')
        query = (
            "SELECT page_id, lsn, base_lsn, backlink_lsn, flags "
            "FROM pages WHERE table_id=? AND " + where_clause +
            " ORDER BY lsn DESC LIMIT 1"
        )
        # Close the WT connection so palite releases its SQLite locks before
        # we open the database directly in read-only mode.
        self.close_conn()
        try:
            with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as conn:
                row = conn.execute(query, (table_id, *params)).fetchone()
        finally:
            self.reopen_conn()
        self.assertIsNotNone(row,
            f"no {description} rows for table_id={table_id} in palite")
        return PalitePage(*row)

    def _find_base_image_page(self):
        return self._find_page("base_lsn=0 AND backlink_lsn=0", (),
                               "base-image")

    def _find_delta_page(self):
        return self._find_page(
            "backlink_lsn != 0 AND (flags & ?) = 0",
            (self.PAGE_LOG_DISCARDED,), "delta")

    def _assert_chain_header(self, stdout, page_id, lsn, base_lsn, backlink_lsn,
                             delta_count_re, results_re):
        self.assertRegex(stdout,
            rf"(?m)^chain: page_id={page_id} lsn={lsn} "
            rf"base_lsn={base_lsn} backlink_lsn={backlink_lsn} "
            rf"base_ckpt=\d+ backlink_ckpt=\d+ "
            rf"delta_count={delta_count_re} results={results_re}$")

    def test_help(self):
        _, stderr = self._run_wt_page('-?')
        self.assertIn('page -p page_id', stderr)
        self.assertIn('-l lsn', stderr)

    def test_unknown_page_id(self):
        self._populate()
        _, stderr = self._run_wt_page("-p", "99999999", "-l", "1",
                                      self.stable_uri, failure=True)
        # util_err prefixes errors with the subcommand name.
        self.assertIn("page:", stderr)

    def test_missing_required_l(self):
        self._populate()
        _, stderr = self._run_wt_page("-p", "1", self.stable_uri, failure=True)
        self.assertIn("-l lsn is required", stderr)

    def test_full_image_chain(self):
        self._populate()
        page = self._find_base_image_page()
        stdout, _ = self._run_wt_page(
            "-p", str(page.page_id), "-l", str(page.lsn), self.stable_uri)
        self._assert_chain_header(stdout, page.page_id, page.lsn,
                                  page.base_lsn, page.backlink_lsn,
                                  delta_count_re="0", results_re="1")
        self.assertRegex(stdout, r"(?m)^- row-store ")

    def test_delta_chain(self):
        self._populate()
        self._dirty_and_checkpoint()
        page = self._find_delta_page()
        stdout, _ = self._run_wt_page(
            "-p", str(page.page_id), "-l", str(page.lsn), self.stable_uri)
        self._assert_chain_header(stdout, page.page_id, page.lsn,
                                  page.base_lsn, page.backlink_lsn,
                                  delta_count_re=r"\d+",
                                  results_re=r"(?:[2-9]|[1-9]\d+)")

    def test_missing_required_p(self):
        self._populate()
        _, stderr = self._run_wt_page(self.stable_uri, failure=True)
        self.assertIn("-p page_id is required", stderr)

if __name__ == '__main__':
    wttest.run()
