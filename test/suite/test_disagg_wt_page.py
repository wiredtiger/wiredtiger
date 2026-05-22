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

import glob, os, re, sqlite3, subprocess
import wttest
from helper_disagg import DisaggConfigMixin, get_shard_id

# Drive the `wt page` command end-to-end through a disagg cell built with
# the in-tree palite page-log extension.
class test_disagg_wt_page(wttest.WiredTigerTestCase, DisaggConfigMixin):
    uri_base = "wt_page_test"
    uri = "layered:" + uri_base
    stable_uri = "file:" + uri_base + ".wt_stable"
    nrows = 1000

    # palite flag bit indicating a tombstoned page chain entry.
    PAGE_LOG_DISCARDED = 0x10000

    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _wt_bin(self):
        build = self.buildDirectory()
        libs_wt = os.path.join(build, '.libs', 'wt')
        return libs_wt if os.path.isfile(libs_wt) else os.path.join(build, 'wt')

    def _palite_ext(self):
        pattern = os.path.join(self.buildDirectory(),
            'ext', 'page_log', 'palite', '**', 'libwiredtiger_palite.so')
        matches = glob.glob(pattern, recursive=True)
        if not matches:
            self.fail("Could not locate libwiredtiger_palite.so in build directory")
        return matches[0]

    def _wt_page_extra_config(self):
        return (f'extensions=["{self._palite_ext()}"],'
                f'disaggregated=(page_log=palite,role="follower")')

    def _run_wt(self, *args, extra_config=False):
        cmd = [self._wt_bin(), '-h', self.home]
        if extra_config:
            cmd += ['-C', self._wt_page_extra_config()]
        cmd += list(args)
        self.close_conn()
        try:
            return subprocess.run(cmd, capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()

    def _run_wt_page(self, *args):
        return self._run_wt('page', *args, extra_config=True)

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _dirty_and_checkpoint(self):
        # Overwrite a few existing keys to force a second checkpoint to emit
        # delta page-log entries for the affected btree pages.
        c = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows, max(1, self.nrows // 8)):
            c[f"k{i:08}"] = f"V{i:08}"
        c.close()
        self.session.checkpoint()

    def _table_id(self, uri):
        # Read id=N out of the metadata config string for the file URI.
        c = self.session.open_cursor('metadata:')
        c.set_key(uri)
        self.assertEqual(c.search(), 0, f"URI not found in metadata: {uri}")
        val = c.get_value()
        c.close()
        m = re.search(r',id=(\d+),', ',' + val + ',')
        self.assertIsNotNone(m, f"id= not found for {uri}: {val}")
        return int(m.group(1))

    def _palite_pages(self, table_id):
        # Every page chain palite recorded for table_id, newest first, as
        # 5-tuples (page_id, lsn, base_lsn, backlink_lsn, flags). See
        # ext/page_log/palite/palite.cpp for the schema. Read-only so a
        # concurrently-running palite process is not disturbed.
        db = os.path.join(self.home, 'kv_home',
                          f'pages_{get_shard_id(table_id):02d}.db')
        with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as conn:
            return conn.execute(
                "SELECT page_id, lsn, base_lsn, backlink_lsn, flags "
                "FROM pages WHERE table_id=? ORDER BY lsn DESC",
                (table_id,)).fetchall()

    def _find_page(self, predicate, description):
        table_id = self._table_id(self.stable_uri)
        row = next((r for r in self._palite_pages(table_id) if predicate(r)), None)
        self.assertIsNotNone(row,
            f"no {description} rows for table_id={table_id} in palite")
        return row

    def _assert_chain_header(self, stdout, page_id, lsn, base_lsn, backlink_lsn,
                             delta_count_re, results_re):
        self.assertRegex(stdout, re.compile(
            rf"^chain: page_id={page_id} lsn={lsn} "
            rf"base_lsn={base_lsn} backlink_lsn={backlink_lsn} "
            rf".* delta_count={delta_count_re} results={results_re}$",
            re.MULTILINE))

    def test_help(self):
        out = self._run_wt('page', '-?')
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertIn('page -p page_id', out.stderr)

    def test_unknown_page_id(self):
        self._populate()
        out = self._run_wt_page("-p", "99999999", self.stable_uri)
        self.assertNotEqual(out.returncode, 0)

    def test_full_image_chain(self):
        self._populate()
        page_id, lsn, _, _, _ = self._find_page(
            lambda r: r[2] == 0 and r[3] == 0, "base-image")
        out = self._run_wt_page("-p", str(page_id), "-l", str(lsn), self.stable_uri)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self._assert_chain_header(out.stdout, page_id, lsn, 0, 0,
                                  delta_count_re="0", results_re="1")
        self.assertRegex(out.stdout, re.compile(r"^- row-store ", re.MULTILINE))

    def test_delta_chain(self):
        self._populate()
        self._dirty_and_checkpoint()
        page_id, lsn, base_lsn, backlink_lsn, _ = self._find_page(
            lambda r: r[3] != 0 and not (r[4] & self.PAGE_LOG_DISCARDED), "delta")
        out = self._run_wt_page("-p", str(page_id), "-l", str(lsn), self.stable_uri)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self._assert_chain_header(out.stdout, page_id, lsn, base_lsn, backlink_lsn,
                                  delta_count_re=r"\d+",
                                  results_re=r"(?:[2-9]|[1-9]\d+)")

    def test_missing_required_p(self):
        self._populate()
        out = self._run_wt_page(self.stable_uri)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("page", out.stderr.lower())

if __name__ == '__main__':
    wttest.run()
