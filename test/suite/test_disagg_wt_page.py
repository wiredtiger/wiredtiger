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

# test_disagg_wt_page.py
#    Drive the `wt page` command end-to-end through a disagg cell built
#    with the in-tree palite page-log extension.
class test_disagg_wt_page(wttest.WiredTigerTestCase, DisaggConfigMixin):
    uri_base = "wt_page_test"
    uri = "layered:" + uri_base
    stable_uri = "file:" + uri_base + ".wt_stable"
    nrows = 1000

    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _wt_bin(self):
        build = self.buildDirectory()
        libs_wt = os.path.join(build, '.libs', 'wt')
        if os.path.isfile(libs_wt):
            return libs_wt
        return os.path.join(build, 'wt')

    def _palite_ext(self):
        build = self.buildDirectory()
        matches = glob.glob(
            os.path.join(build, 'ext', 'page_log', 'palite', '**', 'libwiredtiger_palite.so'),
            recursive=True)
        if matches:
            return matches[0]
        self.fail("Could not locate libwiredtiger_palite.so in build directory")

    def _wt_page_extra_config(self):
        ext = self._palite_ext()
        return f'extensions=["{ext}"],disaggregated=(page_log=palite,role="follower")'

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _dirty_and_checkpoint(self):
        """Overwrite a few existing keys to force a second checkpoint to
        emit delta page-log entries for the affected btree pages."""
        c = self.session.open_cursor(self.uri)
        for i in range(0, self.nrows, max(1, self.nrows // 8)):
            c[f"k{i:08}"] = f"V{i:08}"
        c.close()
        self.session.checkpoint()

    def _table_id(self, uri):
        """Return the integer btree id WiredTiger assigned to ``uri``.

        Reads ``id=N`` out of the metadata config string for the file URI;
        ``WiredTiger.h`` describes the encoding (plain ``key=value`` pairs)
        and avoids any address-cookie parsing.
        """
        c = self.session.open_cursor('metadata:')
        c.set_key(uri)
        self.assertEqual(c.search(), 0, f"URI not found in metadata: {uri}")
        val = c.get_value()
        c.close()
        m = re.search(r',id=(\d+),', ',' + val + ',')
        self.assertIsNotNone(m, f"id= not found for {uri}: {val}")
        return int(m.group(1))

    def _palite_pages(self, table_id):
        """Every page chain palite recorded for ``table_id``, newest first.

        Returns a list of 5-tuples ``(page_id, lsn, base_lsn,
        backlink_lsn, flags)``. See ``ext/page_log/palite/palite.cpp``
        (``CREATE TABLE ... pages``) for the schema. Read-only SQLite
        connection so a concurrently-running palite process is not
        disturbed.
        """
        db = os.path.join(self.home, 'kv_home',
                          f'pages_{get_shard_id(table_id):02d}.db')
        with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as conn:
            return conn.execute(
                "SELECT page_id, lsn, base_lsn, backlink_lsn, flags "
                "FROM pages WHERE table_id=? ORDER BY lsn DESC",
                (table_id,)).fetchall()

    def _run_wt_page(self, *args):
        self.close_conn()
        try:
            return subprocess.run(
                [self._wt_bin(), "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", *args],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()

    def test_help(self):
        """`wt page -?` is plumbed in."""
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), '-h', self.home, 'page', '-?'],
                capture_output=True, text=True, check=False)
            self.assertEqual(out.returncode, 0, msg=out.stderr)
            self.assertIn('page -p page_id', out.stderr)
        finally:
            self.reopen_conn()

    def test_unknown_page_id(self):
        self._populate()
        out = self._run_wt_page("-p", "99999999", self.stable_uri)
        self.assertNotEqual(out.returncode, 0)

    def test_full_image_chain(self):
        """wt page against a base-image chain prints a chain header whose
        fields match palite's record of that page, and renders the leaf
        page contents below it."""
        self._populate()
        table_id = self._table_id(self.stable_uri)
        pages = self._palite_pages(table_id)
        base = next((r for r in pages if r[2] == 0 and r[3] == 0), None)
        self.assertIsNotNone(base,
            f"no base-image rows for table_id={table_id} in palite")
        page_id, lsn, base_lsn, backlink_lsn, _ = base
        out = self._run_wt_page("-p", str(page_id), "-l", str(lsn),
                                self.stable_uri)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertRegex(out.stdout, re.compile(
            rf"^chain: page_id={page_id} lsn={lsn} "
            rf"base_lsn=0 backlink_lsn=0 .* delta_count=0 results=1$",
            re.MULTILINE))
        self.assertRegex(out.stdout, re.compile(r"^- row-store ",
                                                re.MULTILINE))

    def test_delta_chain(self):
        """wt page against a delta chain reports base_lsn/backlink_lsn
        equal to palite's record and delta_count >= 1 / results >= 2."""
        self._populate()
        self._dirty_and_checkpoint()
        table_id = self._table_id(self.stable_uri)
        pages = self._palite_pages(table_id)
        delta = next((r for r in pages if r[3] != 0 and not (r[4] & 0x10000)), None)
        self.assertIsNotNone(delta,
            f"no delta rows for table_id={table_id} in palite "
            f"(dirty_and_checkpoint did not produce a delta)")
        page_id, lsn, base_lsn, backlink_lsn, _ = delta
        out = self._run_wt_page("-p", str(page_id), "-l", str(lsn),
                                self.stable_uri)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertRegex(out.stdout, re.compile(
            rf"^chain: page_id={page_id} lsn={lsn} "
            rf"base_lsn={base_lsn} backlink_lsn={backlink_lsn} "
            rf".* delta_count=\d+ results=(?:[2-9]|[1-9]\d+)$",
            re.MULTILINE))

    def test_missing_required_p(self):
        self._populate()
        out = self._run_wt_page(self.stable_uri)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("page", out.stderr.lower())

if __name__ == '__main__':
    wttest.run()
