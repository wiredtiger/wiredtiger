#!/usr/bin/env python3
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

import json, os, subprocess, wttest
from run import wt_builddir
from helper_disagg import (
    DisaggConfigMixin, DisaggCorruptionMixin, disagg_test_class,
    gen_disagg_storages, get_shard_id, NUM_SHARDS,
)
from wtscenario import make_scenarios

# test_disagg_corruption_mixin.py
#    Exercise the DisaggCorruptionMixin helpers against a palite-backed
#    disaggregated database.
@disagg_test_class
class test_disagg_corruption_mixin(wttest.WiredTigerTestCase, DisaggCorruptionMixin):
    conn_base_config = ',create,statistics=(all),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_disagg_corruption_mixin', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_corruption_mixin'
    nentries = 10

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Populate the table and force palite to flush pages by checkpointing.
    def _populate(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nentries):
            c[f'k{i:04d}'] = f'v{i:04d}'
        c.close()
        self.session.checkpoint()

    # Read rows directly from the per-shard pages DB. Palite uses shared SQLite
    # locks for readers, so this is safe to call whether the WT connection is
    # open or closed.
    def _sqlite_select_json(self, table_id, sql_query):
        shard = get_shard_id(table_id)
        db_path = os.path.join(self.home, 'kv_home', f'pages_{shard:02d}.db')
        sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
        result = subprocess.run(
            [sqlite_exe, '-json', db_path, sql_query],
            capture_output=True, text=True, check=True)
        return json.loads(result.stdout) if result.stdout.strip() else []

    # Find any populated (table_id, page_id) pair from the pages DB by scanning
    # all shards. Returns (table_id, page_id, lsn). Requires the WT connection
    # to be closed. Used by individual test_* methods to pick a victim row.
    def _pick_any_row(self):
        self.close_conn()
        try:
            for shard in range(NUM_SHARDS):
                db_path = os.path.join(self.home, 'kv_home', f'pages_{shard:02d}.db')
                if not os.path.exists(db_path):
                    continue
                sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
                result = subprocess.run(
                    [sqlite_exe, '-json', db_path,
                     'SELECT table_id, page_id, lsn FROM pages '
                     'ORDER BY table_id DESC, page_id ASC, lsn DESC LIMIT 1;'],
                    capture_output=True, text=True, check=True)
                if result.stdout.strip():
                    row = json.loads(result.stdout)[0]
                    return int(row['table_id']), int(row['page_id']), int(row['lsn'])
        finally:
            self.reopen_conn()
        self.fail("no rows found in any pages_NN.db after populate")

    # Smoke test: populate, find at least one row via sqlite, then confirm we
    # can also read sqlite while the WT connection is open (per-method helpers
    # rely on this, but only do reads *after* their reopen_conn; verify here so
    # a SQLITE_BUSY surface is caught at smoke-test time, not in Task 3).
    def test_populate_produces_palite_rows(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        self._populate()
        table_id, page_id, lsn = self._pick_any_row()
        self.assertGreater(table_id, 0)
        self.assertGreaterEqual(lsn, 0)
        # WT connection is reopen after _pick_any_row. Read again now to prove
        # palite doesn't block readers via its exclusive lock.
        rows = self._sqlite_select_json(table_id,
            f'SELECT COUNT(*) AS n FROM pages WHERE table_id={int(table_id)};')
        self.assertGreater(rows[0]['n'], 0)

    def test_corrupt_page_image(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        self._populate()
        table_id, page_id, lsn = self._pick_any_row()

        # Capture the original first byte of page_data via sqlite hex().
        before = self._sqlite_select_json(table_id,
            f'SELECT hex(substr(page_data, 1, 1)) AS first FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} AND lsn={int(lsn)};')
        self.assertEqual(len(before), 1)

        # Mutate. Helper closes WT and leaves it closed; sqlite reads below
        # work either way.
        returned_lsn = self.corrupt_page_image(table_id, page_id)
        self.assertEqual(returned_lsn, lsn)

        # Confirm the first byte is now 0xff.
        after = self._sqlite_select_json(table_id,
            f'SELECT hex(substr(page_data, 1, 1)) AS first FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} AND lsn={int(lsn)};')
        self.assertEqual(after[0]['first'], 'FF')

    def test_delete_page_image(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        self._populate()
        table_id, page_id, lsn = self._pick_any_row()

        returned_lsn = self.delete_page_image(table_id, page_id)
        self.assertEqual(returned_lsn, lsn)

        # The row should be gone.
        after = self._sqlite_select_json(table_id,
            f'SELECT COUNT(*) AS n FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} AND lsn={int(lsn)};')
        self.assertEqual(after[0]['n'], 0)

    def test_set_page_discarded(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        self._populate()
        table_id, page_id, lsn = self._pick_any_row()

        # The row should not be discarded yet.
        before = self._sqlite_select_json(table_id,
            f'SELECT discarded FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} AND lsn={int(lsn)};')
        self.assertEqual(before[0]['discarded'], 0)

        returned_lsn = self.set_page_discarded(table_id, page_id)
        self.assertEqual(returned_lsn, lsn)

        after = self._sqlite_select_json(table_id,
            f'SELECT discarded, flags FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} AND lsn={int(lsn)};')
        self.assertEqual(after[0]['discarded'], 1)
        self.assertTrue(int(after[0]['flags']) & DisaggCorruptionMixin.WT_PAGE_LOG_DISCARDED)

    # Force more than one LSN per (table_id, page_id) by repeatedly modifying
    # and checkpointing. Returns (table_id, page_id) of a row with at least
    # two LSNs.
    def _populate_with_multi_lsn(self):
        self._populate()
        for round in range(5):
            c = self.session.open_cursor(self.uri, None, None)
            for i in range(self.nentries):
                c[f'k{i:04d}'] = f'v{i:04d}-{round}'
            c.close()
            self.session.checkpoint()

        # Find a (table_id, page_id) that has >= 2 LSN rows.
        self.close_conn()
        try:
            for shard in range(NUM_SHARDS):
                db_path = os.path.join(self.home, 'kv_home', f'pages_{shard:02d}.db')
                if not os.path.exists(db_path):
                    continue
                sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
                result = subprocess.run(
                    [sqlite_exe, '-json', db_path,
                     'SELECT table_id, page_id, COUNT(*) AS n FROM pages '
                     'GROUP BY table_id, page_id HAVING n >= 2 '
                     'ORDER BY table_id DESC LIMIT 1;'],
                    capture_output=True, text=True, check=True)
                if result.stdout.strip():
                    row = json.loads(result.stdout)[0]
                    return int(row['table_id']), int(row['page_id'])
        finally:
            self.reopen_conn()
        self.fail("could not produce a (table_id, page_id) with multiple LSNs")

    def test_truncate_delta_chain(self):
        if self.ds_name != 'palite':
            self.skipTest('palite-only test')
        table_id, page_id = self._populate_with_multi_lsn()

        all_lsns = [int(r['lsn']) for r in self._sqlite_select_json(table_id,
            f'SELECT lsn FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} ORDER BY lsn;')]
        self.assertGreaterEqual(len(all_lsns), 2)

        keep = [all_lsns[0]]  # keep only the base
        deleted = self.truncate_delta_chain(table_id, page_id, keep)
        self.assertEqual(sorted(deleted), sorted(all_lsns[1:]))

        remaining = [int(r['lsn']) for r in self._sqlite_select_json(table_id,
            f'SELECT lsn FROM pages '
            f'WHERE table_id={int(table_id)} AND page_id={int(page_id)} ORDER BY lsn;')]
        self.assertEqual(remaining, keep)

if __name__ == '__main__':
    wttest.run()
