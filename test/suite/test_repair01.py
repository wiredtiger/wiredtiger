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

import re, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_repair01.py
#    Exercise the wiredtiger_repair() API (config errors, fetch_database_size, fetch_metadata) and
#    the related operations, in both non-disaggregated and disaggregated scenarios.
class test_repair01(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'statistics=(all),'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=False))

    def conn_config(self):
        if not self.is_disagg_scenario():
            return self.conn_base_config
        return self.conn_base_config + \
            'disaggregated=(page_log=%s,role="leader",lose_all_my_data=true),' % self.ds_name

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def repair(self, config):
        return wiredtiger.wiredtiger_repair(self.conn, config)

    @property
    def uri(self):
        return 'layered:tbl' if self.is_disagg_scenario() else 'table:tbl'

    def populate(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(1000):
            cursor['key%06d' % i] = 'v' * 100
        cursor.close()
        self.session.checkpoint()

    def metadata_value(self, uri):
        cursor = self.session.open_cursor('metadata:')
        cursor.set_key(uri)
        self.assertEqual(cursor.search(), 0)
        value = cursor.get_value()
        cursor.close()
        return value

    # The btree id lives on the file entry, which is the stable constituent of a layered table.
    def file_uri(self, name):
        return f'file:{name}.wt_stable' if self.is_disagg_scenario() else f'file:{name}.wt'

    def btree_id_of(self, uri):
        return int(re.search(r'(?:^|,)id=(\d+)', self.metadata_value(uri)).group(1))

    def btree_id(self, name):
        return self.btree_id_of(self.file_uri(name))

    # An id in the same namespace as the given one (the low three bits), well clear of the ids
    # handed out so far.
    def spare_id(self, existing, distance=1000):
        return (((existing >> 3) + distance) << 3) | (existing & 7)

    def reported_size(self):
        result = self.repair('fetch_database_size=(local=true)')
        return int(re.search(r': (\d+)$', result).group(1))

    def checkpoint_size_fix(self, expect_triggered=False):
        pattern = r'disagg database size fix: recomputed database size -> \d+'
        assertion = self.assertRegex if expect_triggered else self.assertNotRegex

        self.conn.reconfigure('verbose=[disaggregated_storage:1]')
        try:
            with self.customStdoutPattern(lambda output: assertion(output, pattern)):
                self.session.checkpoint('debug=(database_size_fix=true)')
        finally:
            self.conn.reconfigure('verbose=[disaggregated_storage:0]')

    def test_config_errors(self):
        self.assertIn('wiredtiger_repair: empty config', self.repair(''))
        self.assertIn('No command found', self.repair('uri="table:tbl"'))

        if not self.is_disagg_scenario():
            return

        # fetch_database_size is checked first regardless of scenario, and always requires a
        # disagg connection with a picked-up checkpoint, so populate() first to get past that
        # guard and reach the collision check.
        self.populate()
        self.assertIn('Only one command is allowed', self.repair(
            'fetch_database_size=(local=true),fetch_metadata=(local=true)'))

    def test_fetch_metadata(self):
        self.populate()

        cursor = self.session.open_cursor('metadata:')
        cursor.set_key(self.uri)
        self.assertEqual(cursor.search(), 0)
        self.assertIn(f'{self.uri}: {cursor.get_value()}',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}")'))
        cursor.close()

        self.assertIn(f'{self.uri}: key_format=S',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="key_format")'))
        self.assertIn(f'{self.uri}: <no "nope">',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="nope")'))
        self.assertIn('<no matching metadata entry for uri:"table:missing">',
            self.repair('fetch_metadata=(local=true,uri="table:missing")'))

        # Absent and empty uri/key must be equivalent, not "matches nothing".
        all_uris = self.repair('fetch_metadata=(local=true)')
        self.assertIn(f'{self.uri}: ', all_uris)
        self.assertNotIn('<no matching metadata entry', all_uris)
        self.assertEqual(all_uris, self.repair('fetch_metadata=(local=true,uri="")'))

        whole_value = self.repair(f'fetch_metadata=(local=true,uri="{self.uri}")')
        self.assertEqual(whole_value,
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="")'))

        if self.is_disagg_scenario():
            self.assertIn(self.uri,
                self.repair(f'fetch_metadata=(local=false,uri="{self.uri}")'))
        else:
            self.assertIn('requires a disaggregated connection',
                self.repair('fetch_metadata=(local=false)'))

    def test_fetch_database_size(self):
        self.populate()

        if not self.is_disagg_scenario():
            self.assertIn('requires a disaggregated connection',
                self.repair('fetch_database_size=(local=true)'))
            self.assertIn('requires a disaggregated connection',
                self.repair('fetch_database_size=(local=false)'))
            return

        reported = self.reported_size()
        stat_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)
        self.assertEqual(reported, stat_size)
        self.assertGreater(reported, 0)

        # local=false recomputes from the metadata; absent drift it matches local=true exactly.
        self.assertIn(f'fetch_database_size(recompute): {stat_size}',
            self.repair('fetch_database_size=(local=false)'))

    def test_id_fix_rejects_bad_input(self):
        self.populate()
        uri = self.file_uri('tbl')
        old_id = self.btree_id('tbl')
        new_id = self.spare_id(old_id)

        def fix_id(uri=uri, old_id=old_id, new_id=new_id):
            return self.repair(f'fix_id=(uri="{uri}",old_id={old_id},new_id={new_id})')

        self.assertIn('fix_id requires uri, old_id and new_id',
            self.repair(f'fix_id=(uri="{uri}",old_id={old_id})'))
        self.assertIn('fix_id old_id and new_id are the same', fix_id(new_id=old_id))
        self.assertIn('fix_id ids must be between', fix_id(new_id=0))
        self.assertIn('no metadata entry', fix_id(uri=self.file_uri('missing')))
        self.assertIn('not the expected old_id', fix_id(old_id=old_id + 8))
        self.assertIn('not in the same namespace', fix_id(new_id=new_id + 1))

        # A second table gives us an id that is in use, and once dropped, an unused id that is
        # still below the allocator.
        self.session.create('layered:other' if self.is_disagg_scenario() else 'table:other',
            'key_format=S,value_format=S')
        self.session.checkpoint()
        other_id = self.btree_id('other')
        self.assertIn('is already used by', fix_id(new_id=other_id))
        self.session.drop('layered:other' if self.is_disagg_scenario() else 'table:other')
        self.assertIn('not above the largest id allocated so far', fix_id(new_id=other_id))

        # The table is still open here, so its cached id would not pick up the change.
        self.assertIn('is open', fix_id())

    def test_id_fix(self):
        self.populate()
        uri = self.file_uri('tbl')
        old_id = self.btree_id('tbl')
        new_id = self.spare_id(old_id)
        before = self.metadata_value(uri)

        # Reopen so nothing holds the handle, and do not touch the table afterwards: in
        # disaggregated storage its pages are no longer reachable under the new id.
        self.reopen_conn()
        if self.is_disagg_scenario():
            self.ignoreStdoutPatternIfExists('Removing local file due to disagg mode')

            # Only the stable file of a layered table can be fixed, and a rejected fix writes
            # nothing, not even to the local metadata.
            ingest_uri = 'file:tbl.wt_ingest'
            ingest_before = self.metadata_value(ingest_uri)
            self.assertIn('needs a "file:<name>.wt_stable" uri',
                self.repair(f'fix_id=(uri="{ingest_uri}",old_id={self.btree_id_of(ingest_uri)},'
                            f'new_id={self.spare_id(self.btree_id_of(ingest_uri), 2000)})'))
            self.assertEqual(self.metadata_value(ingest_uri), ingest_before)

        result = self.repair(f'fix_id=(uri="{uri}",old_id={old_id},new_id={new_id})')
        self.assertIn('fixed, now rebuild or drop this table', result)
        self.assertIn(f'{uri}: id={new_id}', result)
        self.assertEqual(self.btree_id('tbl'), new_id)

        # Nothing but the id changed.
        self.assertEqual(self.metadata_value(uri).replace(f'id={new_id}', f'id={old_id}'), before)

        # Reopening discards the local files and rebuilds from the page service, so the new id
        # survives only if the fix reached the shared metadata table.
        if self.is_disagg_scenario():
            self.reopen_conn()
            self.ignoreStdoutPatternIfExists('Removing local file due to disagg mode')
            self.assertEqual(self.btree_id('tbl'), new_id)

        # A table created afterwards cannot be given the id we just used.
        self.session.create('layered:after' if self.is_disagg_scenario() else 'table:after',
            'key_format=S,value_format=S')
        self.assertGreater(self.btree_id('after'), new_id)

    def test_id_fix_follower(self):
        if not self.is_disagg_scenario():
            return

        self.populate()
        uri = self.file_uri('tbl')
        old_id = self.btree_id('tbl')

        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertIn('fix_id requires a disaggregated leader connection',
            self.repair(f'fix_id=(uri="{uri}",old_id={old_id},new_id={old_id + 8000})'))
        self.assertEqual(self.btree_id('tbl'), old_id)

    def test_fix_size(self):
        self.populate()

        if not self.is_disagg_scenario():
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.checkpoint_size_fix(),
                '/requires a disaggregated leader connection/')
            return

        stat_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)

        # Absent any drift, the recompute matches the incrementally-tracked total.
        self.checkpoint_size_fix(expect_triggered=True)
        self.assertEqual(self.get_stat(wiredtiger.stat.conn.disagg_database_size), stat_size)

        # Drop a second, already-checkpointed table and grow the main one before fixing, so the
        # recompute has to reflect real change, not just replay the old total.
        extra_uri = 'layered:tbl_fix_size_extra'
        self.session.create(extra_uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(extra_uri)
        for i in range(50):
            cursor['key%06d' % i] = 'v' * 500
        cursor.close()
        self.session.checkpoint()  # settle first, or dropping it can hit its own dirty data

        pre_change_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)
        cursor = self.session.open_cursor(self.uri)
        for i in range(1000, 4000):
            cursor['key%06d' % i] = 'v' * 200
        cursor.close()
        self.session.drop(extra_uri)

        self.checkpoint_size_fix(expect_triggered=True)

        changed = self.reported_size()
        self.assertGreater(changed, pre_change_size)
        self.assertEqual(changed, self.get_stat(wiredtiger.stat.conn.disagg_database_size))

        # Cross-check against the independent __wt_verify_disagg_database_size path, only
        # reachable via verify_metadata=true at open.
        self.reopen_conn(config=self.conn_config() + 'verify_metadata=true,')
        self.ignoreStdoutPatternIfExists('Removing local file due to disagg mode')

        # A follower's session.checkpoint() is already a no-op skip at the session API layer
        # (standby has nothing to checkpoint), so it never reaches the leader-only guard in
        # __checkpoint_parse_config; it just needs to not raise or change the size.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.checkpoint_size_fix()
        self.assertEqual(self.get_stat(wiredtiger.stat.conn.disagg_database_size), changed)
