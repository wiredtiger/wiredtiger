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
#    Exercise the wiredtiger_repair() API for config-error paths, fetch_database_size,
#    fetch_metadata, and fix_size. All run in non-disaggregated and disaggregated scenarios; the
#    disagg scenario additionally cross-validates the reported size against the
#    disagg_database_size connection statistic and exercises the shared (page-server-durable)
#    metadata read.
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

    def reported_size(self):
        result = self.repair('fetch_database_size=(local=true)')
        return int(re.search(r': (\d+)$', result).group(1))

    def test_config_errors(self):
        self.assertIn('wiredtiger_repair: empty config', self.repair(''))
        self.assertIn('No command found', self.repair('uri="table:tbl"'))
        # fetch_metadata(local=true) doesn't require disagg, so the collision is what fires
        # regardless of checkpoint state.
        self.assertIn('Only one command is allowed', self.repair(
            'fetch_metadata=(local=true),fix_size=(old_size=0)'))

    def test_fetch_metadata(self):
        self.populate()

        # A whole-value local fetch equals the metadata cursor's value for the same uri.
        cursor = self.session.open_cursor('metadata:')
        cursor.set_key(self.uri)
        self.assertEqual(cursor.search(), 0)
        self.assertIn(f'{self.uri}: {cursor.get_value()}',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}")'))
        cursor.close()

        # A key-scoped fetch returns just that value; absent keys and uris are reported, not
        # errors.
        self.assertIn(f'{self.uri}: key_format=S',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="key_format")'))
        self.assertIn(f'{self.uri}: <no "nope">',
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="nope")'))
        self.assertIn('<no matching metadata entry for uri:"table:missing">',
            self.repair('fetch_metadata=(local=true,uri="table:missing")'))

        # An empty uri/key is treated as absent, not as a literal target that matches nothing:
        # empty (or absent) uri means all URIs, empty (or absent) key means the whole value. The
        # empty and absent spellings must produce byte-identical reports.
        all_uris = self.repair('fetch_metadata=(local=true)')
        self.assertIn(f'{self.uri}: ', all_uris)
        self.assertNotIn('<no matching metadata entry', all_uris)
        self.assertEqual(all_uris, self.repair('fetch_metadata=(local=true,uri="")'))

        whole_value = self.repair(f'fetch_metadata=(local=true,uri="{self.uri}")')
        self.assertEqual(whole_value,
            self.repair(f'fetch_metadata=(local=true,uri="{self.uri}",key="")'))

        # The shared (page-server-durable) metadata read is disaggregated-only.
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

        # Cross-validate against the disagg_database_size connection statistic.
        reported = self.reported_size()
        stat_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)
        self.assertEqual(reported, stat_size)
        self.assertGreater(reported, 0)

        # local=false recomputes the same total from the metadata; absent any drift it matches
        # the maintained running total exactly.
        self.assertIn(f'fetch_database_size(recompute): {stat_size}',
            self.repair('fetch_database_size=(local=false)'))

    def test_fix_size(self):
        self.populate()

        if not self.is_disagg_scenario():
            self.assertIn('requires a disaggregated connection',
                self.repair('fix_size=(old_size=0)'))
            return

        stat_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)

        # A stale old_size guard rejects the request instead of claiming a fix.
        self.assertIn('does not match requested old_size',
            self.repair(f'fix_size=(old_size={stat_size + 1})'))

        # The correct (or absent/0) old_size claims the cycle; a second call before the next
        # checkpoint consumes it finds the cycle already claimed.
        self.assertIn('size_fix triggered', self.repair(f'fix_size=(old_size={stat_size})'))
        self.assertIn('already in progress', self.repair('fix_size=(old_size=0)'))

        # The next checkpoint consumes the cycle and recomputes the size from the metadata;
        # absent any drift the result is unchanged and self-consistent with the statistic.
        self.session.checkpoint()
        reported = self.reported_size()
        self.assertEqual(reported, self.get_stat(wiredtiger.stat.conn.disagg_database_size))
        self.assertEqual(reported, stat_size)

        # Exercise a real change, not just a no-op re-derivation: create a second table to drop
        # later, then checkpoint so it's stable (a table dropped right after it's written can
        # spuriously conflict with its own not-yet-settled dirty data). Insert more rows into
        # the main table and drop the second table, both left uncheckpointed, then claim the fix
        # cycle before checkpointing. That single checkpoint must take the full-recompute branch
        # (not the incremental delta branch) for this round, so the result has to reflect the
        # new metadata rather than replay the old total.
        extra_uri = 'layered:tbl_fix_size_extra'
        self.session.create(extra_uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(extra_uri)
        for i in range(50):
            cursor['key%06d' % i] = 'v' * 500
        cursor.close()
        self.session.checkpoint()

        pre_change_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)
        cursor = self.session.open_cursor(self.uri)
        for i in range(1000, 4000):
            cursor['key%06d' % i] = 'v' * 200
        cursor.close()
        self.session.drop(extra_uri)

        self.assertIn('size_fix triggered', self.repair(f'fix_size=(old_size={pre_change_size})'))
        self.session.checkpoint()

        changed = self.reported_size()
        self.assertGreater(changed, pre_change_size)
        self.assertEqual(changed, self.get_stat(wiredtiger.stat.conn.disagg_database_size))

        # A follower cannot claim a fix.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertIn('requires a disaggregated leader connection',
            self.repair('fix_size=(old_size=0)'))
