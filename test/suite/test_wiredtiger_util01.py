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

# test_wiredtiger_util01.py
#    Exercise the wiredtiger_util() API and cross-validate every reported value against an
#    independent Python cursor read. The local-metadata read, the local database-size read, and the
#    config-error paths run in both the non-disaggregated and the disaggregated scenario; the
#    shared-metadata read, the recomputed database size, and the leader-only size fix run on
#    disaggregated storage, with their guards checked on the plain connection.
class test_wiredtiger_util01(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'statistics=(all),'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=False))

    # The in-memory disaggregated database size always carries this fixed buffer
    # (WT_DISAGG_CHECKPOINT_SIZE_BUFFER in connection.h); the recomputed sum does not.
    size_buffer = 1024 * 1024

    def conn_config(self):
        if not self.is_disagg_scenario():
            return self.conn_base_config
        return self.conn_base_config + \
            'disaggregated=(page_log=%s,role="leader",lose_all_my_data=true),' % self.ds_name

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def util(self, config):
        return wiredtiger.wiredtiger_util(self.conn, config)

    # Create a populated, checkpointed table so it has metadata and a checkpoint size.
    def populate(self):
        uri = 'layered:tbl' if self.is_disagg_scenario() else 'table:tbl'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(1000):
            cursor['key%06d' % i] = 'v' * 100
        cursor.close()
        self.session.checkpoint()
        return uri

    # Independently sum the most recent checkpoint size of every stable file, mirroring what
    # __wt_disagg_get_database_size accumulates in the engine.
    def metadata_size_sum(self):
        total = 0
        cursor = self.session.open_cursor('metadata:')
        while cursor.next() == 0:
            key = cursor.get_key()
            if key.startswith('file:') and key.endswith('.wt_stable'):
                sizes = re.findall(r',size=(\d+),', cursor.get_value())
                if sizes:
                    total += int(sizes[-1])
        cursor.close()
        return total

    def reported_size(self, config):
        return int(re.search(r': (\d+)$', self.util(config)).group(1))

    def test_config_errors(self):
        self.assertEqual(self.util(''), 'wiredtiger_util: empty config')
        self.assertIn('No command found', self.util('uri="table:tbl"'))
        self.assertIn('Only one command is allowed',
            self.util('fetch_metadata=(local=true),fetch_database_size=(local=true)'))

    def test_fetch_metadata(self):
        uri = self.populate()

        # A whole-value local fetch equals the metadata cursor's value for the same uri.
        cursor = self.session.open_cursor('metadata:')
        cursor.set_key(uri)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(self.util('uri="%s",fetch_metadata=(local=true)' % uri),
            '\n  %s: %s' % (uri, cursor.get_value()))
        cursor.close()

        # A key-scoped fetch returns just that value; absent keys and uris are reported, not errors.
        self.assertEqual(self.util('uri="%s",fetch_metadata=(local=true,key="key_format")' % uri),
            '\n  %s: key_format=S' % uri)
        self.assertEqual(self.util('uri="%s",fetch_metadata=(local=true,key="nope")' % uri),
            '\n  %s: <no "nope">' % uri)
        self.assertEqual(self.util('uri="table:missing",fetch_metadata=(local=true)'),
            ' <no matching metadata entry for uri:"table:missing">')

        # The shared (page-server-durable) metadata read is disaggregated-only.
        if self.is_disagg_scenario():
            self.assertIn(uri, self.util('uri="%s",fetch_metadata=(local=false)' % uri))
        else:
            self.assertIn('requires a disaggregated connection',
                self.util('fetch_metadata=(local=false)'))

    def test_fetch_database_size(self):
        self.populate()

        if not self.is_disagg_scenario():
            self.assertEqual(self.util('fetch_database_size=(local=true)'),
                'fetch_database_size(local): 0')
            self.assertIn('requires a disaggregated connection',
                self.util('fetch_database_size=(local=false)'))
            return

        # local is the in-memory size; accumulate is the recomputed per-file sum without the fixed
        # buffer. Cross-validate both against the independent metadata walk.
        accumulate = self.reported_size('fetch_database_size=(local=false)')
        self.assertEqual(accumulate, self.metadata_size_sum())
        self.assertEqual(self.reported_size('fetch_database_size=(local=true)'),
            accumulate + self.size_buffer)

    def test_fix_size(self):
        self.populate()

        # The size fix is a disaggregated-leader-only operation.
        if not self.is_disagg_scenario():
            self.assertIn('requires a disaggregated connection',
                self.util('fix=(size=(old_size=0))'))
            return

        local = self.reported_size('fetch_database_size=(local=true)')

        # A mismatched guard is rejected; the matching guard claims the cycle; a second claim before
        # the cycle is consumed is rejected as busy.
        self.assertIn('does not match requested old_size',
            self.util('fix=(size=(old_size=%d))' % (local + 1)))
        self.assertEqual(self.util('fix=(size=(old_size=%d))' % local), 'size_fix triggered')
        self.assertIn('already in progress', self.util('fix=(size=(old_size=0))'))

        # The next checkpoint consumes the fix and recomputes the size as the per-file sum plus the
        # fixed buffer; consuming the fix logs a verbose warning.
        with self.expectedStdoutPattern('disagg database size fix: recomputed database size'):
            self.session.checkpoint()
        self.assertEqual(self.reported_size('fetch_database_size=(local=true)'),
            self.metadata_size_sum() + self.size_buffer)
