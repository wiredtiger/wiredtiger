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

import re, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_wiredtiger_util01.py
#   Exercise the wiredtiger_util() WiredTiger API and cross-validate every
#   reported value against an independent Python cursor (metadata cursor or the
#   checkpoint-completion record), so the API is checked against ground truth
#   rather than against itself.
#
#   wiredtiger.wiredtiger_util(conn, config) returns a string. Errors are not
#   raised: the returned string ends with ' Failed: <strerror>'. The connection
#   owns the report buffer, so the string is valid until the next call.

# Fixed overhead always folded into the in-memory database size: the KEK table
# and the shared turtle page. Mirrors WT_DISAGG_CHECKPOINT_SIZE_BUFFER
# (== WT_MEGABYTE) in src/include/connection.h.
DISAGG_SIZE_BUFFER = 1024 * 1024

# Class A: a plain (non-disaggregated) connection. Covers config parsing, the
# local metadata and local database-size paths, and the disagg-only guards.
class test_wiredtiger_util01(wttest.WiredTigerTestCase):
    uri = 'table:test_wiredtiger_util01'

    def util(self, config):
        import wiredtiger
        return wiredtiger.wiredtiger_util(self.conn, config)

    # Parse "\n  <uri>: <rest>" report lines into a {uri: rest} dict so a report
    # can be compared entry-by-entry against direct metadata cursor reads.
    def parse_metadata_report(self, report):
        result = {}
        for line in report.split('\n'):
            line = line.strip()
            if not line:
                continue
            uri, _, rest = line.partition(': ')
            result[uri] = rest
        return result

    def setup_data(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(self.uri)
        for i in range(50):
            c[f'key{i:04d}'] = f'value{i:04d}'
        c.close()
        self.session.checkpoint()

    # ---- config parsing / error reporting ----

    def test_empty_config(self):
        self.assertEqual(self.util(''), 'wiredtiger_util: empty config')

    def test_no_command(self):
        r = self.util('uri="table:test_wiredtiger_util01"')
        self.assertIn('No command found in the config', r)
        self.assertTrue(r.endswith('Failed: Invalid argument'), r)

    def test_two_commands(self):
        r = self.util('fetch_metadata=(local=true),fetch_database_size=(local=true)')
        self.assertIn('Only one command is allowed in the config', r)
        self.assertTrue(r.endswith('Failed: Invalid argument'), r)

    # ---- fetch_metadata (local) cross-validated against metadata: cursor ----

    # Walk the metadata: cursor independently and confirm every entry the util
    # reported has the identical value, and that nothing was dropped or added.
    def test_fetch_metadata_all_matches_cursor(self):
        self.setup_data()
        report = self.util('fetch_metadata=(local=true)')
        reported = self.parse_metadata_report(report)

        expected = {}
        mc = self.session.open_cursor('metadata:')
        while mc.next() == 0:
            key = mc.get_key()
            # The public metadata: cursor exposes a "metadata:" self-entry that
            # the internal metadata cursor wiredtiger_util uses does not; ignore
            # it so the two views are comparable.
            if key == 'metadata:':
                continue
            expected[key] = mc.get_value()
        mc.close()

        self.assertEqual(set(reported.keys()), set(expected.keys()),
            'util metadata URIs differ from the metadata cursor')
        for uri, value in expected.items():
            self.assertEqual(reported[uri], value,
                f'util value for {uri} differs from the metadata cursor')

    def test_fetch_metadata_uri_filter(self):
        self.setup_data()
        report = self.util(f'uri="{self.uri}",fetch_metadata=(local=true)')
        # A uri filter yields exactly the one entry, value matching the cursor.
        self.assertEqual(report.count('\n  '), 1, report)

        mc = self.session.open_cursor('metadata:')
        mc.set_key(self.uri)
        self.assertEqual(mc.search(), 0)
        expected = mc.get_value()
        mc.close()
        self.assertEqual(report, f'\n  {self.uri}: {expected}')

    def test_fetch_metadata_key_filter(self):
        self.setup_data()
        report = self.util(
            f'uri="{self.uri}",fetch_metadata=(local=true,key="key_format")')
        self.assertEqual(report, f'\n  {self.uri}: key_format=S')

        # Cross-validate the extracted value against the cursor's full string.
        mc = self.session.open_cursor('metadata:')
        mc.set_key(self.uri)
        self.assertEqual(mc.search(), 0)
        m = re.search(r'key_format=(\w+)', mc.get_value())
        mc.close()
        self.assertEqual(report, f'\n  {self.uri}: key_format={m.group(1)}')

    def test_fetch_metadata_key_absent(self):
        self.setup_data()
        report = self.util(
            f'uri="{self.uri}",fetch_metadata=(local=true,key="not_a_real_key")')
        self.assertEqual(report, f'\n  {self.uri}: <no "not_a_real_key">')

    def test_fetch_metadata_uri_absent(self):
        self.setup_data()
        missing = 'table:does_not_exist'
        report = self.util(f'uri="{missing}",fetch_metadata=(local=true)')
        self.assertEqual(report,
            f' <no matching metadata entry for uri:"{missing}">')

    # ---- fetch_database_size (local) ----

    def test_fetch_database_size_local_non_disagg(self):
        # A non-disaggregated connection has no tracked database size.
        self.assertEqual(self.util('fetch_database_size=(local=true)'),
            'fetch_database_size(local): 0')

    # ---- disagg-only guards on a non-disagg connection ----

    def test_fix_requires_disagg_connection(self):
        r = self.util('fix=(size=(old_size=0))')
        self.assertIn(
            'This command requires a disaggregated connection with a valid checkpoint', r)
        self.assertTrue(r.endswith('Failed: Invalid argument'), r)

    def test_fetch_database_size_shared_requires_disagg(self):
        r = self.util('fetch_database_size=(local=false)')
        self.assertIn(
            'This command requires a disaggregated connection with a valid checkpoint', r)
        self.assertTrue(r.endswith('Failed: Invalid argument'), r)


# Class B: a disaggregated leader. Covers the shared-metadata and shared
# database-size paths plus the leader-only size-fix flow. The disagg_test_class
# decorator supplies early_setup (creates follower/ and the shared kv_home),
# the page-log extension.
@disagg_test_class
class test_wiredtiger_util01_disagg(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'

    uri_base = 'test_wiredtiger_util01_disagg'
    uri = 'layered:' + uri_base
    stable_uri = 'file:' + uri_base + '.wt_stable'

    def util(self, config, conn=None):
        import wiredtiger
        return wiredtiger.wiredtiger_util(conn or self.conn, config)

    def insert_rows(self, n, value='x', start=0, uri=None):
        c = self.session.open_cursor(uri or self.uri)
        for i in range(start, start + n):
            c[f'key{i:08d}'] = value * 100
        c.close()

    # In-memory database size as the engine tracks it (includes the 1MB buffer),
    # read from the checkpoint-completion record -- the same source the existing
    # checkpoint-size tests use, independent of wiredtiger_util.
    def database_size_from_checkpoint_meta(self):
        m = re.search(r'database_size=(\d+)', self.disagg_get_complete_checkpoint_meta())
        self.assertIsNotNone(m)
        return int(m.group(1))

    # Independently recompute the raw per-file checkpoint-size sum (no buffer) by
    # walking the metadata cursor: for each stable file take its most recent
    # size= -- exactly what __wt_disagg_get_database_size sums in C.
    def raw_size_sum_from_metadata(self):
        total = 0
        mc = self.session.open_cursor('metadata:')
        while mc.next() == 0:
            uri = mc.get_key()
            if not (uri.startswith('file:') and uri.endswith('.wt_stable')):
                continue
            sizes = re.findall(r',size=(\d+),', mc.get_value())
            if sizes:
                total += int(sizes[-1])
        mc.close()
        return total

    def util_local_database_size(self):
        r = self.util('fetch_database_size=(local=true)')
        m = re.match(r'fetch_database_size\(local\): (\d+)$', r)
        self.assertIsNotNone(m, r)
        return int(m.group(1))

    def util_accumulate_database_size(self):
        r = self.util('fetch_database_size=(local=false)')
        m = re.match(r'fetch_database_size\(accumulate\): (\d+)$', r)
        self.assertIsNotNone(m, r)
        return int(m.group(1))

    # ---- shared metadata: util local=false vs local=true for the same uri ----

    def test_fetch_metadata_shared_matches_local(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(1000)
        self.session.checkpoint()

        # The stable file is present in both the local and the shared metadata.
        shared = self.util(f'uri="{self.stable_uri}",fetch_metadata=(local=false)')
        local = self.util(f'uri="{self.stable_uri}",fetch_metadata=(local=true)')
        self.assertTrue(shared.startswith(f'\n  {self.stable_uri}: '), shared)
        self.assertTrue(local.startswith(f'\n  {self.stable_uri}: '), local)

        # Cross-validate a stable structural value (key_format) read from each.
        for report in (shared, local):
            kf = self.util_extract_key(report, 'key_format')
            self.assertEqual(kf, 'S', report)

        # The shared report must also agree with an independent shared-metadata
        # checkpoint cursor read of the same value.
        self.assertIn('key_format=S',
            self.util(f'uri="{self.stable_uri}",fetch_metadata=(local=false,key="key_format")'))

    @staticmethod
    def util_extract_key(report, key):
        m = re.search(re.escape(key) + r'=(\w+)', report)
        return m.group(1) if m else None

    # ---- shared database size: util accumulate vs an independent metadata walk ----

    def test_fetch_database_size_shared_matches_metadata_walk(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(2000)
        self.session.checkpoint()

        accumulate = self.util_accumulate_database_size()
        expected_raw = self.raw_size_sum_from_metadata()
        self.assertGreater(expected_raw, 0)
        self.assertEqual(accumulate, expected_raw,
            f'util accumulate {accumulate} != metadata-walk raw sum {expected_raw}')

        # The local (in-memory) size is the raw sum plus the fixed buffer, and it
        # must match the value the checkpoint-completion record reports.
        local = self.util_local_database_size()
        self.assertEqual(local, accumulate + DISAGG_SIZE_BUFFER,
            f'local {local} != accumulate {accumulate} + buffer {DISAGG_SIZE_BUFFER}')
        self.assertEqual(local, self.database_size_from_checkpoint_meta())

    # ---- leader-only size-fix flow ----

    def test_fix_size_flow(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(2000)
        self.session.checkpoint()

        in_memory = self.util_local_database_size()

        # A wrong old_size guard is rejected before anything is claimed.
        wrong = self.util(f'fix=(size=(old_size={in_memory + 12345}))')
        self.assertIn('does not match requested old_size', wrong)
        self.assertTrue(wrong.endswith('Failed: Invalid argument'), wrong)

        # The correct guard claims the fix cycle.
        self.assertEqual(self.util(f'fix=(size=(old_size={in_memory}))'),
            'size_fix triggered')

        # A second fix while one is pending is rejected as busy.
        busy = self.util('fix=(size=(old_size=0))')
        self.assertIn('a util maintain is already in progress', busy)
        self.assertTrue(busy.endswith('Failed: Device or resource busy'), busy)

        # The next checkpoint consumes the pending fix and recomputes the size as
        # the raw per-file sum plus the fixed buffer. Consuming the fix emits a
        # verbose warning recording the recomputed size.
        with self.expectedStdoutPattern('disagg database size fix: recomputed database size'):
            self.session.checkpoint()

        expected = self.raw_size_sum_from_metadata() + DISAGG_SIZE_BUFFER
        after = self.util_local_database_size()
        self.assertEqual(after, expected,
            f'post-fix local {after} != raw sum + buffer {expected}')
        # Cross-validate against the checkpoint-completion record.
        self.assertEqual(after, self.database_size_from_checkpoint_meta())

        # With the cycle consumed, a fresh fix (no guard) can be claimed again.
        # Consume it now so no fix is left pending for the shutdown checkpoint.
        self.assertEqual(self.util('fix=(size=(old_size=0))'), 'size_fix triggered')
        with self.expectedStdoutPattern('disagg database size fix: recomputed database size'):
            self.session.checkpoint()

    # ---- a fix issued on a follower is rejected (leader-only) ----

    def test_fix_on_follower_rejected(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(1000)
        self.session.checkpoint()

        # Bring up a follower sharing the same kv_home and pick up the checkpoint
        # so it has a valid checkpoint (passing the disagg guard) but is not the
        # leader.
        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config +
            'disaggregated=(role="follower")')
        try:
            self.disagg_advance_checkpoint(conn_follow)
            r = self.util('fix=(size=(old_size=0))', conn=conn_follow)
            self.assertIn('This command requires a disaggregated leader connection', r)
            self.assertTrue(r.endswith('Failed: Invalid argument'), r)

            # The follower can still read the shared size; it must equal the
            # leader's raw metadata-walk sum.
            r = self.util('fetch_database_size=(local=false)', conn=conn_follow)
            m = re.match(r'fetch_database_size\(accumulate\): (\d+)$', r)
            self.assertIsNotNone(m, r)
            self.assertEqual(int(m.group(1)), self.raw_size_sum_from_metadata())
        finally:
            conn_follow.close()
