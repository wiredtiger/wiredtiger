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

import wiredtiger, wttest
from wiredtiger.packing import unpack
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wiredtiger import stat
from wtscenario import make_scenarios

# Leader blind removes succeed on absent keys and preserve existing cursor positions.
@disagg_test_class
class test_layered_cursor35(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = 'layered:' + test_name
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    update_tombstone = 4

    disagg_storages = gen_disagg_storages(disagg_only=True)
    layouts = [
        ('updates', dict(checkpointed=False)),
        ('checkpoint', dict(checkpointed=True)),
    ]
    scenarios = make_scenarios(disagg_storages, layouts)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def open_blind_cursor(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v1', 'k2': 'v2'}, 10)
        if self.checkpointed:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
            self.session.checkpoint()
        return self.session.open_cursor(self.uri, None, 'blind_remove=true')

    def finish(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))

    def test_leader_remove(self):
        cursor = self.open_blind_cursor()
        self.session.begin_transaction()
        cursor.set_key('missing')
        self.assertEqual(cursor.remove(), 0)
        cursor.set_key('k1')
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k2': 'v2'})
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 2)
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_follower), 0)
        self.assertEqual(self.remove_versions('missing'), {})
        self.finish()

    # Repeated blind removes on a leader preserve the position and continue the walk.
    def test_positioned_orphan_blind_remove(self):
        cursor = self.open_blind_cursor()
        try:
            for key, direction, neighbour in (('k1', 'next', 'k2'), ('k2', 'prev', 'k1')):
                self.session.begin_transaction()
                try:
                    cursor.set_key(key)
                    self.assertEqual(cursor.search(), 0)
                    self.assertEqual(cursor.remove(), 0)
                    self.assertEqual(cursor.get_key(), key)
                    versions = self.remove_versions(key)

                    self.assertEqual(cursor.remove(), 0)
                    self.assertEqual(cursor.get_key(), key)
                    self.assertEqual(self.remove_versions(key), versions)
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: cursor.get_value(), '/requires value be set/')
                    self.assertEqual(getattr(cursor, direction)(), 0)
                    self.assertEqual(cursor.get_key(), neighbour)
                finally:
                    self.session.rollback_transaction()
        finally:
            cursor.close()
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 4)
        self.finish()

    def remove_versions(self, key):
        uris = [self.stable_uri(self.uri)]
        versions = {}
        for uri in uris:
            cursor = self.session.open_cursor(uri, None, 'debug=(dump_version=(enabled=true))')
            try:
                cursor.set_key(key)
                entries = []
                if cursor.search() == 0:
                    entries.append(self.version_entry(cursor))
                    while cursor.next() == 0 and cursor.get_key() == key:
                        entries.append(self.version_entry(cursor))
                if entries:
                    versions[uri] = entries
            finally:
                cursor.close()
        return versions

    def version_entry(self, cursor):
        metadata, value = cursor._get_version_cursor_value()
        metadata = unpack('QQQQQQQQQQBBBB', metadata)
        return metadata[1], metadata[6], metadata[10], value

    def assert_no_consecutive_deletes(self, versions):
        for uri, entries in versions.items():
            deletes = [entry[2] == self.update_tombstone for entry in entries]
            self.assertFalse(any(a and b for a, b in zip(deletes, deletes[1:])),
                f'Consecutive deletion versions in {uri}: {entries}')

    def test_unpositioned_missing_remove(self):
        cursor = self.open_blind_cursor()
        with self.transaction(rollback=True):
            cursor.set_key('missing')
            self.assertEqual(cursor.remove(), 0)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.get_key(), '/requires key be set/')
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.get_value(), '/requires value be set/')
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), 'k1')
        cursor.close()
        self.finish()

    # An eviction request must not make a positioned no-op lose its place in the walk.
    def test_positioned_remove_after_eviction_request(self):
        cursor = self.open_blind_cursor()
        evict_cursor = self.session.open_cursor(
            self.stable_uri(self.uri), None, 'debug=(release_evict)')
        self.session.begin_transaction()
        try:
            cursor.set_key('k1')
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.remove(), 0)
            evict_cursor.set_key('k1')
            self.assertEqual(evict_cursor.search(), wiredtiger.WT_NOTFOUND)
            evict_cursor.reset()
            self.assertEqual(cursor.remove(), 0)
            self.assertEqual(cursor.get_key(), 'k1')
            self.assertEqual(cursor.next(), 0)
            self.assertEqual(cursor.get_key(), 'k2')
        finally:
            self.session.rollback_transaction()
            cursor.close()
            evict_cursor.close()
        self.finish()

    def test_default_and_reconfigure(self):
        self.open_blind_cursor().close()
        for config in (None, 'blind_remove=false'):
            cursor = self.session.open_cursor(self.uri, None, config)
            with self.transaction(rollback=True):
                cursor.set_key('missing')
                self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
                cursor.set_key('k1')
                self.assertEqual(cursor.search(), 0)
                self.assertEqual(cursor.remove(), 0)
                self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
            cursor.close()
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 0)

        cursor = self.session.open_cursor(self.uri)
        for enabled in (True, False, True):
            cursor.reconfigure('blind_remove=' + str(enabled).lower())
            with self.transaction(rollback=True):
                cursor.set_key('missing')
                self.assertEqual(cursor.remove(), 0 if enabled else wiredtiger.WT_NOTFOUND)
        cursor.close()
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 2)
        self.finish()

    # A write through another cursor must not make a subsequent blind remove a stale no-op.
    def test_remove_reinsert_remove_same_transaction(self):
        cursor = self.open_blind_cursor()
        other = self.session.open_cursor(self.uri)
        with self.transaction(commit_timestamp=30):
            cursor.set_key('k1')
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.remove(), 0)
            other['k1'] = 'reinserted'
            self.assertEqual(cursor.remove(), 0)
        cursor.close()
        other.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k2': 'v2'})
        self.finish()

    # Removing a key read from a rolled-back insert succeeds without recreating it.
    def test_remove_rolled_back_insert(self):
        cursor = self.open_blind_cursor()
        writer = self.conn.open_session('')
        writer_cursor = writer.open_cursor(self.uri)
        evict_cursor = self.session.open_cursor(
            self.stable_uri(self.uri), None, 'debug=(release_evict)')
        try:
            writer.begin_transaction()
            writer_cursor['k1_rolled_back'] = 'uncommitted'
            self.session.begin_transaction('isolation=read-uncommitted')
            cursor.set_key('k1_rolled_back')
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'uncommitted')
            self.session.commit_transaction()
            self.assertEqual(cursor.get_key(), 'k1_rolled_back')
            writer.rollback_transaction()
            evict_cursor.set_key('k1_rolled_back')
            self.assertEqual(evict_cursor.search(), wiredtiger.WT_NOTFOUND)
            evict_cursor.reset()
            self.session.begin_transaction()
            self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        finally:
            cursor.close()
            evict_cursor.close()
            writer_cursor.close()
            writer.close()
        self.assertEqual(self.remove_versions('k1_rolled_back'), {})
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        self.finish()

    def check_remove_history(self, reinsert):
        cursor = self.open_blind_cursor()
        with self.transaction(commit_timestamp=30):
            cursor.set_key('k1')
            self.assertEqual(cursor.remove(), 0)
        if reinsert:
            self.write_at(self.uri, {'k1': 'reinserted'}, 40)
        versions = self.remove_versions('k1')
        with self.transaction(commit_timestamp=50):
            cursor.set_key('k1')
            self.assertEqual(cursor.remove(), 0)
        cursor.close()
        if not reinsert:
            self.assertEqual(self.remove_versions('k1'), versions)
        final_versions = self.remove_versions('k1')
        self.assert_no_consecutive_deletes(final_versions)

        def check_reads():
            for ts in (15, 30, 35, 40, 45, 50, 60):
                expected = {'k2': 'v2'}
                if ts < 30:
                    expected['k1'] = 'v1'
                elif reinsert and 40 <= ts < 50:
                    expected['k1'] = 'reinserted'
                self.assertEqual(self.read_kvs_at(self.uri, ts), expected)

        check_reads()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(60))
        self.session.checkpoint()
        check_reads()
        self.reopen_conn()
        check_reads()

    def test_repeated_remove_timestamps(self):
        self.check_remove_history(False)

    def test_remove_reinsert_remove_timestamps(self):
        self.check_remove_history(True)

    def test_conflicts(self):
        cursor = self.open_blind_cursor()
        other = self.conn.open_session('')
        other_cursor = other.open_cursor(self.uri)
        try:
            for positioned in (False, True):
                for prepared in (False, True):
                    self.session.begin_transaction()
                    cursor.set_key('k1')
                    if positioned:
                        self.assertEqual(cursor.search(), 0)
                    other.begin_transaction()
                    try:
                        other_cursor['k1'] = 'uncommitted'
                        if prepared:
                            other.prepare_transaction('prepare_timestamp=' + self.timestamp_str(30))
                        before = self.get_stat(stat.conn.layered_curs_blind_remove_leader)
                        self.assertRaisesException(wiredtiger.WiredTigerError,
                            lambda: cursor.remove(), '/WT_ROLLBACK/')
                        self.assertEqual(
                            self.get_stat(stat.conn.layered_curs_blind_remove_leader), before)
                    finally:
                        self.session.rollback_transaction()
                        other.rollback_transaction()
        finally:
            cursor.close()
            other_cursor.close()
            other.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        self.finish()

    # Newer committed inserts and deletes must conflict with an older reader in either mode.
    def test_committed_history_conflicts(self):
        self.open_blind_cursor().close()
        writer = self.conn.open_session('')
        writer_cursor = writer.open_cursor(self.uri)
        try:
            for insert, key in ((True, 'k0_inserted'), (False, 'k1')):
                readers = []
                try:
                    def add_readers():
                        for blind in (False, True):
                            for positioned in ((False,) if insert else (False, True)):
                                reader = self.conn.open_session('')
                                cursor = reader.open_cursor(
                                    self.uri, None, 'blind_remove=' + str(blind).lower())
                                readers.append((reader, cursor, positioned))
                                reader.begin_transaction('read_timestamp=' + self.timestamp_str(15))
                                cursor.set_key(key)
                                self.assertEqual(
                                    cursor.search(), wiredtiger.WT_NOTFOUND if insert else 0)

                    add_readers()
                    with self.transaction(session=writer, commit_timestamp=30):
                        if insert:
                            writer_cursor[key] = 'committed'
                        else:
                            writer_cursor.set_key(key)
                            self.assertEqual(writer_cursor.remove(), 0)

                    # New snapshots also conflict when their read timestamp predates the write.
                    add_readers()
                    before = self.get_stat(stat.conn.layered_curs_blind_remove_leader)
                    for reader, cursor, positioned in readers:
                        if not positioned:
                            cursor.set_key(key)
                        self.assertRaisesException(wiredtiger.WiredTigerError,
                            lambda: cursor.remove(), '/WT_ROLLBACK/')
                    self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), before)
                finally:
                    for reader, cursor, _ in readers:
                        reader.rollback_transaction()
                        cursor.close()
                        reader.close()
        finally:
            writer_cursor.close()
            writer.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k0_inserted': 'committed', 'k2': 'v2'})
        self.finish()

    def test_autocommit_remove(self):
        cursor = self.open_blind_cursor()
        try:
            cursor.set_key('missing')
            self.assertEqual(cursor.remove(), 0)
            self.assertEqual(self.remove_versions('missing'), {})

            cursor.set_key('k1')
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.remove(),
                '/commit timestamp is required for writes to disaggregated tables/')
            self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})

            self.conn.reconfigure('debug_mode=(disagg_commit_ts_optional=true)')
            cursor['k0_autocommit'] = 'untimestamped'
            cursor.set_key('k0_autocommit')
            self.assertEqual(cursor.remove(), 0)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.get_key(), '/requires key be set/')
            versions = self.remove_versions('k0_autocommit')
            cursor.set_key('k0_autocommit')
            self.assertEqual(cursor.remove(), 0)
            self.assertEqual(self.remove_versions('k0_autocommit'), versions)
            self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        finally:
            cursor.close()
            self.conn.reconfigure('debug_mode=(disagg_commit_ts_optional=false)')
        self.finish()

    # A retryable conflict is handled internally before the autocommit delete succeeds.
    def test_autocommit_remove_retry(self):
        cursor = self.open_blind_cursor()
        try:
            self.conn.reconfigure('debug_mode=(disagg_commit_ts_optional=true)')
            keys = [f'retry_{i}' for i in range(20)]
            for key in keys:
                cursor[key] = 'untimestamped'
            self.conn.reconfigure('debug_mode=(rollback_error=5)')

            retries = self.get_stat(stat.conn.autocommit_update_retry)
            for key in keys:
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
            self.assertGreater(self.get_stat(stat.conn.autocommit_update_retry), retries)
            self.conn.reconfigure('debug_mode=(rollback_error=0)')
            for key in keys:
                versions = self.remove_versions(key)
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
                self.assertEqual(self.remove_versions(key), versions)
            self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        finally:
            cursor.close()
            self.conn.reconfigure('debug_mode=(disagg_commit_ts_optional=false,rollback_error=0)')
        self.finish()

    def test_missing_key_argument(self):
        cursor = self.open_blind_cursor()
        with self.transaction(rollback=True):
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.remove(), '/requires key be set/')
        cursor.close()
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 0)
        self.finish()

    # A cursor keeps its option across role changes and operations count against the current role.
    def test_cursor_role_change(self):
        cursor = self.open_blind_cursor()
        with self.transaction(commit_timestamp=30):
            cursor.set_key('missing_leader')
            self.assertEqual(cursor.remove(), 0)
        cursor.reset()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()
        self.conn.reconfigure('disaggregated=(role="follower")')
        with self.transaction(commit_timestamp=40):
            cursor.set_key('missing_follower')
            self.assertEqual(cursor.remove(), 0)
        cursor.reset()
        self.ignoreStdoutPattern('step-up drain skipped orphan removes')
        self.conn.reconfigure('disaggregated=(role="leader")')
        with self.transaction(commit_timestamp=50):
            cursor.set_key('missing_leader_again')
            self.assertEqual(cursor.remove(), 0)
        cursor.close()
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 2)
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_follower), 1)
        self.assertEqual(self.get_stat(stat.conn.disagg_step_up_ingest_drain_orphan_remove), 1)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(60))
        self.session.checkpoint()

    # Removing a key outside the cursor bounds fails with an invalid argument error and leaves the
    # key as it was.
    def test_leader_remove_out_of_bounds(self):
        cursor = self.open_blind_cursor()
        cursor.set_key('k2')
        cursor.bound('bound=lower')
        self.session.begin_transaction()
        cursor.set_key('k1')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: cursor.remove(),
            '/blind_remove cannot remove a key outside the cursor bounds/')
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        self.finish()

# Step-down removes retain their existing missing-key behavior.
@disagg_test_class
class test_layered_cursor35_stepdown(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = 'layered:' + test_name
    layouts = [
        ('updates', dict(checkpointed=False)),
        ('checkpoint', dict(checkpointed=True)),
    ]
    windows = [
        ('mirrored', dict(write_mirroring=True)),
        ('ingest_only', dict(write_mirroring=False)),
    ]
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), windows, layouts)

    def conn_config(self):
        return 'statistics=(all),precise_checkpoint=true,disaggregated=(role="leader",' + \
            'stepdown_write_mirroring=' + str(self.write_mirroring).lower() + ')'

    def setUp(self):
        super().setUp()
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'v1', 'k2': 'v2'}, 10)
        if self.checkpointed:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
            self.session.checkpoint()
        self.set_step_down_ts(20)

    def test_stepdown_remove(self):
        cursor = self.session.open_cursor(self.uri, None, 'blind_remove=true')
        with self.transaction(commit_timestamp=30):
            cursor.set_key('missing')
            self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
            cursor.set_key('k1')
            self.assertEqual(cursor.remove(), 0)
            cursor.set_key('k1')
            self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k2': 'v2'})
        self.assertEqual(self.get_stat(stat.conn.layered_curs_blind_remove_leader), 1)
        self.complete_step_down(20)

    def test_stepdown_remove_out_of_bounds(self):
        cursor = self.session.open_cursor(self.uri, None, 'blind_remove=true')
        cursor.set_key('k2')
        cursor.bound('bound=lower')
        with self.transaction(rollback=True):
            cursor.set_key('k1')
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.remove(), '/blind_remove cannot remove a key outside the cursor bounds/')
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'v1', 'k2': 'v2'})
        self.complete_step_down(20)
