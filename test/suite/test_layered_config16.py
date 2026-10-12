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

# test_layered_config16.py
#    A database whose shared metadata table entry was created without the disaggregated block
#    manager is upgraded when the connection opens.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_config16(wttest.WiredTigerTestCase):
    conn_config = 'precise_checkpoint=true,disaggregated=(role="leader")'
    uri = 'layered:test_layered_config16'
    shared_metadata_uri = 'file:WiredTigerShared.wt_stable'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def shared_metadata_config(self):
        cursor = self.session.open_cursor('file:WiredTiger.wt')
        cursor.set_key(self.shared_metadata_uri)
        self.assertEqual(cursor.search(), 0)
        value = cursor.get_value()
        cursor.close()
        return value

    def test_legacy_shared_metadata(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor['key'] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        cursor.close()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

        value = self.shared_metadata_config()
        self.assertIn('block_manager=disagg', value)

        # A follower does not checkpoint the shared metadata table, so the rewritten entry survives
        # the restart, as an entry written by an older version would.
        self.conn.reconfigure('disaggregated=(role="follower")')
        cursor = self.session.open_cursor('file:WiredTiger.wt')
        cursor.set_key(self.shared_metadata_uri)
        cursor.set_value(value.replace('block_manager=disagg', 'block_manager=default'))
        self.assertEqual(cursor.insert(), 0)
        cursor.close()
        self.assertNotIn('block_manager=disagg', self.shared_metadata_config())

        self.reopen_conn()
        self.assertIn('block_manager=disagg', self.shared_metadata_config())

        cursor = self.session.open_cursor(self.uri)
        self.assertEqual(cursor['key'], 'value')
        cursor.close()
        self.session.checkpoint()
        self.assertEqual(self.session.verify(self.shared_metadata_uri), 0)

    def local_config(self, uri):
        cursor = self.session.open_cursor('file:WiredTiger.wt')
        cursor.set_key(uri)
        self.assertEqual(cursor.search(), 0)
        value = cursor.get_value()
        cursor.close()
        return value

    def test_legacy_local_file_entry(self):
        stable_uri = 'file:test_layered_config16.wt_stable'

        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor['key'] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        cursor.close()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()
        self.conn.reconfigure('disaggregated=(role="follower")')

        # Rewrite the local stable entry as an older version would have written it.
        value = self.local_config(stable_uri)
        self.assertIn('block_manager=disagg', value)
        cursor = self.session.open_cursor('file:WiredTiger.wt')
        cursor.set_key(stable_uri)
        cursor.set_value(value.replace('block_manager=disagg', 'block_manager=default'))
        self.assertEqual(cursor.insert(), 0)
        cursor.close()
        self.assertNotIn('block_manager=disagg', self.local_config(stable_uri))

        # The rewritten entry survives a restart. Picking up the checkpoint the entry already holds
        # still marks it as a shared tree.
        self.reopen_conn(config='precise_checkpoint=true,disaggregated=(role="follower")')
        self.assertNotIn('block_manager=disagg', self.local_config(stable_uri))
        self.conn.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')
        self.assertIn('block_manager=disagg', self.local_config(stable_uri))

    def shared_entry(self, uri):
        cursor = self.session.open_cursor(self.shared_metadata_uri)
        cursor.set_key(uri)
        self.assertEqual(cursor.search(), 0)
        value = cursor.get_value()
        cursor.close()
        return value

    def test_legacy_stable_entry_rewritten_by_checkpoint(self):
        stable_uri = 'file:test_layered_config16.wt_stable'

        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor['key1'] = 'value1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        value = self.shared_entry(stable_uri)
        self.assertIn('block_manager=disagg', value)

        # Leave the stable entry in the shared metadata table as an older version wrote it.
        shared_cursor = self.session.open_cursor(self.shared_metadata_uri)
        shared_cursor.set_key(stable_uri)
        shared_cursor.set_value(value.replace('block_manager=disagg', 'block_manager=default'))
        self.assertEqual(shared_cursor.insert(), 0)
        shared_cursor.close()
        self.assertNotIn('block_manager=disagg', self.shared_entry(stable_uri))

        # Checkpointing the table writes its entry to the shared metadata table again.
        self.session.begin_transaction()
        cursor['key2'] = 'value2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(2))
        self.session.checkpoint()
        self.assertIn('block_manager=disagg', self.shared_entry(stable_uri))
