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

import time, wttest
from eviction_util import eviction_util
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios


# Test we don't review obsolete time window for readonly btree in follower.
# Also tests WT-16571: no crash when eviction processes checkpoint-split pages
# on disaggregated btrees during the leader→follower step-down window.
@disagg_test_class
class test_layered55(eviction_util, wttest.WiredTigerTestCase):
    conn_base_config = 'cache_size=10MB,'

    disagg_storages = gen_disagg_storages('test_layered55', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)
    uri='layered:test_layered55'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    def conn_config_follower(self):
        return self.conn_base_config + 'disaggregated=(role="follower"),'

    def read(self, nrows):
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(nrows):
            cursor.set_key(i)
            cursor.search()
            if i % 5 == 0:
                cursor.reset()
        cursor.close()

    def test_obsolete_time_window(self):
        create_params = 'key_format=i,value_format=S,block_manager=disagg'
        nrows = 10000
        value = 'k' * 1024

        self.session.create(self.uri, create_params)

        # Write some data on leader mode.
        self.populate(self.uri, 0, nrows, value)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(nrows))
        self.session.checkpoint()
        # Reopen as follower.
        self.reopen_disagg_conn(self.conn_config_follower())
        # Read data into cache.
        self.read(nrows)

        # Set oldest timestamp to an older value.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(nrows // 2))
        # Read data again which triggers eviction.
        self.read(nrows)
        # We should not review obsolete time window as btree is readonly.
        btree_stat = self.get_stat(stat.dsrc.cache_eviction_dirty_obsolete_tw, self.uri)
        conn_stat = self.get_stat(stat.conn.cache_eviction_dirty_obsolete_tw)
        self.assertEqual(btree_stat, 0)
        self.assertEqual(conn_stat, 0)

    def test_step_down_dirty_eviction(self):
        """
        WT-16571: Test that leader step-down doesn't crash when eviction encounters
        checkpoint-split (WT_PM_REC_MULTIBLOCK) pages on disaggregated btrees after
        leader=false is set but before the connection is closed.

        The crash path without the fix:
          eviction-server thread:
            __evict_page_dirty_update        (rec_result==WT_PM_REC_MULTIBLOCK, entries>1)
            → __wt_split_multi
            → __split_parent
            → __wt_page_modify_set(session, parent_page)
            → WT_ASSERT(!WT_BTREE_DISAGGREGATED || leader)   ← CRASH: leader is now false

        Root cause: the leader's stable btree dhandle (file:xxx.wt_stable) has
        WT_BTREE_DISAGGREGATED set but WT_BTREE_READONLY not set. After step-down sets
        leader=false, eviction threads can still attempt to dirty the parent of a
        checkpoint-split page, hitting the assertion.

        Fix: __disagg_step_down() must mark all disaggregated btrees as WT_BTREE_READONLY
        BEFORE setting leader=false, preventing the eviction path from dirtying them.
        """
        create_params = 'key_format=i,value_format=S,block_manager=disagg'
        nrows = 10000
        value = 'k' * 1024  # 1KB per row, ~10MB total fills the 10MB cache

        self.session.create(self.uri, create_params)

        # Write data as leader. Each row gets a unique commit timestamp.
        self.populate(self.uri, 0, nrows, value)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(nrows))

        # Checkpoint: reconciles dirty pages into WT_PM_REC_MULTIBLOCK on disk.
        # After checkpoint, many leaf pages will have:
        #   - mod->rec_result = WT_PM_REC_MULTIBLOCK  (multi-block split by checkpoint)
        #   - page_state = WT_PAGE_CLEAN              (marked clean after reconciliation)
        #
        # These pages confuse eviction: __wt_page_evict_clean() returns false (rec_result!=0),
        # so they route to __evict_page_dirty_update → __wt_split_multi → __split_parent.
        self.session.checkpoint()

        # Make eviction aggressive so it actively processes the checkpoint-split pages
        # during the step-down window. This widens the race and increases reproduction rate.
        self.conn.reconfigure(
            'eviction_dirty_target=1,eviction_dirty_trigger=5,'
            'eviction_updates_target=1,eviction_updates_trigger=5'
        )

        # Capture checkpoint metadata while the connection is still the leader.
        # This must be done before close_conn().
        checkpoint_meta = self.disagg_get_complete_checkpoint_meta()

        # --- STEP-DOWN: leader=false set here ---
        # Without the fix, eviction threads running concurrently can hit the crash path:
        #   __split_parent → __wt_page_modify_set → ASSERT(!WT_BTREE_DISAGGREGATED || leader)
        # The fix marks all disaggregated btrees as WT_BTREE_READONLY before setting
        # leader=false, making __wt_page_modify_set return early (readonly guard at line 1131).
        self.conn.reconfigure('disaggregated=(role="follower")')

        # Sleep to widen the race window, giving eviction time to encounter the
        # checkpoint-split pages and attempt to split their parents.
        # Without the fix, this reliably triggers the SIGABRT on macOS/Linux Debug builds.
        time.sleep(0.5)

        self.close_conn()

        # Reopen as follower with the captured checkpoint metadata.
        config = (self.conn_config_follower() +
                  f'disaggregated=(checkpoint_meta="{checkpoint_meta}"),')
        self.open_conn(".", config)

        # Verify all data is readable from the follower.
        cursor = self.session.open_cursor(self.uri, None, None)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        self.assertEqual(count, nrows)
