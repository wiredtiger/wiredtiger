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

import wiredtiger, wttest

# Tests checkpoint behavior with prepared transactions, specifically:
# - Writing prepared updates to disk during checkpoint

class test_prepare36(wttest.WiredTigerTestCase):

    conn_config = 'checkpoint=(precise=true),preserve_prepared=true,statistics=(all)'
    uri = 'table:test_prepare36'

    def get_stats(self, stats):
        """Get the current values of multiple statistics."""
        stat_cursor = self.session.open_cursor('statistics:'+self.uri)
        results = {}
        for stat in stats:
            results[stat] = stat_cursor[stat][2]
        stat_cursor.close()
        return results

    def checkpoint_and_verify_stats(self, expected_changes):
        """
        Perform a checkpoint and verify the expected changes in multiple statistics.

        Args:
            expected_changes: Dict mapping stat -> bool
                             where True means expect increase, False means expect no change
        """
        stats_to_check = list(expected_changes.keys())
        old_stats = self.get_stats(stats_to_check)

        self.session.checkpoint()

        new_stats = self.get_stats(stats_to_check)

        for stat, expect_increase in expected_changes.items():
            diff = new_stats[stat] - old_stats[stat]
            if expect_increase:
                self.assertGreater(diff, 0,
                    f"Stat {stat}: expected increase, got diff {diff}")
            else:
                self.assertEqual(diff, 0,
                    f"Stat {stat}: expected no change, got diff {diff}")

        return new_stats

    def check_ckpt_hs(self, expected_hs_value, expected_hs_start_ts,
                      expected_hs_stop_ts, expect_prepared_in_datastore = False):
        session = self.conn.open_session(self.session_config)
        session.checkpoint()
        # Check the data file value.
        cursor = session.open_cursor(self.uri, None, 'checkpoint=WiredTigerCheckpoint')

        # We no longer need to do anything special if we are expecting prepared updates
        # in the datastore, because checkpoint cursors always set ignore_prepare.

        # for _, value in cursor:
        #     self.assertEqual(value, expected_data_value)

        cursor.close()
        # Check the history store file value.
        cursor = session.open_cursor("file:WiredTigerHS.wt", None, 'checkpoint=WiredTigerCheckpoint')
        for _, _, hs_start_ts, _, hs_stop_ts, _, type, value in cursor:
            # No WT_UPDATE_TOMBSTONE in the history store.
            self.assertNotEqual(type, 5)
            # No WT_UPDATE_RESERVE? in the history store.
            self.assertNotEqual(type, 1)
            # WT_UPDATE_STANDARD
            # if (type == 3):
                # self.assertEqual(value.decode(), expected_hs_value + '\x00')
            self.assertEqual(hs_start_ts, expected_hs_start_ts)
            self.assertEqual(hs_stop_ts, expected_hs_stop_ts)

        cursor.close()
        session.close()

    def test_hs_commit_prepare(self):
        # Setup: Initialize timestamps with stable < prepare timestamp
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        if 'disagg' in self.hook_names:
            self.skipTest("Skip test until cell packing/unpacking is supported for page delta and tier storage")
        create_params = 'key_format=i,value_format=S'
        self.session.create(self.uri, create_params)

        # Step 1: Insert committed baseline data for keys 1-20
        cursor_committed = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, 22):
            cursor_committed.set_key(i)
            cursor_committed.set_value("committed_value_" + str(i))
            cursor_committed.insert()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(25))
        cursor_committed.close()

        # Step 2: Create first prepared transaction for key 21 with prepared_id=1
        session_prepare = self.conn.open_session()
        cursor_prepare = session_prepare.open_cursor(self.uri)
        session_prepare.begin_transaction()
        cursor_prepare.set_key(21)
        cursor_prepare.set_value("prepared_value_21")
        cursor_prepare.insert()
        session_prepare.prepare_transaction('prepare_timestamp=' + self.timestamp_str(30)+',prepared_id=' + self.prepared_id_str(1))
        cursor_prepare.close()

        # Make stable timestamp equal to prepare timestamp - this should allow checkpoint to reconcile prepared update
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        # Verify checkpoint writes prepared time window to disk
        self.checkpoint_and_verify_stats({
            wiredtiger.stat.dsrc.rec_time_window_prepared: True,
        })

        # Step 3: Force eviction to trigger reconciliation with the prepared data
        # This ensures the prepared update gets written to disk storage
        session_evict = self.conn.open_session("debug=(release_evict_page=true)")
        session_evict.begin_transaction("ignore_prepare=true")
        evict_cursor = session_evict.open_cursor(self.uri, None, None)
        for i in range(1, 21):  # Evict committed data pages
            evict_cursor.set_key(i)
            self.assertEqual(evict_cursor.search(), 0)
            evict_cursor.reset()
        evict_cursor.close()
        session_evict.rollback_transaction()
        
        session_prepare.commit_transaction('commit_timestamp=' + self.timestamp_str(35)+', durable_timestamp='+ self.timestamp_str(40))

        session_prepare.close()

        self.session.begin_transaction('read_timestamp='+ self.timestamp_str(40)+ ',ignore_prepare=true')
        read_cursor = self.session.open_cursor(self.uri, None, None)
        read_cursor.set_key(21)
        self.assertEqual(read_cursor.search(), 0)
        self.assertEqual(read_cursor.get_value(), 'prepared_value_21')
        self.session.rollback_transaction()

        self.session.begin_transaction('read_timestamp='+ self.timestamp_str(30)+ ',ignore_prepare=true')
        read_cursor = self.session.open_cursor(self.uri, None, None)
        read_cursor.set_key(21)
        self.assertEqual(read_cursor.search(), 0)
        self.assertEqual(read_cursor.get_value(), 'committed_value_21')
        self.session.rollback_transaction()
        
        # move stable timestamp and do another checkpoint where commit ts is still not stable,
        # we might try to reinsert the same update to the history store, check that there's no error here
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(35))
        self.check_ckpt_hs('committed_value_21', 25, 18446744073709551615)        
        
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(41))
        self.check_ckpt_hs('committed_value_21', 25, 40)     