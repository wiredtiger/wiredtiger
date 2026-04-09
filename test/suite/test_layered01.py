#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form as a compiled
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

# test_wt16252.py
#   Reproduce WT-16252: a follower that becomes leader must not reuse btree
#   IDs that the original leader already allocated.
#
#   The bug: when a leader creates tables (allocating file IDs) then drops
#   them and checkpoints, those IDs are no longer visible in metadata.  A
#   follower that picks up only that final checkpoint doesn't know the
#   highest ID the leader ever used.  When the follower steps up and
#   creates new tables it may hand out IDs that the old leader already
#   gave to now-deleted tables, causing conflicts in the shared storage
#   layer (PALI/SLS).
#
#   The fix (WT-16252) stores the ID high-water mark in the disaggregated
#   checkpoint metadata so that a stepping-up follower always starts above it.

import re
from suite_subprocess import suite_subprocess
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_wt16252(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config = 'disaggregated=(role="leader")'
    conn_config_follower = 'disaggregated=(role="follower")'


    disagg_storages = gen_disagg_storages('test_wt16252', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def extract_id(self, metadata_value):
        """Parse the numeric btree ID out of a metadata config string."""
        match = re.search(r',id=(\d+)', metadata_value)
        return int(match.group(1)) if match else None

    def max_file_id(self, session):
        """Return the highest btree ID currently visible in metadata."""
        cursor = session.open_cursor('metadata:', None, None)
        max_id = 0
        for key, value in cursor:
            if not key.startswith('file:') and key != 'metadata:':
                continue
            fid = self.extract_id(value)

            print(key, fid)
            if fid is not None and fid > max_id:
                max_id = fid
        cursor.close()
        return max_id

    def test_follower_does_not_reuse_leader_file_ids(self):
        """
        Without the WT-16252 fix the follower reuses IDs the leader already
        used, so follower_max_id <= leader_max_id after step-up.
        With the fix the follower's ID counter starts above the leader's
        high-water mark, so follower_max_id > leader_max_id.
        """
        num_tables = 50
        nrows = 100

        # Create first batch of tables with timestamped data → C1. Follower picks up C1.
        for i in range(10):
            self.session.create(f'layered:test_wt16252_{i}', 'key_format=S,value_format=S')
            cur = self.session.open_cursor(f'layered:test_wt16252_{i}')
            for r in range(nrows):
                self.session.begin_transaction()
                cur[f'key_{r}'] = f'value_{r}'
                self.session.commit_transaction(f'commit_timestamp={r + 1}')
            cur.close()
        self.conn.set_timestamp(f'stable_timestamp={nrows},oldest_timestamp={nrows // 2}')
        self.session.checkpoint()

        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,checkpoint_cleanup=(wait=1),' + self.conn_config_follower)
        self.session_follow = self.conn_follow.open_session('')
        self.disagg_advance_checkpoint(self.conn_follow)

        # Create remaining tables → C2. Follower advances to C2.
        for i in range(10, num_tables):
            self.session.create(f'layered:test_wt16252_{i}', 'key_format=S,value_format=S')
        self.conn.set_timestamp(f'stable_timestamp={nrows + 1}')
        self.session.checkpoint()

        # Record the true high-water mark after all tables are created.
        leader_max_id = self.max_file_id(self.session)

        # Follower picks up C2, then kill the leader.
        self.disagg_advance_checkpoint(self.conn_follow)
        self.session.close()
        self.conn.close('debug=(skip_checkpoint=true)')

        # Step up to leader. checkpoint_cleanup=(wait=1) already took effect at open time.
        # self.ignoreStdoutPattern('WT_VERB_CHECKPOINT|WT_VERB_READ')
        self.conn_follow.reconfigure('disaggregated=(role="leader"),verbose=(checkpoint_cleanup:5)')

        # Create new tables with timestamped data.
        ts_base = nrows + 10
        for i in range(10):
            self.session_follow.create(
                f'layered:test_wt16252_new_{i}', 'key_format=S,value_format=S')
            print(f'Created table layered:test_wt16252_new_{i}')
            cur = self.session_follow.open_cursor(f'layered:test_wt16252_new_{i}')
            if (i == 1):
                for r in range(1000):
                    self.session_follow.begin_transaction()
                    cur[f'key_{r}'] = f'value_{r}'
                    self.session_follow.commit_transaction(f'commit_timestamp={ts_base + r + 1}')
            cur.close()
        self.session_follow.checkpoint()

        leader_max_id = self.max_file_id(self.session_follow)
        print('END Leader max ID:', leader_max_id)

        # Re-open the connection and verify the metadata file.  If the follower reused IDs, the metadata file will have multiple entries with the same ID and the verify will fail.
        self.conn_follow.close()
        self.runWt(["verify", "-c"],closeconn=False)
