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

# A table awaiting publication keeps its contents in memory and cannot be evicted. Once the
# stable schema epoch covers the table's create, eviction publishes the table itself rather
# than leaving it in memory until the next checkpoint visits the tree.

import errno, time
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wiredtiger import stat
from wtscenario import make_scenarios

# Eviction only publishes a table it walks, so the tests need it working for its cache.
conn_base_config = 'cache_size=20MB,statistics=(all),debug_mode=(eviction=true),' \
                 + 'eviction_dirty_target=1,'

@disagg_test_class
class test_layered_schema33(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__

    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    nitems = 5000

    def published_count(self):
        return self.get_stat(stat.conn.disagg_publish_epoch_cleared)

    def wait_for_published(self, uri, expected):
        """
        Wait for eviction to publish the table. Eviction only visits a table when it wants its
        memory, so the inserts below keep the cache under pressure until it does.
        """
        for _ in range(600):
            if self.published_count() >= expected:
                self.assertEqual(self.published_count(), expected)
                return
            self.insert(uri, self.nitems, 100, 20)
            time.sleep(0.1)
        self.fail('eviction never published %d tables, saw %d' %
                  (expected, self.published_count()))

    def insert(self, uri, start, count, commit_ts):
        cursor = self.session.open_cursor(uri)
        for i in range(start, start + count):
            self.session.begin_transaction()
            cursor[str(i)] = 'v' * 100 + str(i)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def check(self, uri, count):
        cursor = self.session.open_cursor(uri)
        seen = 0
        while cursor.next() == 0:
            seen += 1
        cursor.close()
        self.assertEqual(seen, count)

    def test_eviction_publishes_covered_table(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.insert(uri, 0, self.nitems, 20)

        # An unpublished table has nothing to publish.
        self.assertEqual(self.published_count(), 0)

        # Published, but the stable epoch does not cover the create yet.
        self.publish(uri, 10)
        self.assertEqual(self.published_count(), 0)

        # Once the epoch covers the create, eviction publishes the table without a checkpoint.
        self.set_stable_epoch(10)
        self.wait_for_published(uri, 1)

        # The table behaves normally from here: the checkpoint succeeds and the data survives.
        self.leader_checkpoint(30)
        self.check(uri, self.nitems + 100)

    def test_publication_lets_eviction_take_pages(self):
        # The mirror of test_layered_schema26, which shows eviction skipping the table while it
        # awaits publication. Once the epoch covers the create, eviction takes its pages, and does
        # so without a checkpoint having run.
        self.set_stable_epoch(1)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(10))

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=i,value_format=S')
        self.publish(uri, 20)

        nrows = 300
        with self.transaction(commit_timestamp=30):
            with wttest.open_cursor(self.session, uri) as cursor:
                for i in range(1, nrows + 1):
                    cursor[i] = 'v' * 2048
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))

        # The epoch does not cover the create, so the table holds its pages in memory.
        self.assertEqual(
            self.get_stat(stat.dsrc.cache_eviction_pages_seen, uri=self.stable_uri(uri)), 0)

        # Covering the create releases the table to eviction, with no checkpoint in between.
        self.set_stable_epoch(20)
        self.assertStatGreaterSoon(
            stat.dsrc.cache_eviction_pages_seen, 0, uri=self.stable_uri(uri), timeout=60)
        self.assertGreater(self.published_count(), 0)

        with wttest.open_cursor(self.session, uri) as cursor:
            self.assertEqual(sum(1 for _ in cursor), nrows)

    def test_eviction_publishes_every_covered_table(self):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        covered = ['layered:' + self.test_name + str(i) for i in range(3)]
        for uri in covered:
            self.session.create(uri, 'key_format=S,value_format=S')
            self.insert(uri, 0, 100, 20)
            self.publish(uri, 10)

        above = 'layered:' + self.test_name + '_above'
        self.session.create(above, 'key_format=S,value_format=S')
        self.publish(above, 20)

        # The table published above the new epoch keeps waiting.
        self.set_stable_epoch(10)
        self.wait_for_published(covered[0], len(covered))

        self.set_stable_epoch(20)
        self.wait_for_published(above, len(covered) + 1)

    def test_drop_blocked_until_checkpoint(self):
        # The published table still holds uncheckpointed data, so the drop must keep refusing
        # until a checkpoint persists it.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.insert(uri, 0, self.nitems, 20)
        self.publish(uri, 10)
        self.set_stable_epoch(10)
        self.wait_for_published(uri, 1)

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.drop(uri, None))
        err, sub, msg = self.session.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, wiredtiger.WT_DIRTY_DATA)
        self.assertTrue('unpublished data' in msg)

        self.leader_checkpoint(30)
        self.dropUntilSuccess(self.session, uri)

    def test_verify_after_publish(self):
        # Verify skips a table awaiting publication. Once it is published the table is an
        # ordinary one: verifying it while it still holds dirty data reports the same busy
        # error any dirty table reports, and it verifies once a checkpoint has run.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.insert(uri, 0, self.nitems, 20)
        self.publish(uri, 10)
        self.set_stable_epoch(10)
        self.wait_for_published(uri, 1)

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.verify(uri, None))
        err, sub, msg = self.session.get_last_error()
        self.assertEqual(err, errno.EBUSY)
        self.assertEqual(sub, wiredtiger.WT_DIRTY_DATA)

        self.leader_checkpoint(30)
        self.session.verify(uri, None)
        self.check(uri, self.nitems + 100)

    def test_table_above_the_epoch_keeps_waiting(self):
        # The stable epoch never reaches the table's publish epoch, so the table keeps waiting
        # through checkpoints, its data stays available, and the drop keeps being refused.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.publish(uri, 20)

        # A table awaiting publication may only hold data the checkpoint does not consider
        # stable, so commit above the timestamp the checkpoint below runs at.
        self.insert(uri, 0, 100, 50)

        self.leader_checkpoint(30)
        self.assertEqual(self.published_count(), 0)

        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.session.drop(uri, None))
        self.check(uri, 100)

    def test_step_up_publishes(self):
        # A table created and published on a follower has no stable constituent until this
        # node steps up and rebuilds it from the queue entry. That entry is the only record of
        # the published epoch, as no publish call will ever run for the table again, so
        # publishing after the step up proves the rebuilt btree finds it.
        # Reopening in disaggregated mode reports that it removed the local history store.
        self.ignoreStdoutPattern('wiredtiger_open:.*WT_VERB_METADATA')
        self.reopen_conn(config=self.conn_config_follower)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.publish(uri, 10)

        self.step_up()
        self.insert(uri, 0, self.nitems, 20)
        self.set_stable_epoch(10)
        self.wait_for_published(uri, 1)

        self.leader_checkpoint(30)
        self.check(uri, self.nitems + 100)

    def test_follower_does_not_publish(self):
        # Only a leader writes pages, so a follower has nothing to publish and must leave the
        # table alone when the epoch advances.
        # Reopening in disaggregated mode reports that it removed the local history store.
        self.ignoreStdoutPattern('wiredtiger_open:.*WT_VERB_METADATA')
        self.reopen_conn(config=self.conn_config_follower)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.set_stable_epoch(5)

        uri = 'layered:' + self.test_name
        self.session.create(uri, 'key_format=S,value_format=S')
        self.publish(uri, 10)

        self.set_stable_epoch(10)
        self.session.checkpoint()
        self.assertEqual(self.published_count(), 0)
