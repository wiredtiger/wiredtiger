#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

import re, wttest
from wiredtiger import WT_NOTFOUND
from wiredtiger.packing import pack

# Exploratory: a prepared rollback after eviction removes a history store record, and the next
# record written at the same start timestamp takes a new counter, leaving a gap.
class test_hs_counter_gap(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    uri = 'table:hs_counter_gap'
    max_counter = 6

    def evict(self, key):
        evict_cursor = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        self.session.begin_transaction('ignore_prepare=true')
        evict_cursor.set_key(key)
        evict_cursor.search()
        evict_cursor.reset()
        self.session.rollback_transaction()
        evict_cursor.close()

    def commit(self, key, value, ts):
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c.close()

    def btree_id(self):
        c = self.session.open_cursor('metadata:', None, None)
        config = c['file:hs_counter_gap.wt']
        c.close()
        return int(re.search(r'\bid=(\d+)', config).group(1))

    # Return the counters of the history store records that still exist for the key at the given
    # start timestamp. Point searches only: a scan marks pages holding deleted records for eviction,
    # which drops the removed record and hides the gap. Reading at timestamp 1 keeps the records'
    # real stop times invisible, so only a removal hides a record.
    def hs_counters(self, key, start_ts):
        session = self.conn.open_session('cache_cursors=false')
        session.begin_transaction('read_timestamp=1')
        c = session.open_cursor('file:WiredTigerHS.wt', None, None)
        present = []
        for counter in range(self.max_counter):
            c.set_key(self.btree_id(), pack('S', key), start_ts, counter)
            if c.search() == 0:
                present.append(counter)
        c.close()
        session.rollback_transaction()
        session.close()
        return present

    def test_prepare_rollback(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')

        # Three versions at the same start timestamp.
        self.commit('k', 'a' * 100, 5)
        self.commit('k', 'b' * 100, 5)
        self.commit('k', 'c' * 100, 5)

        # Evict with a prepared update on top: the three versions move to the history store.
        prep = self.conn.open_session()
        prep_cursor = prep.open_cursor(self.uri)
        prep.begin_transaction()
        prep_cursor['k'] = 'p' * 100
        prep.prepare_transaction('prepare_timestamp=' + self.timestamp_str(10))
        self.evict('k')
        present = self.hs_counters('k', 5)
        self.prout('after eviction with prepared update: counters %s' % present)
        self.assertEqual(present, [0, 1, 2])

        # Roll back and evict: the newest version is restored to the page and its history store
        # record is removed.
        prep.rollback_transaction()
        prep_cursor.close()
        prep.close()
        self.evict('k')
        present = self.hs_counters('k', 5)
        self.prout('after rollback and eviction: counters %s' % present)
        self.assertEqual(present, [0, 1])

        # Write two more versions and evict: the restored version goes back to the history store
        # at the same start timestamp, under a new counter.
        self.commit('k', 'd' * 100, 5)
        self.commit('k', 'e' * 100, 20)
        self.evict('k')
        present = self.hs_counters('k', 5)
        self.prout('after reinsert: counters %s' % present)
        self.assertEqual(present, [0, 1, 3, 4])

        self.session.checkpoint()
        self.verifyUntilSuccess(uri=self.uri)

if __name__ == '__main__':
    wttest.run()
