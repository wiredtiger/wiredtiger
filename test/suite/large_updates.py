#!/usr/bin/env python

import wiredtiger, wttest, time, string
from wiredtiger import stat
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

class test_foo(wttest.WiredTigerTestCase):

    def large_updates(self, uri, value, ds, nrows, commit_ts):
        # Update a large number of records, we'll hang if the history store table isn't working.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def test_it(self):

        nrows = 10000
        self.uri = 'table:test'

        # Create and populate
        ds = SimpleDataSet(self, self.uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()
        bigvalue = "aaaaa" * 100
        self.large_updates(self.uri, bigvalue, ds, nrows, 1)

        #self.session.create(self.uri, 'key_format=i,value_format=i')
        #c = self.session.open_cursor(self.uri)

        # Put some records in the table
        #for i in range(10000):
        #    c[i] = i

        # Set the oldest and stable timestamps to something older than the following updates
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        #stat_cursor = self.session.open_cursor('statistics:', None, None)
        #upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        #stat_cursor.close()

        # Update the records in batches (txns) of 100, increasing the timestamp for each batch
        # This should force old values to get copied to the history store
        test_time_min = 11
        commit_ts = 2
        t = 0
        i = 10
        cpt = 0
        letters = list(string.ascii_lowercase)
        cur_letter = 0
        cur_multplier = 10
        enable_checkpoint = False
        old_time_30_sec = time.time()
        old_time = time.time()
        #while i < 1000000:
        while True:
            self.large_updates(self.uri, letters[cur_letter] * cur_multplier, ds, nrows, commit_ts)
            if(cur_multplier >= 1000):
                cur_multplier = 10
                cur_letter = (cur_letter + 1) % len(letters)
            else:
                cur_multplier = cur_multplier * 10
            i = i + 1
            #self.session.begin_transaction()
            #for j in range (100):
            #    c[i%10000] = i
            #    i += 1
            diff = time.time() - old_time
            diff_30_sec = time.time() - old_time_30_sec
            if(enable_checkpoint and cpt >= 1):
                #ckptcfg = 'use_timestamp=true'
                self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(commit_ts) + ',stable_timestamp=' + timestamp_str(commit_ts))
                self.session.checkpoint()
                self.session.compact(self.uri, None)
                self.prout("Timestamps updated to " + str(commit_ts))
                cpt = 0
            commit_ts += 1

            # Every 30 sec
            if(diff_30_sec >= 30):
                old_time_30_sec = time.time()
                stat_cursor = self.session.open_cursor('statistics:', None, None)
                cache_hs_ondisk = stat_cursor[stat.conn.cache_hs_ondisk][2] / 1024 / 1024
                #cache_state_pages = stat_cursor[stat.dsrc.cache_state_pages][2]
                cc_pages_evict = stat_cursor[stat.conn.cc_pages_evict][2]
                cc_pages_removed = stat_cursor[stat.conn.cc_pages_removed][2]
                cc_pages_visited = stat_cursor[stat.conn.cc_pages_visited][2]
                self.prout("Value of the cache_hs_ondisk at " + str(t) + " is " + str(cache_hs_ondisk))
                #self.prout("Total number of pages currently in cache at " + str(t) + " is " + str(cache_state_pages))
                self.prout("cc_pages_evict at " + str(t) + " is " + str(cc_pages_evict))
                self.prout("cc_pages_removed at " + str(t) + " is " + str(cc_pages_removed))
                self.prout("cc_pages_visited at " + str(t) + " is " + str(cc_pages_visited))

                stat_cursor.close()

            # Every minute
            if(diff > 59):
                old_time = time.time()
                t = t + 1
                if(t % 2 == 0):
                    cpt = 1
                self.prout(str(t) + " minutes have passed")

            # Stop test
            if(t >= test_time_min):
                break
        self.conn.close()