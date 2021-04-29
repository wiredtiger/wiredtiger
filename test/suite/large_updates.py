#!/usr/bin/env python

import wiredtiger, wttest, time, string, os
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

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value)
            count += 1
        cursor.close()
        session.rollback_transaction()
        self.assertEqual(count, nrows)

    def evict(self, uri, ds, nrows):
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        for i in range(1, nrows + 1):
            evict_cursor.set_key(ds.key(i))
            self.assertEqual(evict_cursor.search(), 0)
            self.assertEqual(evict_cursor.reset(), 0)
        evict_cursor.close()

    def test_it(self):
        nrows = 100
        self.uri = 'table:test'
        commit_ts = 1
        t = 0
        cpt = 0
        #letters = list(string.ascii_lowercase)
        letters = list(string.printable)
        cur_letter = 0
        old_time_30_sec = time.time()
        old_time = time.time()
        number_of_updates = 0
        total_nb_updates = nrows * 1000000

        # Create and populate
        ds = SimpleDataSet(self, self.uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds.populate()
        bigvalue = "." * 100
        current_value = bigvalue
        self.large_updates(self.uri, bigvalue, ds, nrows, commit_ts)
        commit_ts += 1
        offset = 0

        # Set the oldest and stable timestamps to something older than the following updates
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        while True:

            # Perform updates
            self.large_updates(self.uri, current_value, ds, nrows, commit_ts)
            number_of_updates += nrows
            os.write(wttest.WiredTigerTestCase._dupout, str.encode('%d/%d\r' % (number_of_updates,total_nb_updates)))
            #self.prout(current_value)
            if offset < 100:
                current_value = current_value[0:offset] + letters[cur_letter] + current_value[(offset + 1):]
                offset += 1
            else:
                offset = 0
                cur_letter = (cur_letter + 1) % len(letters)

            diff = time.time() - old_time
            diff_30_sec = time.time() - old_time_30_sec

            commit_ts += 1

            # Every 30 sec
            if(diff_30_sec >= 30):
                old_time_30_sec = time.time()
                stat_cursor = self.session.open_cursor('statistics:', None, None)
                cache_hs_ondisk = stat_cursor[stat.conn.cache_hs_ondisk][2] / 1024 / 1024
                #cache_state_pages = stat_cursor[stat.dsrc.cache_state_pages][2]
                cache_eviction_force = stat_cursor[stat.conn.cache_eviction_force][2]
                cc_pages_evict = stat_cursor[stat.conn.cc_pages_evict][2]
                cc_pages_removed = stat_cursor[stat.conn.cc_pages_removed][2]
                cc_pages_visited = stat_cursor[stat.conn.cc_pages_visited][2]
                self.prout("Value of the cache_hs_ondisk at " + str(t) + " is " + str(cache_hs_ondisk))
                #self.prout("Total number of pages currently in cache at " + str(t) + " is " + str(cache_state_pages))
                self.prout("cache_eviction_force at " + str(t) + " is " + str(cache_eviction_force))
                self.prout("cc_pages_evict at " + str(t) + " is " + str(cc_pages_evict))
                self.prout("cc_pages_removed at " + str(t) + " is " + str(cc_pages_removed))
                self.prout("cc_pages_visited at " + str(t) + " is " + str(cc_pages_visited))
                stat_cursor.close()
                self.session.checkpoint()

            # Every minute
            if(diff > 59):
                old_time = time.time()
                t = t + 1
                if(t % 2 == 0):
                    cpt = 1
                self.prout(str(t) + " minutes have passed")

            # Stop test
            if(number_of_updates >= total_nb_updates):
                break

        self.prout("Eviction...")
        self.evict(self.uri, ds, nrows)
        # Check all values are correct
        cur_letter = 0
        offset = 0
        current_value = bigvalue
        self.prout("Checking values...")
        for i in range(2,commit_ts):
            os.write(wttest.WiredTigerTestCase._dupout, str.encode('%d/%d\r' % (i,commit_ts)))
            #self.prout(str(i) + "checked out of " + str(commit_ts), end='\r', flush=True)
            self.check(current_value, self.uri, nrows, i)
            if offset < 100:
                current_value = current_value[0:offset] + letters[cur_letter] + current_value[(offset + 1):]
                offset += 1
            else:
                offset = 0
                cur_letter = (cur_letter + 1) % len(letters)

        self.conn.close()