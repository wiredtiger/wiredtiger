#!/usr/bin/env python
#
# Public Domain 2014-2017 MongoDB, Inc.
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
#
# test_timestamp04.py
#   Timestamps: Test that rollback_to_stable obeys expected visibility rules
#

from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

def timestamp_ret_str(t):
    s = timestamp_str(t)
    if len(s) % 2 == 1:
        s = '0' + s
    return s

class test_timestamp04(wttest.WiredTigerTestCase, suite_subprocess):
    table_ts_log     = 'ts04_ts_logged'
    table_ts_nolog   = 'ts04_ts_nologged'
    table_nots_log   = 'ts04_nots_logged'
    table_nots_nolog = 'ts04_nots_nologged'

    # Below two lists conncfg and types are not removed but kept commented for
    # better understanding of the combinations of scenarios.
    # Different scenarios are manaually provided instead of combinations as
    # the cache_size for lsm minimum requirement is 31MB.
    #conncfg = [
        #('nolog', dict(conn_config='create,cache_size=20MB', using_log=False)),
        #('V1', dict(conn_config='create,cache_size=20MB,log=(enabled),compatibility=(release="2.9")', using_log=True)),
        #('V2', dict(conn_config='create,cache_size=20MB,log=(enabled)', using_log=True)),
    #]

    #types = [
        #('col_fix', dict(empty=1, uri='table:', extra_config=',key_format=r, value_format=8t',is_lsm=False)),
        #('col_var', dict(empty=0, uri='table:', extra_config=',key_format=r',is_lsm=False)),
        #('lsm', dict(empty=0, uri='lsm:', extra_config=',type=lsm',is_lsm=True)),
        #('row', dict(empty=0, uri='table:', extra_config='',is_lsm=False)),
    #]

    scenarios = make_scenarios([
        ('nolog_col_fix', dict(conn_config='create,cache_size=20MB', using_log=False,
            empty=1, uri='table:', extra_config=',key_format=r, value_format=8t',is_lsm=False)),
        ('nolog_col_var', dict(conn_config='create,cache_size=20MB', using_log=False,
            empty=0, uri='table:', extra_config=',key_format=r',is_lsm=False)),
        ('nolog_row', dict(conn_config='create,cache_size=20MB', using_log=False,
            empty=0, uri='table:', extra_config='',is_lsm=False)),
        ('nolog_lsm', dict(conn_config='create,cache_size=31MB', using_log=False,
            empty=0, uri='lsm:', extra_config=',type=lsm',is_lsm=True)),
        ('V1_col_fix', dict(conn_config='create,cache_size=20MB,log=(enabled),compatibility=(release="2.9")', using_log=True,
            empty=1, uri='table:', extra_config=',key_format=r, value_format=8t',is_lsm=False)),
        ('V1_col_var', dict(conn_config='create,cache_size=20MB,log=(enabled),compatibility=(release="2.9")', using_log=True,
            empty=0, uri='table:', extra_config=',key_format=r',is_lsm=False)),
        ('V1_row', dict(conn_config='create,cache_size=20MB,log=(enabled),compatibility=(release="2.9")', using_log=True,
            empty=0, uri='table:', extra_config='',is_lsm=False)),
        ('V1_lsm', dict(conn_config='create,cache_size=31MB,log=(enabled),compatibility=(release="2.9")', using_log=True,
            empty=0, uri='lsm:', extra_config=',type=lsm',is_lsm=True)),
        ('V2_col_fix', dict(conn_config='create,cache_size=20MB,log=(enabled)', using_log=True,
            empty=1, uri='table:', extra_config=',key_format=r, value_format=8t',is_lsm=False)),
        ('V2_col_var', dict(conn_config='create,cache_size=20MB,log=(enabled)', using_log=True,
            empty=0, uri='table:', extra_config=',key_format=r',is_lsm=False)),
        ('V2_row', dict(conn_config='create,cache_size=20MB,log=(enabled)', using_log=True,
            empty=0, uri='table:', extra_config='',is_lsm=False)),
        ('V2_lsm', dict(conn_config='create,cache_size=31MB,log=(enabled)', using_log=True,
            empty=0, uri='lsm:', extra_config=',type=lsm',is_lsm=True)),
    ])

    # Check that a cursor (optionally started in a new transaction), sees the
    # expected values.
    def check(self, session, txn_config, tablename, expected, missing=False, prn=False):
        if txn_config:
            session.begin_transaction(txn_config)
        cur = session.open_cursor(self.uri + tablename, None)
        if missing == False:
            actual = dict((k, v) for k, v in cur if v != 0)
            if prn == True:
                print "CHECK : Expected "
                print expected
                print "CHECK : Actual   "
                print actual
            self.assertTrue(actual == expected)

        # Search for the expected items as well as iterating
        for k, v in expected.iteritems():
            if missing == False:
                self.assertEqual(cur[k], v, "for key " + str(k))
            else:
                cur.set_key(k)
                if self.empty:
                    # Fixed-length column-store rows always exist.
                    self.assertEqual(cur.search(), 0)
                else:
                    self.assertEqual(cur.search(), wiredtiger.WT_NOTFOUND)
        cur.close()
        if txn_config:
            session.commit_transaction()

    def test_rollback_to_stable(self):
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        uri_ts_log      = self.uri + self.table_ts_log
        uri_ts_nolog    = self.uri + self.table_ts_nolog
        uri_nots_log    = self.uri + self.table_nots_log
        uri_nots_nolog  = self.uri + self.table_nots_nolog

        # Configure small page sizes to ensure eviction comes through and we have a
        #  somewhat complex tree
        config_default = 'key_format=i,value_format=i,memory_page_max=32k,leaf_page_max=8k,internal_page_max=8k'
        config_nolog   = ',log=(enabled=false)'
        #
        # Open four tables:
        # 1. Table is logged and uses timestamps.
        # 2. Table is not logged and uses timestamps.
        # 3. Table is logged and does not use timestamps.
        # 4. Table is not logged and does not use timestamps.
        #
        self.session.create(uri_ts_log, config_default + self.extra_config)
        cur_ts_log = self.session.open_cursor(uri_ts_log)
        self.session.create(uri_ts_nolog, config_default + config_nolog + self.extra_config)
        cur_ts_nolog = self.session.open_cursor(uri_ts_nolog)
        self.session.create(uri_nots_log, config_default + self.extra_config)
        cur_nots_log = self.session.open_cursor(uri_nots_log)
        self.session.create(uri_nots_nolog, config_default + config_nolog + self.extra_config)
        cur_nots_nolog = self.session.open_cursor(uri_nots_nolog)

        # Insert keys each with timestamp=key, in some order
        key_range = 10000
        keys = range(1, key_range + 1)

        for k in keys:
            cur_nots_log[k] = 1
            cur_nots_nolog[k] = 1
            self.session.begin_transaction()
            cur_ts_log[k] = 1
            cur_ts_nolog[k] = 1
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(k))
            # Setup an oldest timestamp to ensure state remains in cache.
            if k == 1:
                self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))

        # Scenario: 1
        # Check that we see all the inserted values(i.e 1) in all tables
        latest_ts = timestamp_str(key_range)
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_log, dict((k, 1) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_nolog, dict((k, 1) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_log, dict((k, 1) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, 1) for k in keys[:]))

        #Scenario: 2
        # Roll back half timestamps.
        stable_ts = timestamp_str(key_range / 2)
        self.conn.set_timestamp('stable_timestamp=' + stable_ts)
        self.conn.rollback_to_stable()

        # Check that we see the inserted value (i.e. 1) for all the keys in
        # non-timestamp tables
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_log, dict((k, 1) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_nolog, dict((k, 1) for k in keys[:]))

        # if nologging tables the behavior is consistent across connections
        # with or without log enabled
        # Check that we see the inserted value (i.e. 1) for the keys in a
        # timestamp table till the stable_timestamp only.
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, 1) for k in keys[:(key_range / 2)]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, 1) for k in keys[(key_range / 2 + 1):]), missing=True)

        # behavior of logging tables changes for rollback
        if self.using_log == True:
            # if log is enabled, none of the keys will be rolledback.
            # Check that we see all the keys
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, 1) for k in keys[:]))
        else:
            # if log is disabled, keys will be rolledback till stable_timestamp
            # Check that we see the insertions are rolledback in timestamp tables
            # till the stable_timestamp
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, 1) for k in keys[:(key_range / 2)]))
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, 1) for k in keys[(key_range / 2 + 1):]), missing=True)

        # Bump the oldest timestamp, we're not going back...
        self.conn.set_timestamp('oldest_timestamp=' + stable_ts)

        # Update the values again in preparation for rolling back more
        for k in keys:
            cur_nots_log[k] = 2
            cur_nots_nolog[k] = 2
            self.session.begin_transaction()
            cur_ts_log[k] = 2
            cur_ts_nolog[k] = 2
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(k + key_range))

        # Scenario: 3
        # Check that we see all values updated (i.e 2) in all tables
        latest_ts = timestamp_str(2 * key_range)
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_log, dict((k, 2) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_nolog, dict((k, 2) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_log, dict((k, 2) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, 2) for k in keys[:]))

        #Scenario: 4
        # Advance the stable_timestamp by a quater range and rollback.
        # three-quarter timestamps will be rolledback.
        stable_ts = timestamp_str(1 + key_range + key_range / 4)
        self.conn.set_timestamp('stable_timestamp=' + stable_ts)
        self.conn.rollback_to_stable()

        # Check that we see the updated value (i.e. 2) for all the keys in
        # non-timestamp tables
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_log, dict((k, 2) for k in keys[:]))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_nots_nolog, dict((k, 2) for k in keys[:]))
        # if nologging tables the behavior is consistent across connections
        # with or without log enabled
        # Check that we see only half key ranges in timestamp tables.
        # Check that we see the updated value (i.e. 2) for the first quarter
        # keys in a timestamp table and old values (i.e. 1) for the second
        # quarter keys.
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, (2 if j <= (key_range / 4) else 1))
            for j, k in enumerate(keys[:(key_range / 2)])))
        self.check(self.session, 'read_timestamp=' + latest_ts,
            self.table_ts_nolog, dict((k, 1) for k in keys[(1 + key_range / 2):]), missing=True)

        # behavior of logging tables changes for rollback
        if self.using_log == True:
            # if log is enabled, none of the keys will be rolledback.
            # Check that we see all the keys
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, 2) for k in keys[:]))
        else:
            # if log is disabled, keys will be rolledback till stable_timestamp
            # Check that we see only half key ranges in timestamp tables.
            # Check that we see the updated value (i.e. 2) for the first quarter
            # keys in a timestamp table and old values (i.e. 1) for the second
            # quarter keys.
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, (2 if j <= (key_range / 4) else 1))
                for j, k in enumerate(keys[:(key_range / 2)])))
            self.check(self.session, 'read_timestamp=' + latest_ts,
                self.table_ts_log, dict((k, 1) for k in keys[(1 + key_range / 2):]), missing=True)

if __name__ == '__main__':
    wttest.run()
