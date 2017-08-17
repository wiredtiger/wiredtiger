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
# test_timestamp03.py
#   Timestamps: checkpoints
#

from helper import copy_wiredtiger_home
import random
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

class test_timestamp03(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'ts03_ts_nologged'
    tablename2 = 'ts03_nots_logged'
    tablename3 = 'ts03_ts_logged'
    tablename4 = 'ts03_nots_nologged'

    types = [
        ('file', dict(uri='file:', use_cg=False, use_index=False)),
        ('lsm', dict(uri='lsm:', use_cg=False, use_index=False)),
        ('table-cg', dict(uri='table:', use_cg=True, use_index=False)),
        ('table-index', dict(uri='table:', use_cg=False, use_index=True)),
        ('table-simple', dict(uri='table:', use_cg=False, use_index=False)),
    ]

    ckpt = [
        ('use_ts_def', dict(ckptcfg='', val='none')),
        ('use_ts_false', dict(ckptcfg='use_timestamp=false', val='all')),
        ('use_ts_true', dict(ckptcfg='use_timestamp=true', val='none')),
        ('read_ts', dict(ckptcfg='read_timestamp', val='none')),
    ]

    conncfg = [
        ('nolog', dict(conn_config='create', using_log=False)),
        ('V1', dict(conn_config='create,log=(enabled),compatibility=(release="2.9")', using_log=True)),
        ('V2', dict(conn_config='create,log=(enabled)', using_log=True)),
    ]

    scenarios = make_scenarios(types, ckpt, conncfg)

    # Binary values.
    value = u'\u0001\u0002abcd\u0003\u0004'
    value2 = u'\u0001\u0002dcba\u0003\u0004'
    value3 = u'\u0001\u0002cdef\u0003\u0004'
    value4 = u'\u0001\u0002fedc\u0003\u0004'

    # Check that a cursor (optionally started in a new transaction), sees the
    # expected values.
    def check(self, session, txn_config, tablename, expected):

        if txn_config:
            session.begin_transaction(txn_config)

        c = session.open_cursor(self.uri + tablename, None)
        actual = dict((k, v) for k, v in c if v != 0)
        self.assertEqual(actual, expected)
        # Search for the expected items as well as iterating
        for k, v in expected.iteritems():
            self.assertEqual(c[k], v, "for key " + str(k))
        c.close()
        if txn_config:
            session.commit_transaction()
    #
    # Take a backup of the database and verify that the value we want to
    # check exists in the tables the expected number of times.
    #
    def backup_check(self, check_value, valcnt, valcnt2, valcnt3, valcnt4):
        newdir = "BACKUP"
        copy_wiredtiger_home('.', newdir, True)

        conn = self.setUpConnectionOpen(newdir)
        session = self.setUpSessionOpen(conn)
        c = session.open_cursor(self.uri + self.tablename, None)
        c2 = session.open_cursor(self.uri + self.tablename2, None)
        c3 = session.open_cursor(self.uri + self.tablename3, None)
        c4 = session.open_cursor(self.uri + self.tablename4, None)
        # Count how many times the second value is present
        count = 0
        for k, v in c:
            if check_value in str(v):
                # print "check_value found in key " + str(k)
                count += 1
        c.close()
        # Count how many times the second value is present in the
        # non-timestamp table.
        count2 = 0
        for k, v in c2:
            if check_value in str(v):
                # print "check_value found in key " + str(k)
                count2 += 1
        c2.close()
        # Count how many times the second value is present in the
        # logged timestamp table.
        count3 = 0
        for k, v in c3:
            if check_value in str(v):
                count3 += 1
        c3.close()
        # Count how many times the second value is present in the
        # non-timestamp table.
        count4 = 0
        for k, v in c4:
            if check_value in str(v):
                count4 += 1
        c4.close()
        conn.close()
        # print "CHECK BACKUP: Count " + str(count) + " Count2 " + str(count2) + " Count3 " + str(count3) + "Count4 " + str(count4)
        # print "CHECK BACKUP: Expect value2 count " + str(valcnt)
        # print "CHECK BACKUP: 2nd table Expect value2 count " + str(valcnt2)
        # print "CHECK BACKUP: 3rd table Expect value2 count " + str(valcnt3)
        # print "CHECK BACKUP: 4th table Expect value2 count " + str(valcnt4)
        # print "CHECK BACKUP: config " + str(self.ckptcfg)
        self.assertEqual(count, valcnt)
        self.assertEqual(count2, valcnt2)
        self.assertEqual(count3, valcnt3)
        self.assertEqual(count4, valcnt4)

    # Check that a cursor sees the expected values after a checkpoint.
    def ckpt_backup(self, check_value, valcnt, valcnt2, valcnt3, valcnt4, ckptts):

        # Take a checkpoint.  Make a copy of the database.  Open the
        # copy and verify whether or not the expected data is in there.
        self.pr("CKPT: " + self.ckptcfg)
        ckptcfg = self.ckptcfg
        if not ckptts:
            if ckptcfg == 'read_timestamp':
                ckptcfg = self.ckptcfg + '=' + self.oldts
        else:
            ckptcfg = ckptts
        # print "CKPT: " + ckptcfg

        self.session.checkpoint(ckptcfg)
        self.backup_check(check_value, valcnt, valcnt2, valcnt3, valcnt4)

    def test_timestamp03(self):
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        uri = self.uri + self.tablename
        uri2 = self.uri + self.tablename2
        uri3 = self.uri + self.tablename3
        uri4 = self.uri + self.tablename4
        #
        # Open four tables:
        # 1. Table is not logged and uses timestamps.
        # 2. Table is logged and does not use timestamps.
        # 3. Table is logged and uses timestamps.
        # 4. Table is not logged and does not use timestamps.
        #
        self.session.create(uri, 'key_format=i,value_format=S,log=(enabled=false)')
        c = self.session.open_cursor(uri)
        self.session.create(uri2, 'key_format=i,value_format=S')
        c2 = self.session.open_cursor(uri2)
        self.session.create(uri3, 'key_format=i,value_format=S')
        c3 = self.session.open_cursor(uri3)
        self.session.create(uri4, 'key_format=i,value_format=S, log=(enabled=false)')
        c4 = self.session.open_cursor(uri4)

        # Insert keys 1..100 each with timestamp=key, in some order
        nkeys = 100
        orig_keys = range(1, nkeys+1)
        keys = orig_keys[:]
        random.shuffle(keys)

        for k in keys:
            c2[k] = self.value
            c4[k] = self.value
            self.session.begin_transaction()
            c[k] = self.value
            c3[k] = self.value
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(k))

        # Scenario : 1
        # Now check that we see the expected when reading with out
        # read_timestamp as per transaction visibility
        self.check(self.session, "", self.tablename,
            dict((k, self.value) for k in orig_keys))
        self.check(self.session, "", self.tablename2,
            dict((k, self.value) for k in orig_keys))
        self.check(self.session, "", self.tablename3,
            dict((k, self.value) for k in orig_keys))
        self.check(self.session, "", self.tablename4,
            dict((k, self.value) for k in orig_keys))

        # Scenario : 2
        # Now check that we see the expected state when reading at each
        # timestamp
        for i, t in enumerate(orig_keys):
            self.check(self.session, 'read_timestamp=' + timestamp_str(t),
                self.tablename, dict((k, self.value) for k in orig_keys[:i+1]))
            self.check(self.session, 'read_timestamp=' + timestamp_str(t),
                self.tablename2, dict((k, self.value) for k in orig_keys))
            self.check(self.session, 'read_timestamp=' + timestamp_str(t),
                self.tablename3, dict((k, self.value) for k in orig_keys[:i+1]))
            self.check(self.session, 'read_timestamp=' + timestamp_str(t),
                self.tablename4, dict((k, self.value) for k in orig_keys))

        # Bump the oldest timestamp, we're not going back...
        self.assertEqual(self.conn.query_timestamp(), timestamp_ret_str(100))
        self.oldts = timestamp_str(100)
        self.conn.set_timestamp('oldest_timestamp=' + self.oldts)
        self.conn.set_timestamp('stable_timestamp=' + self.oldts)
        # print "Oldest " + self.oldts

        # Scenario : 3
        # Now check that we see all the data after moving the oldest timestamp
        # to current timestamp
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename, dict((k, self.value) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename2, dict((k, self.value) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename3, dict((k, self.value) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename4, dict((k, self.value) for k in orig_keys))

        # Update them and retry.
        random.shuffle(keys)
        count = 0
        for k in keys:
            # Make sure a timestamp cursor is the last one to update.  This
            # tests the scenario for a bug we found where recovery replayed
            # the last record written into the log.
            #
            # print "Key " + str(k) + " to value2"
            c2[k] = self.value2
            c4[k] = self.value2
            self.session.begin_transaction()
            c[k] = self.value2
            c3[k] = self.value2
            ts = timestamp_str(k + 100)
            self.session.commit_transaction('commit_timestamp=' + ts)
            # print "Commit key " + str(k) + " ts " + ts
            count += 1
        # print "Updated " + str(count) + " keys to value2"

        # Scenario : 4
        # Now check that we don't see the updated data of timestamp tables
        # with old read_timestamp
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename, dict((k, self.value) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename2, dict((k, self.value2) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename3, dict((k, self.value) for k in orig_keys))
        self.check(self.session, 'read_timestamp=' + timestamp_str(100),
            self.tablename4, dict((k, self.value2) for k in orig_keys))

        # Scenario : 5
        # Now check that we see the expected state when reading at each
        # updated timestamp
        valdict = dict((k, self.value) for k in orig_keys)
        for i, t in enumerate(orig_keys):
            valdict[i+1] = self.value2
            self.check(self.session, 'read_timestamp=' + timestamp_str(t + 100),
                self.tablename, valdict)
            self.check(self.session, 'read_timestamp=' + timestamp_str(t + 100),
                self.tablename2, dict((k, self.value2) for k in orig_keys))
            self.check(self.session, 'read_timestamp=' + timestamp_str(t + 100),
                self.tablename3, valdict)
            self.check(self.session, 'read_timestamp=' + timestamp_str(t + 100),
                self.tablename4, dict((k, self.value2) for k in orig_keys))

        # Take a checkpoint using the given configuration.  Then verify
        # whether value2 appears in a copy of that data or not.
        valcnt2 = valcnt3 = valcnt4 = nkeys
        if self.val == 'all':
            valcnt = nkeys
        else:
            valcnt = 0
        self.ckpt_backup(self.value2, valcnt, valcnt2, valcnt3, valcnt4, "")
        if self.ckptcfg != 'read_timestamp':
            # Update the stable timestamp to the latest, but not the oldest
            # timestamp and make sure we can see the data.  Once the stable
            # timestamp is moved we should see all keys with value2.
            self.conn.set_timestamp('stable_timestamp=' + \
                timestamp_str(100+nkeys))
            self.ckpt_backup(self.value2, nkeys, nkeys, nkeys, nkeys, "")

        # Update the keys and checkpoing using stable_timestamp, not using
        # read_timestamp.
        random.shuffle(keys)
        count = 0
        for k in keys:
            # Make sure a timestamp cursor is the last one to update.  This
            # tests the scenario for a bug we found where recovery replayed
            # the last record written into the log.
            #
            # print "Key " + str(k) + " to value3"
            c2[k] = self.value3
            c4[k] = self.value3
            self.session.begin_transaction()
            c[k] = self.value3
            c3[k] = self.value3
            ts = timestamp_str(k + 200)
            self.session.commit_transaction('commit_timestamp=' + ts)
            # print "Commit key " + str(k) + " ts " + ts
            count += 1
        # print "Updated " + str(count) + " keys to value3"

        # Scenario : 6
        # when read time stamp != stable timestamp, take checkpoints using both
        # stable timestamp and read timestamp and check to see the values differ
        # and are proper.
        if self.ckptcfg == 'use_timestamp=true':
            # make sure stable_timestamp is set.
            ckpt_ts = 'stable_timestamp=' + timestamp_str(100 + nkeys)
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(100 + nkeys))
            valcnt2 = valcnt3 = valcnt4 = nkeys
            valcnt = 0
            self.ckpt_backup(self.value3, valcnt, valcnt2, valcnt3, valcnt4, "")
            # checkpoint at latest timestamp with read_timestamp
            ckpt_ts = 'read_timestamp=' + timestamp_str(200 + nkeys)
            valcnt = nkeys
            self.ckpt_backup(self.value3, valcnt, valcnt2, valcnt3, valcnt4, ckpt_ts)

        # If we're not using the log we're done.
        if not self.using_log:
            return

        # Update them and retry.  This time take a backup and recover.
        random.shuffle(keys)
        count = 0
        for k in keys:
            # Make sure a timestamp cursor is the last one to update.  This
            # tests the scenario for a bug we found where recovery replayed
            # the last record written into the log.
            #
            # print "Key " + str(k) + " to value3"
            c2[k] = self.value4
            c4[k] = self.value4
            self.session.begin_transaction()
            c[k] = self.value4
            c3[k] = self.value4
            ts = timestamp_str(k + 300)
            self.session.commit_transaction('commit_timestamp=' + ts)
            # print "Commit key " + str(k) + " ts " + ts
            count += 1
        # print "Updated " + str(count) + " keys to value4"

        # Flush the log but don't checkpoint
        self.session.log_flush('sync=on')

        # Take a backup and then verify whether value3 appears in a copy
        # of that data or not.  Both tables that are logged should see
        # all the data regardless of timestamps.  The table that is not
        # logged should not see any of it.
        valcnt = valcnt4 = 0
        valcnt2 = valcnt3 = nkeys
        self.backup_check(self.value4, valcnt, valcnt2, valcnt3, valcnt4)

if __name__ == '__main__':
    wttest.run()
