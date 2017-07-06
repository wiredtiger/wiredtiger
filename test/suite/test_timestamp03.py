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
    tablename = 'test_timestamp03'

    types = [
        ('file', dict(uri='file:', use_cg=False, use_index=False)),
        ('lsm', dict(uri='lsm:', use_cg=False, use_index=False)),
        ('table-cg', dict(uri='table:', use_cg=True, use_index=False)),
        ('table-index', dict(uri='table:', use_cg=False, use_index=True)),
        ('table-simple', dict(uri='table:', use_cg=False, use_index=False)),
    ]
    types = [
        ('file', dict(uri='file:', use_cg=False, use_index=False)),
    ]

    ckpt = [
        ('use_ts_def', dict(ckptcfg='', val=False)),
        ('use_ts_false', dict(ckptcfg='use_timestamp=false', val=True)),
        ('use_ts_true', dict(ckptcfg='use_timestamp=true', val=False)),
    ]

    scenarios = make_scenarios(types, ckpt)

    # Binary values.
    value = u'\u0001\u0002abcd\u0003\u0004'
    value2 = u'\u0001\u0002dcba\u0003\u0004'

    conn_config = 'log=(enabled)'

    # Check that a cursor (optionally started in a new transaction), sees the
    # expected values.
    def check(self, session, txn_config, expected):
        if txn_config:
            session.begin_transaction(txn_config)
        c = session.open_cursor(self.uri + self.tablename, None)
        actual = dict((k, v) for k, v in c if v != 0)
        self.assertEqual(actual, expected)
        # Search for the expected items as well as iterating
        for k, v in expected.iteritems():
            self.assertEqual(c[k], v, "for key " + str(k))
        c.close()
        if txn_config:
            session.commit_transaction()

    # Check that a cursor sees the expected values after a checkpoint.
    def ckpt_backup(self):
        newdir = "BACKUP"

        # Take a checkpoint.  Make a copy of the database.  Open the
        # copy and verify whether or not the expected data is in there.
        self.pr("CKPT: " + self.ckptcfg)
        print "CKPT: " + self.ckptcfg
        self.session.checkpoint(self.ckptcfg)
        copy_wiredtiger_home('.', newdir, True)

        conn = self.setUpConnectionOpen(newdir)
        session = self.setUpSessionOpen(conn)
        c = session.open_cursor(self.uri + self.tablename, None)
        # Count how many times the second value is present
        count = 0
        for k, v in c:
            if self.value2 in str(v):
                count += 1
        c.close()
        conn.close()
        self.assertEqual(count != 0, self.val)

    def test_timestamp03(self):
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        uri = self.uri + self.tablename
        self.session.create(uri, 'key_format=i,value_format=S')
        c = self.session.open_cursor(uri)

        # Insert keys 1..100 each with timestamp=key, in some order
        orig_keys = range(1, 101)
        keys = orig_keys[:]
        random.shuffle(keys)

        for k in keys:
            self.session.begin_transaction()
            c[k] = self.value
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(k))

        # Now check that we see the expected state when reading at each
        # timestamp
        for i, t in enumerate(orig_keys):
            self.check(self.session, 'read_timestamp=' + timestamp_str(t),
                dict((k, self.value) for k in orig_keys[:i+1]))

        # Bump the oldest timestamp, we're not going back...
        self.assertEqual(self.conn.query_timestamp(), timestamp_ret_str(100))
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(100))

        # Update them and retry.
        random.shuffle(keys)
        for k in keys:
            self.session.begin_transaction()
            c[k] = self.value2
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(k + 100))

        # Take a checkpoint using the given configuration.  Then verify
        # whether value2 appears in a copy of that data or not.
        self.ckpt_backup()

if __name__ == '__main__':
    wttest.run()
