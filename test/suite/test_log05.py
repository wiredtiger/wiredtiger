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

import os, wiredtiger, wttest
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet

def mod_val(value, char, location, nbytes=1):
    return value[0:location] + char + value[location+nbytes:]

# test_log05.py
#    Test all log operations with new packing logic and print them using old unpack logic.
class test_log05(wttest.WiredTigerTestCase, suite_subprocess):
    types = [
        ('col', dict(key_format='r',value_format='S')),
        ('fix', dict(key_format='r',value_format='8t')),
        ('row', dict(key_format='S',value_format='S')),
    ]
    log = [
        ('log', dict(log=True, extraconfig='log=(enabled=true)')),
        ('no-log', dict(log=False, extraconfig='log=(enabled=false)')),
    ]

    scenarios = make_scenarios(types, log)

    def conn_config(self):
        config = 'log=(enabled)'
        if not self.log:
            config += ',debug_mode=(table_logging=true)'
        return config

    def check(self, cursor, read_ts, key, value):
        if read_ts == 0:
            self.session.begin_transaction()
        else:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        if value == None:
            if self.value_format == '8t':
                self.assertEqual(cursor[key], 0)
            else:
                cursor.set_key(key)
                self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor[key], value)
        self.session.rollback_transaction()

    def test_logts(self):
        uri = 'table:test_log05'
        ds= SimpleDataSet(self, uri, 100,
            key_format=self.key_format, value_format=self.value_format,
            config=self.extraconfig)
        ds.populate()
        c = self.session.open_cursor(uri)

        # Set oldest and stable timestamps to 10.
        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))

        key = ds.key(10)
        value10 = ds.value(10)

        # Confirm initial data.
        self.check(c, 0 if self.log else 10, key, value10)

        # Truncate a range of data.
        self.session.begin_transaction()
        start = ds.open_cursor(uri, None)
        start.set_key(ds.key(10))
        end = ds.open_cursor(uri, None)
        end.set_key(ds.key(90))
        self.assertEqual(self.session.truncate(None, start, end, None), 0)
        if self.log:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        start.close()
        end.close()
        self.check(c, 0 if self.log else 20, key, None)

        # Insert, Update, modify and then remove.
        key = ds.key(110)
        if self.value_format == '8t':
            value_a = 97 # 'a'
            value_b = 98 # 'b'
        else:
            value_a = "aaaa"
            value_b = "bbbb"

        self.session.begin_transaction()
        c.set_key(key)
        c.set_value(value_a)
        self.assertEqual(c.insert(), 0)
        if self.log:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        self.check(c, 0 if self.log else 20, key, value_a)

        self.session.begin_transaction()
        c.set_key(key)
        c.set_value(value_b)
        self.assertEqual(c.update(), 0)
        c[key] = value_b
        if self.log:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        self.check(c, 0 if self.log else 30, key, value_b)

        if not self.value_format == '8t':
            value_modQ = mod_val(value_b, 'Q', 0)
            self.session.begin_transaction()
            c.set_key(key)
            mods = [wiredtiger.Modify('Q', 0, 1)]
            self.assertEqual(c.modify(mods), 0)
            if self.log:
                self.session.commit_transaction()
            else:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(40))
            self.check(c, 0 if self.log else 40, key, value_modQ)

        self.session.begin_transaction()
        c.set_key(key)
        self.assertEqual(c.remove(), 0)
        if self.log:
            self.session.commit_transaction()
        else:
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))
        self.check(c, 0 if self.log else 50, key, None)

        # Move the stable timestamp to 50. Checkpoint.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.session.checkpoint()

        #
        # Run printlog and make sure it exits with zero status.
        #
        self.runWt(['-h', '.', 'printlog'], outfilename='printlog.out')

if __name__ == '__main__':
    wttest.run()
