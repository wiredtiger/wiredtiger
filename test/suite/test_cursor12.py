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

import wiredtiger, wttest
from helper import copy_wiredtiger_home
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_cursor12.py
#    Test cursor modify call
class test_cursor12(wttest.WiredTigerTestCase):
    keyfmt = [
        ('recno', dict(keyfmt='r')),
        ('string', dict(keyfmt='S')),
    ]
    types = [
        ('file', dict(uri='file:modify')),
        ('lsm', dict(uri='lsm:modify')),
        ('table', dict(uri='table:modify')),
    ]
    scenarios = make_scenarios(types, keyfmt)

    # List with original value, final value, and modifications to get
    # there.
    list = [
    {
    'o' : 'ABCDEFGH',           # no operation
    'f' : 'ABCDEFGH',
    'mods' : [['', 0, 0]]
    },{
    'o' : 'ABCDEFGH',           # no operation with offset
    'f' : 'ABCDEFGH',
    'mods' : [['', 4, 0]]
    },{
    'o' : 'ABCDEFGH',           # rewrite beginning
    'f' : '--CDEFGH',
    'mods' : [['--', 0, 2]]
    },{
    'o' : 'ABCDEFGH',           # rewrite end
    'f' : 'ABCDEF--',
    'mods' : [['--', 6, 2]]
    },{
    'o' : 'ABCDEFGH',           # append
    'f' : 'ABCDEFGH--',
    'mods' : [['--', 8, 2]]
    },{
    'o' : 'ABCDEFGH',           # append with gap
    'f' : 'ABCDEFGH\00\00--',
    'mods' : [['--', 10, 2]]
    },{
    'o' : 'ABCDEFGH',           # multiple replacements
    'f' : 'A-C-E-G-',
    'mods' : [['-', 1, 1], ['-', 3, 1], ['-', 5, 1], ['-', 7, 1]]
    },{
    'o' : 'ABCDEFGH',           # multiple overlapping replacements
    'f' : 'A-CDEFGH',
    'mods' : [['+', 1, 1], ['+', 1, 1], ['+', 1, 1], ['-', 1, 1]]
    },{
    'o' : 'ABCDEFGH',           # multiple overlapping gap replacements
    'f' : 'ABCDEFGH\00\00--',
    'mods' : [['+', 10, 1], ['+', 10, 1], ['+', 10, 1], ['--', 10, 2]]
    },{
    'o' : 'ABCDEFGH',           # shrink beginning
    'f' : '--EFGH',
    'mods' : [['--', 0, 4]]
    },{
    'o' : 'ABCDEFGH',           # shrink middle
    'f' : 'AB--GH',
    'mods' : [['--', 2, 4]]
    },{
    'o' : 'ABCDEFGH',           # shrink end
    'f' : 'ABCD--',
    'mods' : [['--', 4, 4]]
    },{
    'o' : 'ABCDEFGH',           # grow beginning
    'f' : '--ABCDEFGH',
    'mods' : [['--', 0, 0]]
    },{
    'o' : 'ABCDEFGH',           # grow middle
    'f' : 'ABCD--EFGH',
    'mods' : [['--', 4, 0]]
    },{
    'o' : 'ABCDEFGH',           # grow end
    'f' : 'ABCDEFGH--',
    'mods' : [['--', 8, 0]]
    },{
    'o' : 'ABCDEFGH',           # discard beginning
    'f' : 'EFGH',
    'mods' : [['', 0, 4]]
    },{
    'o' : 'ABCDEFGH',           # discard middle
    'f' : 'ABGH',
    'mods' : [['', 2, 4]]
    },{
    'o' : 'ABCDEFGH',           # discard end
    'f' : 'ABCD',
    'mods' : [['', 4, 4]]
    },{
    'o' : 'ABCDEFGH',           # discard everything
    'f' : '',
    'mods' : [['', 0, 8]]
    },{
    'o' : 'ABCDEFGH',           # overlap the end and append
    'f' : 'ABCDEF--XX',
    'mods' : [['--XX', 6, 2]]
    },{
    'o' : 'ABCDEFGH',           # overlap the end with incorrect size
    'f' : 'ABCDEFG01234567',
    'mods' : [['01234567', 7, 2000]]
    }
    ]

    def skip(self):
        return self.keyfmt == 'r' and 'lsm' in self.uri

    # Smoke-test the modify API.
    def test_modify_smoke(self):
        if self.skip():
            return

        # Populate a database.
        ds = SimpleDataSet(self,
            self.uri, 100, key_format=self.keyfmt, value_format='u')
        ds.populate()

        # For each test in the list:
        #       set the original value,
        #       apply modifications in order,
        #       confirm the final state,
        row = 10
        c = self.session.open_cursor(self.uri, None)
        for i in self.list:
            c.set_key(ds.key(row))
            c.set_value(i['o'])
            self.assertEquals(c.update(), 0)
            c.reset()

            c.set_key(ds.key(row))
            mods = []
            for j in i['mods']:
                mod = wiredtiger.Modify(j[0], j[1], j[2])
                mods.append(mod)
            self.assertEquals(c.modify(mods), 0)
            c.reset()

            c.set_key(ds.key(row))
            self.assertEquals(c.search(), 0)
            self.assertEquals(c.get_value(), i['f'])

            row = row + 1
        c.close()

    # Smoke-test the modify API, closing and re-opening the database.
    def test_modify_smoke_reopen(self):
        if self.skip():
            return

        # Populate a database.
        ds = SimpleDataSet(self,
            self.uri, 100, key_format=self.keyfmt, value_format='u')
        ds.populate()

        # For each test in the list:
        #       set the original value,
        #       apply modifications in order,
        #       confirm the final state,
        row = 10
        c = self.session.open_cursor(self.uri, None)
        for i in self.list:
            c.set_key(ds.key(row))
            c.set_value(i['o'])
            self.assertEquals(c.update(), 0)
            c.reset()

            c.set_key(ds.key(row))
            mods = []
            for j in i['mods']:
                mod = wiredtiger.Modify(j[0], j[1], j[2])
                mods.append(mod)
            self.assertEquals(c.modify(mods), 0)
            c.reset()

            c.set_key(ds.key(row))
            self.assertEquals(c.search(), 0)
            self.assertEquals(c.get_value(), i['f'])

            row = row + 1
        c.close()

        # Flush to disk, forcing reconciliation.
        #
        # XXX KEITH
        # I think this is a Python API problem, for some reason LSM can't
        # reopen the connection if the final is an empty string.
        if 'lsm' not in self.uri or i['f'] != '':
            self.reopen_conn()

        # For each test in the list:
        #       confirm the final state
        row = 10
        c = self.session.open_cursor(self.uri, None)
        for i in self.list:
            c.set_key(ds.key(row))
            self.assertEquals(c.search(), 0)
            self.assertEquals(c.get_value(), i['f'])

            row = row + 1
        c.close()

    # Smoke-test the modify API, recovering the database.
    def test_modify_smoke_recover(self):
        if self.skip():
            return

        # Close the original database.
        self.conn.close()

        # Open a new database with logging configured.
        self.conn_config = \
            'log=(enabled=true),transaction_sync=(method=dsync,enabled)'
        self.conn = self.setUpConnectionOpen(".")
        self.session = self.setUpSessionOpen(self.conn)

        # Populate a database, and checkpoint it so it exists after recovery.
        ds = SimpleDataSet(self,
            self.uri, 100, key_format=self.keyfmt, value_format='u')
        ds.populate()
        self.session.checkpoint()

        # For each test in the list:
        #       set the original value,
        #       apply modifications in order,
        #       confirm the final state,
        #       confirm the final state is there.
        row = 10
        c = self.session.open_cursor(self.uri, None)
        for i in self.list:
            c.set_key(ds.key(row))
            c.set_value(i['o'])
            self.assertEquals(c.update(), 0)
            c.reset()

            c.set_key(ds.key(row))
            mods = []
            for j in i['mods']:
                mod = wiredtiger.Modify(j[0], j[1], j[2])
                mods.append(mod)
            self.assertEquals(c.modify(mods), 0)
            c.reset()

            c.set_key(ds.key(row))
            self.assertEquals(c.search(), 0)
            self.assertEquals(c.get_value(), i['f'])

            row = row + 1
        c.close()

        # Crash and recover in a new directory.
        newdir = 'RESTART'
        copy_wiredtiger_home('.', newdir)
        self.conn.close()
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)
        self.session.verify(self.uri)

        # For each test in the list:
        #       confirm the final state is there.
        row = 10
        c = self.session.open_cursor(self.uri, None)
        for i in self.list:
            c.set_key(ds.key(row))
            self.assertEquals(c.search(), 0)
            self.assertEquals(c.get_value(), i['f'])

            row = row + 1
        c.close()

    # Check that modify returns not-found after a delete.
    def test_modify_delete(self):
        ds = SimpleDataSet(self,
            self.uri, 20, key_format=self.keyfmt, value_format='u')
        ds.populate()

        c = self.session.open_cursor(self.uri, None)
        c.set_key(ds.key(10))
        self.assertEquals(c.remove(), 0)

        mods = []
        mod = wiredtiger.Modify('ABCD', 3, 3)
        mods.append(mod)

        c.set_key(ds.key(10))
        # XXX KEITH
        # I think this is a Python API problem, c.modify should return
        # WT_NOTFOUND, not raising an exception.
        # self.assertEqual(c.modify(mods), wiredtiger.WT_NOTFOUND)
        self.assertRaises(
            wiredtiger.WiredTigerError, lambda:c.modify(mods))

if __name__ == '__main__':
    wttest.run()
