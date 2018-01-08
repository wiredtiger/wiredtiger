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
from wiredtiger import stat
from wtscenario import make_scenarios
import test_cursor01, test_cursor02, test_cursor03
import test_checkpoint01, test_checkpoint02
from wtdataset import SimpleDataSet, ComplexDataSet, ComplexLSMDataSet
from helper import confirm_does_not_exist

# Cursor caching tests
class test_cursor13_base(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(fast)'
    stat_cursor_cache = 0
    stat_cursor_reopen = 0

    def setUpSessionOpen(self, conn):
        return conn.open_session('cache_cursors=true')

    def caching_stats(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        cache = stat_cursor[stat.conn.cursor_cache][2]
        reopen = stat_cursor[stat.conn.cursor_reopen][2]
        stat_cursor.close()
        return [cache, reopen]

    def assert_cursor_cached(self):
        stats = self.caching_stats()
        self.assertGreater(stats[0], self.stat_cursor_cache)
        self.stat_cursor_cache = stats[0]

    def assert_cursor_reopened(self):
        stats = self.caching_stats()
        self.assertGreater(stats[1], self.stat_cursor_reopen)
        self.stat_cursor_reopen = stats[1]

    def assert_cursor_reopened_same(self):
        stats = self.caching_stats()
        self.assertEqual(stats[1], self.stat_cursor_reopen)

    def cursor_stats_init(self):
        stats = self.caching_stats()
        self.stat_cursor_cache = stats[0]
        self.stat_cursor_reopen = stats[1]

# Override other cursor tests with cursors cached.
class test_cursor13_01(test_cursor01.test_cursor01, test_cursor13_base):
    pass

class test_cursor13_02(test_cursor02.test_cursor02, test_cursor13_base):
    pass

class test_cursor13_03(test_cursor03.test_cursor03, test_cursor13_base):
    pass

class test_cursor13_ckpt01(test_checkpoint01.test_checkpoint,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt02(test_checkpoint01.test_checkpoint_cursor,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt03(test_checkpoint01.test_checkpoint_target,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt04(test_checkpoint01.test_checkpoint_cursor_update,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt05(test_checkpoint01.test_checkpoint_last,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt06(test_checkpoint01.test_checkpoint_empty,
                           test_cursor13_base):
    pass

class test_cursor13_ckpt2(test_checkpoint02.test_checkpoint02,
                          test_cursor13_base):
    pass

class test_cursor13_reopens(test_cursor13_base):
    scenarios = make_scenarios([
        ('file', dict(uri='file:cursor13_reopen1', dstype=None)),
        ('table', dict(uri='table:cursor13_reopen2', dstype=None)),
        ('sfile', dict(uri='file:cursor13_reopen3', dstype=SimpleDataSet)),
        ('stable', dict(uri='table:cursor13_reopen4', dstype=SimpleDataSet)),
        ('ctable', dict(uri='table:cursor13_reopen5', dstype=ComplexDataSet)),
        ('clsm', dict(uri='table:cursor13_reopen6', dstype=ComplexLSMDataSet))
    ])

    def basic_populate(self, uri):
        cursor = self.session.open_cursor(uri)
        cursor['A'] = 'B'
        cursor.close()
        self.assert_cursor_cached()
        cursor = self.session.open_cursor(uri)
        self.assert_cursor_reopened()
        cursor['B'] = 'C'
        cursor.close()
        self.assert_cursor_cached()

    def basic_check(self, cursor):
        count = 0
        for x,y in cursor:
            if count == 0:
                self.assertEqual('A', x)
                self.assertEqual('B', y)
            elif count == 1:
                self.assertEqual('B', x)
                self.assertEqual('C', y)
            count += 1
        self.assertEqual(count, 2)

    def basic_reopen(self):
        session = self.session
        session.create(self.uri, 'key_format=S,value_format=S')
        self.basic_populate(self.uri)

        # Reopen/close 10 times, with multiple cursors
        for opens in range(0, 10):
            c = session.open_cursor(self.uri)
            self.assert_cursor_reopened()
            c2 = session.open_cursor(self.uri)
            if opens == 0:
                self.assert_cursor_reopened_same()
            else:
                self.assert_cursor_reopened()

            self.basic_check(c)
            self.basic_check(c2)
            c.close()
            self.assert_cursor_cached()
            c2.close()
            self.assert_cursor_cached()

    def dataset_reopen(self):
        ds = self.dstype(self, self.uri, 100)
        ds.populate()
        self.assert_cursor_cached()
        ds.check()
        self.assert_cursor_reopened()

    def test_reopen(self):
        self.cursor_stats_init()
        if self.dstype == None:
            self.basic_reopen()
        else:
            self.dataset_reopen()

class test_cursor13_drops(test_cursor13_base):
    def open_and_drop(self, uri, cursor_session, drop_session, nopens, ntrials):
        for i in range(0, ntrials):
            cursor_session.create(uri, 'key_format=S,value_format=S')
            for i in range(0, nopens):
                c = cursor_session.open_cursor(uri)
                c.close()
            drop_session.drop(uri)
            confirm_does_not_exist(self, uri)

    def test_open_and_drop(self):
        session = self.session
        for uri in [ 'file:test_cursor13_drops', 'table:test_cursor13_drops' ]:
            self.open_and_drop(uri, session, session, 0, 5)
            self.open_and_drop(uri, session, session, 1, 5)
            self.open_and_drop(uri, session, session, 3, 5)

            # It should still work with different sessions
            session2 = self.conn.open_session(None)
            self.open_and_drop(uri, session2, session, 0, 5)
            self.open_and_drop(uri, session2, session, 1, 5)
            self.open_and_drop(uri, session2, session, 3, 5)
            session2.close()

    def test_open_index_and_drop(self):
        # We should also be able to detect cached cursors
        # for indices
        session = self.session
        uri = 'table:test_cursor13_drops'
        ds = ComplexDataSet(self, uri, 100)
        ds.create()
        indexname = ds.index_name(0)
        c = session.open_cursor(indexname)
        # The index is really open, so we cannot drop the main table.
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: session.drop(uri))
        c.close()
        session.drop(uri)
        confirm_does_not_exist(self, uri)

        # Same test for indices, but with cursor held by another session.
        # TODO: try with session that DOES have cache_cursors and another
        # that does not.
        session2 = self.conn.open_session(None)
        ds = ComplexDataSet(self, uri, 100)
        ds.create()
        indexname = ds.index_name(0)
        c = session2.open_cursor(indexname)
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: session.drop(uri))
        c.close()
        session.drop(uri)
        confirm_does_not_exist(self, uri)
        session2.close()

    def test_cursor_drops(self):
        session = self.session
        uri = 'table:test_cursor13_drops'
        idxuri = 'index:test_cursor13_drops:index1'
        config = 'key_format=S,value_format=S,columns=(k,v1)'

        for i in range(0, 2):
            session.create(uri, config)
            session.drop(uri)

        for i in range(0, 2):
            session.create(uri, config)
            cursor = session.open_cursor(uri, None)
            cursor['A'] = 'B'
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: session.drop(uri))
            cursor.close()
            session.drop(uri)

        for i in range(0, 2):
            session.create(uri, config)
            session.create(idxuri, 'columns=(v1)')
            cursor = session.open_cursor(uri, None)
            cursor['A'] = 'B'
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: session.drop(uri))
            cursor.close()
            session.drop(uri)

        for i in range(0, 2):
            session.create(uri, config)
            session.create(idxuri, 'columns=(v1)')
            cursor = session.open_cursor(uri, None)
            cursor['A'] = 'B'
            cursor.close()
            cursor = session.open_cursor(idxuri, None)
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: session.drop(uri))
            cursor.close()
            session.drop(uri)
