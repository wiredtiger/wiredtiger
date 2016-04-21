#!/usr/bin/env python
#
# Public Domain 2014-2016 MongoDB, Inc.
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
from wtscenario import check_scenarios, multiply_scenarios, number_scenarios

# test_join01.py
#    Join operations
# Basic tests for join
class test_join01(wttest.WiredTigerTestCase):
    nentries = 100

    scenarios = [
        ('table', dict(ref='table')),
        ('index', dict(ref='index'))
    ]
    # We need statistics for these tests.
    conn_config = 'statistics=(all)'

    def gen_key(self, i):
        return [ i + 1 ]

    def gen_values(self, i):
        s = str(i)
        rs = s[::-1]
        sort3 = (self.nentries * (i % 3)) + i    # multiples of 3 sort first
        return [s, rs, sort3]

    # Common function for testing iteration of join cursors
    def iter_common(self, jc, do_proj, do_nested, join_order):
        # See comments in join_common()
        # The order that the results are seen depends on
        # the ordering of the joins.  Specifically, the first
        # join drives the order that results are seen.
        if do_nested:
            if join_order == 0:
                expect = [73, 82, 83, 92]
            elif join_order == 1:
                expect = [73, 82, 83, 92]
            elif join_order == 2:
                expect = [82, 92, 73, 83]
            elif join_order == 3:
                expect = [92, 73, 82, 83]
        else:
            if join_order == 0:
                expect = [73, 82, 62, 83, 92]
            elif join_order == 1:
                expect = [62, 73, 82, 83, 92]
            elif join_order == 2:
                expect = [62, 82, 92, 73, 83]
            elif join_order == 3:
                expect = [73, 82, 62, 83, 92]
        while jc.next() == 0:
            [k] = jc.get_keys()
            i = k - 1
            if do_proj:  # our projection test simply reverses the values
                [v2,v1,v0] = jc.get_values()
            else:
                [v0,v1,v2] = jc.get_values()
            self.assertEquals(self.gen_values(i), [v0,v1,v2])
            if len(expect) == 0 or i != expect[0]:
                self.tty('ERROR: ' + str(i) + ' is not next in: ' +
                         str(expect))
                self.tty('JOIN ORDER=' + str(join_order) + ', NESTED=' + str(do_nested))
            self.assertTrue(i == expect[0])
            expect.remove(i)
        self.assertEquals(0, len(expect))

    # Stats are collected twice: after iterating
    # through the join cursor once, and secondly after resetting
    # the join cursor and iterating again.
    def stats(self, jc, which):
        statcur = self.session.open_cursor('statistics:join', jc, None)
        # pick a stat we always expect to see
        statdesc = 'bloom filter false positives'
        expectstats = [
            'join: index:join01:index1: ' + statdesc,
            'join: index:join01:index2: ' + statdesc ]
        if self.ref == 'index':
            expectstats.append('join: index:join01:index0: ' + statdesc)
        else:
            expectstats.append('join: table:join01: ' + statdesc)
        self.check_stats(statcur, expectstats)
        statcur.reset()
        self.check_stats(statcur, expectstats)
        statcur.close()

    def statstr_to_int(self, str):
        """
        Convert a statistics value string, which may be in either form:
        '12345' or '33M (33604836)'
        """
        parts = str.rpartition('(')
        return int(parts[2].rstrip(')'))

    # All of the expect strings should appear
    def check_stats(self, statcursor, expectstats):
        stringclass = ''.__class__
        intclass = (0).__class__

        # Reset the cursor, we're called multiple times.
        statcursor.reset()

        self.printVerbose(3, 'statistics:')
        for id, desc, valstr, val in statcursor:
            self.assertEqual(type(desc), stringclass)
            self.assertEqual(type(valstr), stringclass)
            self.assertEqual(type(val), intclass)
            self.assertEqual(val, self.statstr_to_int(valstr))
            self.printVerbose(3, '  stat: \'' + desc + '\', \'' +
                              valstr + '\', ' + str(val))
            if desc in expectstats:
                expectstats.remove(desc)

        self.assertTrue(len(expectstats) == 0,
                        'missing expected values in stats: ' + str(expectstats))

    def session_record_join(self, jc, refc, config, order, joins):
        joins.append([order, [jc, refc, config]])

    def session_play_one_join(self, firsturi, jc, refc, config):
        if refc.uri == firsturi and config != None:
            config = config.replace('strategy=bloom','')
        #self.tty('->join(jc, uri="' + refc.uri +
        #         '", config="' + str(config) + '"')
        self.session.join(jc, refc, config)

    def session_play_joins(self, joins, join_order):
        #self.tty('->')
        firsturi = None
        for [i, joinargs] in joins:
            if i >= join_order:
                if firsturi == None:
                    firsturi = joinargs[1].uri
                self.session_play_one_join(firsturi, *joinargs)
        for [i, joinargs] in joins:
            if i < join_order:
                if firsturi == None:
                    firsturi = joinargs[1].uri
                self.session_play_one_join(firsturi, *joinargs)

    # Common function for testing the most basic functionality
    # of joins
    def join_common(self, joincfg0, joincfg1, do_proj, do_nested, do_stats,
                    join_order):
        #self.tty('join_common(' + joincfg0 + ',' + joincfg1 + ',' +
        #         str(do_proj) + ',' + str(do_nested) + ',' +
        #         str(do_stats) + ',' + str(join_order) + ')')
        closeme = []
        joins = []   # cursors to be joined

        self.session.create('table:join01', 'key_format=r' +
                            ',value_format=SSi,columns=(k,v0,v1,v2)')
        self.session.create('index:join01:index0','columns=(v0)')
        self.session.create('index:join01:index1','columns=(v1)')
        self.session.create('index:join01:index2','columns=(v2)')

        c = self.session.open_cursor('table:join01', None, None)
        for i in range(0, self.nentries):
            c.set_key(*self.gen_key(i))
            c.set_value(*self.gen_values(i))
            c.insert()
        c.close()

        if do_proj:
            proj_suffix = '(v2,v1,v0)'  # Reversed values
        else:
            proj_suffix = ''            # Default projection (v0,v1,v2)

        # We join on index2 first, not using bloom indices.
        # This defines the order that items are returned.
        # index2 sorts multiples of 3 first (see gen_values())
        # and by using 'gt' and key 99, we'll skip multiples of 3,
        # and examine primary keys 2,5,8,...,95,98,1,4,7,...,94,97.
        jc = self.session.open_cursor('join:table:join01' + proj_suffix,
                                      None, None)
        # Adding a projection to a reference cursor should be allowed.
        c2 = self.session.open_cursor('index:join01:index2(v1)', None, None)
        c2.set_key(99)   # skips all entries w/ primary key divisible by three
        self.assertEquals(0, c2.search())
        self.session_record_join(jc, c2, 'compare=gt', 0, joins)

        # Then select all the numbers 0-99 whose string representation
        # sort >= '60'.
        if self.ref == 'index':
            c0 = self.session.open_cursor('index:join01:index0', None, None)
            c0.set_key('60')
        else:
            c0 = self.session.open_cursor('table:join01', None, None)
            c0.set_key(60)
        self.assertEquals(0, c0.search())
        self.session_record_join(jc, c0, 'compare=ge' + joincfg0, 1, joins)

        # Then select all numbers whose reverse string representation
        # is in '20' < x < '40'.
        c1a = self.session.open_cursor('index:join01:index1(v1)', None, None)
        c1a.set_key('21')
        self.assertEquals(0, c1a.search())
        self.session_record_join(jc, c1a, 'compare=gt' + joincfg1, 2, joins)

        c1b = self.session.open_cursor('index:join01:index1(v1)', None, None)
        c1b.set_key('41')
        self.assertEquals(0, c1b.search())
        self.session_record_join(jc, c1b, 'compare=lt' + joincfg1, 2, joins)

        # Numbers that satisfy these 3 conditions (with ordering implied by c2):
        #    [73, 82, 62, 83, 92].
        #
        # After iterating, we should be able to reset and iterate again.
        if do_nested:
            # To test nesting, we create two new levels of conditions:
            #
            #     x == 72 or x == 73 or x == 82 or x == 83 or
            #       (x >= 90 and x <= 99)
            #
            # that will get AND-ed into our existing join.  The expected
            # result is   [73, 82, 83, 92].
            #
            # We don't specify the projection here, it should be picked up
            # from the 'enclosing' join.
            nest1 = self.session.open_cursor('join:table:join01', None, None)
            nest2 = self.session.open_cursor('join:table:join01', None, None)

            nc = self.session.open_cursor('index:join01:index0', None, None)
            nc.set_key('90')
            self.assertEquals(0, nc.search())
            self.session.join(nest2, nc, 'compare=ge')  # joincfg left out
            closeme.append(nc)

            nc = self.session.open_cursor('index:join01:index0', None, None)
            nc.set_key('99')
            self.assertEquals(0, nc.search())
            self.session.join(nest2, nc, 'compare=le')
            closeme.append(nc)

            self.session.join(nest1, nest2, "operation=or")

            for val in [ '72', '73', '82', '83' ]:
                nc = self.session.open_cursor('index:join01:index0', None, None)
                nc.set_key(val)
                self.assertEquals(0, nc.search())
                self.session.join(nest1, nc, 'compare=eq,operation=or' +
                                  joincfg0)
                closeme.append(nc)
            self.session_record_join(jc, nest1, None, 3, joins)

        self.session_play_joins(joins, join_order)
        self.iter_common(jc, do_proj, do_nested, join_order)
        if do_stats:
            self.stats(jc, 0)
        jc.reset()
        self.iter_common(jc, do_proj, do_nested, join_order)
        if do_stats:
            self.stats(jc, 1)
        jc.reset()
        self.iter_common(jc, do_proj, do_nested, join_order)
        if do_stats:
            self.stats(jc, 2)
        jc.reset()
        self.iter_common(jc, do_proj, do_nested, join_order)

        jc.close()
        c2.close()
        c1a.close()
        c1b.close()
        c0.close()
        if do_nested:
            nest1.close()
            nest2.close()
            for c in closeme:
                c.close()
        self.session.drop('table:join01')

    # Test joins with basic functionality
    def test_join(self):
        bloomcfg1000 = ',strategy=bloom,count=1000'
        bloomcfg10000 = ',strategy=bloom,count=10000'
        for cfga in [ '', bloomcfg1000, bloomcfg10000 ]:
            for cfgb in [ '', bloomcfg1000, bloomcfg10000 ]:
                for do_proj in [ False, True ]:
                    for do_nested in [ False, True ]:
                        for order in range(0, 4):
                            #self.tty('cfga=' + cfga +
                            #         ', cfgb=' + cfgb +
                            #         ', doproj=' + str(do_proj) +
                            #         ', donested=' + str(do_nested) +
                            #         ', order=' + str(order))
                            self.join_common(cfga, cfgb, do_proj, do_nested,
                                             False, order)

    def test_join_errors(self):
        self.session.create('table:join01', 'key_format=r,value_format=SS'
                            ',columns=(k,v0,v1)')
        self.session.create('table:join01B', 'key_format=r,value_format=SS'
                            ',columns=(k,v0,v1)')
        self.session.create('index:join01:index0','columns=(v0)')
        self.session.create('index:join01:index1','columns=(v1)')
        self.session.create('index:join01B:index0','columns=(v0)')
        jc = self.session.open_cursor('join:table:join01', None, None)
        tc = self.session.open_cursor('table:join01', None, None)
        fc = self.session.open_cursor('file:join01.wt', None, None)
        ic0 = self.session.open_cursor('index:join01:index0', None, None)
        ic0again = self.session.open_cursor('index:join01:index0', None, None)
        ic1 = self.session.open_cursor('index:join01:index1', None, None)
        icB = self.session.open_cursor('index:join01B:index0', None, None)
        tcB = self.session.open_cursor('table:join01B', None, None)

        tc.set_key(1)
        tc.set_value('val1', 'val1')
        tc.insert()
        tcB.set_key(1)
        tcB.set_value('val1', 'val1')
        tcB.insert()
        fc.next()

        # Joining using a non join-cursor
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(tc, ic0, 'compare=ge'),
            '/not a join cursor/')
        # Joining a table cursor, not index
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, fc, 'compare=ge'),
            '/must be an index, table or join cursor/')
        # Joining a non positioned cursor
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0, 'compare=ge'),
            '/requires reference cursor be positioned/')
        ic0.set_key('val1')
        # Joining a non positioned cursor (no search or next has been done)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0, 'compare=ge'),
            '/requires reference cursor be positioned/')
        ic0.set_key('valXX')
        self.assertEqual(ic0.search(), wiredtiger.WT_NOTFOUND)
        # Joining a non positioned cursor after failed search
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0, 'compare=ge'),
            '/requires reference cursor be positioned/')

        # position the cursors now
        ic0.set_key('val1')
        ic0.search()
        ic0again.next()
        icB.next()

        # Joining non matching index
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, icB, 'compare=ge'),
            '/table for join cursor does not match/')

        # The cursor must be positioned
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic1, 'compare=ge'),
            '/requires reference cursor be positioned/')
        ic1.next()

        # This succeeds.
        self.session.join(jc, ic1, 'compare=ge'),

        # With bloom filters, a count is required
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0, 'compare=ge,strategy=bloom'),
            '/count must be nonzero/')

        # This succeeds.
        self.session.join(jc, ic0, 'compare=ge,strategy=bloom,count=1000'),

        bloom_config = ',strategy=bloom,count=1000'
        # Cannot use the same index cursor
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0,
            'compare=le' + bloom_config),
            '/cursor already used in a join/')

        # When joining with the same index, need compatible compares
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0again, 'compare=ge' + bloom_config),
            '/join has overlapping ranges/')

        # Another incompatible compare
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0again, 'compare=gt' + bloom_config),
            '/join has overlapping ranges/')

        # Compare is compatible, but bloom args need to match
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0again, 'compare=le'),
            '/join has incompatible strategy/')

        # Counts need to match for bloom filters
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.join(jc, ic0again, 'compare=le,strategy=bloom,'
            'count=100'), '/count.* does not match previous count/')

        # This succeeds
        self.session.join(jc, ic0again, 'compare=le,strategy=bloom,count=1000')

        # Need to do initial next() before getting key/values
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: jc.get_keys(),
            '/join cursor must be advanced with next/')

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: jc.get_values(),
            '/join cursor must be advanced with next/')

        # Operations on the joined cursor are frozen until the join is closed.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: ic0.next(),
            '/cursor is being used in a join/')

        # Operations on the joined cursor are frozen until the join is closed.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: ic0.prev(),
            '/cursor is being used in a join/')

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: ic0.reset(),
            '/cursor is being used in a join/')

        # Only a small number of operations allowed on a join cursor
        msg = "/Unsupported cursor/"
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: jc.search(), msg)

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: jc.prev(), msg)

        self.assertEquals(jc.next(), 0)
        self.assertEquals(jc.next(), wiredtiger.WT_NOTFOUND)

        # Only after the join cursor is closed can we use the index cursor
        # normally
        jc.close()
        self.assertEquals(ic0.next(), wiredtiger.WT_NOTFOUND)
        self.assertEquals(ic0.prev(), 0)

    # common code for making sure that cursors can be
    # implicitly closed, no matter the order they are created
    def cursor_close_common(self, joinfirst):
        self.session.create('table:join01', 'key_format=r' +
                            ',value_format=SS,columns=(k,v0,v1)')
        self.session.create('index:join01:index0','columns=(v0)')
        self.session.create('index:join01:index1','columns=(v1)')
        c = self.session.open_cursor('table:join01', None, None)
        for i in range(0, self.nentries):
            c.set_key(*self.gen_key(i))
            c.set_value(*self.gen_values(i))
            c.insert()
        c.close()

        if joinfirst:
            jc = self.session.open_cursor('join:table:join01', None, None)
        c0 = self.session.open_cursor('index:join01:index0', None, None)
        c1 = self.session.open_cursor('index:join01:index1', None, None)
        c0.next()        # index cursors must be positioned
        c1.next()
        if not joinfirst:
            jc = self.session.open_cursor('join:table:join01', None, None)
        self.session.join(jc, c0, 'compare=ge')
        self.session.join(jc, c1, 'compare=ge')
        self.session.close()
        self.session = None

    def test_cursor_close1(self):
        self.cursor_close_common(True)

    def test_cursor_close2(self):
        self.cursor_close_common(False)

    # test statistics using the framework set up for this test
    def test_stats(self):
        bloomcfg1000 = ',strategy=bloom,count=1000'
        bloomcfg10 = ',strategy=bloom,count=10'
        self.join_common(bloomcfg1000, bloomcfg1000, False, False, True, 0)

        # Intentially run with an underconfigured Bloom filter,
        # statistics should pick up some false positives.
        self.join_common(bloomcfg10, bloomcfg10, False, False, True, 0)

        # Run stats with a nested join
        self.join_common(bloomcfg1000, bloomcfg1000, False, True, True, 0)
        self.join_common(bloomcfg1000, bloomcfg1000, False, True, True, 3)

    # test statistics with a simple one index join cursor
    def test_simple_stats(self):
        self.session.create("table:join01b",
                       "key_format=i,value_format=i,columns=(k,v)")
        self.session.create("index:join01b:index", "columns=(v)")

        cursor = self.session.open_cursor("table:join01b", None, None)
        cursor[1] = 11
        cursor[2] = 12
        cursor[3] = 13
        cursor.close()

        cursor = self.session.open_cursor("index:join01b:index", None, None)
        cursor.set_key(11)
        cursor.search()

        jcursor = self.session.open_cursor("join:table:join01b", None, None)
        self.session.join(jcursor, cursor, "compare=gt")

        while jcursor.next() == 0:
            [k] = jcursor.get_keys()
            [v] = jcursor.get_values()

        statcur = self.session.open_cursor("statistics:join", jcursor, None)
        found = False
        while statcur.next() == 0:
            [desc, pvalue, value] = statcur.get_values()
            #self.tty(str(desc) + "=" + str(pvalue))
            found = True
        self.assertEquals(found, True)

        jcursor.close()
        cursor.close()


if __name__ == '__main__':
    wttest.run()
