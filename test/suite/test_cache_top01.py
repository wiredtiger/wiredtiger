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

import re, time
import wiredtiger
import wttest

# The rankings name the data handle behind a table, and which handle that is depends on the running
# configuration: a tiered table is named by its tiered URI, and a layered table has separate ingest
# and stable constituents. Accept any of the handles a table can legitimately be reported under,
# including whichever backing file the running hook says a table starts life in.
def table_names(testcase, base):
    names = ['file:%s.wt' % base, 'file:%s.wt_stable' % base, 'file:%s.wt_ingest' % base,
        'tiered:%s' % base]
    initial = testcase.initialFileName('table:' + base)
    if initial is not None:
        names.append('file:' + initial)
    return names

# The rankings of the tables consuming the most cache, as reported by
# WT_CONNECTION::debug_info and by the cache_top verbose category.
class test_cache_top01(wttest.WiredTigerTestCase):
    # Eviction is deliberately given nothing to do: a table's resident bytes are only a stable thing
    # to assert on when eviction is not free to take them away underneath the test.
    conn_config = ('create,cache_size=100MB,statistics=(all),'
        'eviction_dirty_target=60,eviction_dirty_trigger=80,'
        'eviction_updates_target=50,eviction_updates_trigger=70')

    # The number of slots each ranking has, which bounds the length of a report.
    slots = 32

    # Every ranking the report is expected to produce.
    rankings = ['update bytes', 'dirty leaf bytes', 'total cache bytes',
        'recent bytes read', 'recent bytes evicted']

    # Rankings of a level, which can also report a connection-wide total. The rest track a decayed
    # flow, which has no connection-wide equivalent.
    level_rankings = ['update bytes', 'dirty leaf bytes', 'total cache bytes']

    value = 'v' * 4096

    header_re = re.compile(r'cache top (?P<ranking>.+?): (?P<count>\d+) tables above '
        r'(?P<threshold>\d+)B hold (?P<listed>\d+)B(?: of (?P<total>\d+)B)?$')
    entry_re = re.compile(r'^\s+(?P<value>\d+)B (?P<name>\S+)$')

    def populate(self, uri, rows, start = 0):
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        for i in range(start, start + rows):
            c['k%08d' % i] = self.value
        c.close()

    # Populate and checkpoint, so the pages are clean. Dirty eviction runs against a target that is
    # a fraction of the cache, so a table left dirty can lose its resident bytes at any moment;
    # clean pages are only evicted under cache pressure, which these tests stay well clear of.
    def populate_clean(self, uri, rows):
        self.populate(uri, rows)
        self.session.checkpoint()

    def read_all(self, uri):
        c = self.session.open_cursor(uri)
        for _ in c:
            pass
        c.close()

    # Ask for a report and parse it into {ranking: {threshold, listed, total, entries}}, where
    # entries is a list of (bytes, name) in the order reported.
    def report(self):
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        # The report is what this test is here to look at, not unexpected output.
        self.cleanStdout()

        report = {}
        ranking = None
        for line in out.splitlines():
            header = self.header_re.search(line)
            if header is not None:
                ranking = header.group('ranking')
                report[ranking] = {
                    'count': int(header.group('count')),
                    'threshold': int(header.group('threshold')),
                    'listed': int(header.group('listed')),
                    'total': None if header.group('total') is None
                        else int(header.group('total')),
                    'entries': [],
                }
                continue
            entry = self.entry_re.match(line)
            if entry is not None:
                self.assertIsNotNone(ranking, 'entry line before any ranking: ' + line)
                report[ranking]['entries'].append(
                    (int(entry.group('value')), entry.group('name')))
        return report

    def names(self, report, ranking):
        return [name for _, name in report[ranking]['entries']]

    def assertTableIn(self, base, names):
        self.assertTrue(any(n in names for n in table_names(self, base)),
            '%s not found in %s' % (base, names))

    def assertTableNotIn(self, base, names):
        for n in table_names(self, base):
            self.assertNotIn(n, names)

    # Every report is expected to hold together internally, whatever the workload.
    def check_report_consistent(self, report):
        for ranking in self.rankings:
            self.assertIn(ranking, report, 'ranking missing from the report: ' + ranking)
            r = report[ranking]

            # The count on the header line is the number of entries that follow.
            self.assertEqual(r['count'], len(r['entries']))

            # A report can never be longer than the ranking has room for.
            self.assertLessEqual(r['count'], self.slots)

            # The listed bytes are the sum of the entries.
            self.assertEqual(r['listed'], sum(value for value, _ in r['entries']))

            # Entries are ordered largest first. Which named table leads is deliberately not
            # asserted anywhere: how many bytes a table has resident at any moment depends on when
            # eviction last ran, so only the ordering of the reported values is a guarantee.
            values = [value for value, _ in r['entries']]
            self.assertEqual(values, sorted(values, reverse = True))

            # Every entry is at or above the threshold, and every name is a data handle.
            for value, name in r['entries']:
                self.assertGreaterEqual(value, r['threshold'])
                self.assertTrue(name.startswith('file:') or name.startswith('tiered:'),
                    'unexpected name: ' + name)

            # A ranking of a level can compare itself against the connection, a flow cannot.
            if ranking in self.level_rankings:
                self.assertIsNotNone(r['total'])
                self.assertGreaterEqual(r['total'], 0)
            else:
                self.assertIsNone(r['total'])

    # A report against an untouched connection produces every ranking, all empty.
    def test_report_empty(self):
        report = self.report()
        self.check_report_consistent(report)
        for ranking in self.rankings:
            self.assertEqual(report[ranking]['count'], 0)
            self.assertEqual(report[ranking]['listed'], 0)

    # A table large enough to matter is named; one holding almost nothing is not.
    def test_large_table_reported_small_ignored(self):
        self.populate_clean('table:big', 2500)
        self.populate_clean('table:small', 10)

        report = self.report()
        self.check_report_consistent(report)

        resident = self.names(report, 'total cache bytes')
        self.assertTableIn('big', resident)
        self.assertTableNotIn('small', resident)

    # Two tables both large enough to rank are both reported.
    def test_two_large_tables(self):
        self.populate_clean('table:first', 2500)
        self.populate_clean('table:second', 2500)

        report = self.report()
        self.check_report_consistent(report)

        resident = self.names(report, 'total cache bytes')
        self.assertTableIn('first', resident)
        self.assertTableIn('second', resident)

    # The metadata is never named: it is the connection statistics' business, not the operator's.
    def test_metadata_excluded(self):
        self.populate('table:visible', 2000)
        self.session.checkpoint()

        report = self.report()
        for ranking in self.rankings:
            for _, name in report[ranking]['entries']:
                self.assertNotIn('WiredTiger.wt', name)

    # The history store is ranked like any other tree. It counts towards the connection totals a
    # ranking is measured against, so an operator looking at a report where it holds the cache has
    # to be able to see that.
    def test_history_store_ranked_under_load(self):
        uri = 'table:hsload'
        self.session.create(uri, 'key_format=S,value_format=S')

        rows = 2000
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))

        # Successive committed versions of every key push the older ones into the history store.
        for ts in range(2, 12):
            c = self.session.open_cursor(uri)
            for i in range(rows):
                self.session.begin_transaction()
                c['k%08d' % i] = self.value
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
            c.close()
        self.session.checkpoint()

        # Reading at an old timestamp comes back out of the history store, so it takes read and
        # eviction traffic as well as the writes above.
        for ts in range(2, 6):
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
            c = self.session.open_cursor(uri)
            for _ in c:
                pass
            c.close()
            self.session.rollback_transaction()

        # The guard below is only meaningful if the workload actually used the history store.
        stat = self.session.open_cursor('statistics:')
        hs_inserts = stat[wiredtiger.stat.conn.cache_hs_insert][2]
        stat.close()
        self.assertGreater(hs_inserts, 0)

        report = self.report()
        self.check_report_consistent(report)

        # The workload above drove enough history store traffic that it has to show up somewhere.
        self.assertTrue(
            any('WiredTigerHS' in name
                for ranking in self.rankings for name in self.names(report, ranking)),
            'the history store held the cache but was not ranked')

        for ranking in self.rankings:
            for name in self.names(report, ranking):
                self.assertNotIn('WiredTiger.wt', name)

    # A table dropped while it is being reported leaves the ranking without taking the connection
    # with it.
    def test_drop_while_reported(self):
        self.populate_clean('table:doomed', 2500)
        self.assertTableIn('doomed', self.names(self.report(), 'total cache bytes'))

        self.session.drop('table:doomed')

        report = self.report()
        self.check_report_consistent(report)
        for ranking in self.rankings:
            self.assertTableNotIn('doomed', self.names(report, ranking))

    # Many tables at once still produce a bounded, well-formed report.
    def test_many_tables(self):
        for i in range(60):
            self.populate('table:many%d' % i, 60)

        # Report repeatedly: the threshold settles towards the tables that are actually there.
        for _ in range(10):
            report = self.report()
            self.check_report_consistent(report)

        for ranking in self.rankings:
            self.assertLessEqual(report[ranking]['count'], self.slots)

    # A threshold nothing reaches is lowered until it says something, and never below its floor.
    def test_threshold_lowers_when_nothing_qualifies(self):
        self.populate('table:modest', 200)

        thresholds = []
        for _ in range(12):
            thresholds.append(self.report()['total cache bytes']['threshold'])

        self.assertEqual(thresholds, sorted(thresholds, reverse = True),
            'threshold rose while nothing qualified: %s' % thresholds)
        self.assertLess(thresholds[-1], thresholds[0])
        # The threshold has a small nonzero floor, so it can fall only so far, not to zero.
        self.assertGreaterEqual(thresholds[-1], 4 * 1024)

    # Reading a table back into an empty cache puts it in the read ranking.
    def test_read_ranking(self):
        self.populate('table:reread', 2500)
        self.session.checkpoint()

        # Reopening leaves the cache empty, so the scan below has to read every page.
        self.reopen_conn()
        self.read_all('table:reread')

        report = self.report()
        self.check_report_consistent(report)
        self.assertTableIn('reread', self.names(report, 'recent bytes read'))

    # A table being evicted from appears in the eviction ranking.
    def test_evict_ranking(self):
        # A cache this small cannot hold the data, so eviction has to run.
        self.conn.reconfigure('cache_size=10MB')
        self.populate('table:evicted', 4000)

        report = self.report()
        self.check_report_consistent(report)
        self.assertTableIn('evicted', self.names(report, 'recent bytes evicted'))

    # Update memory is attributed to the table that holds it.
    def test_update_ranking(self):
        self.populate('table:updates', 2500)

        report = self.report()
        self.check_report_consistent(report)

        self.assertTableIn('updates', self.names(report, 'update bytes'))
        self.assertLessEqual(report['update bytes']['listed'], report['update bytes']['total'])

    # Repeated reports neither drift nor crash.
    def test_repeated_reports(self):
        self.populate('table:steady', 2000)
        for _ in range(25):
            self.check_report_consistent(self.report())

    # The report survives the cache being resized underneath it, which is where the threshold comes
    # from.
    def test_cache_resize(self):
        self.populate('table:resized', 2000)
        self.check_report_consistent(self.report())

        self.conn.reconfigure('cache_size=500MB')
        self.check_report_consistent(self.report())

        self.conn.reconfigure('cache_size=20MB')
        self.check_report_consistent(self.report())

# The same rankings, delivered through the verbose category rather than on request.
class test_cache_top02(wttest.WiredTigerTestCase):
    # A short sweep interval so the server that emits the report comes around promptly.
    conn_config = ('create,cache_size=100MB,file_manager=(close_scan_interval=1),'
        'verbose=[cache_top]')

    value = 'v' * 4096

    def test_verbose_report_emitted(self):
        self.session.create('table:verbose', 'key_format=S,value_format=S')
        c = self.session.open_cursor('table:verbose')
        for i in range(2500):
            c['k%08d' % i] = self.value
        c.close()

        # The report is emitted by a background server, so poll for it with a deadline rather than
        # sleeping for one.
        deadline = time.time() + 60
        while True:
            found = 'cache top ' in self.readStdout(200000)
            self.cleanStdout()
            if found:
                break
            self.assertLess(time.time(), deadline,
                'no cache_top verbose report within the deadline')
            time.sleep(0.5)

# The rankings on a connection that has no disk behind it.
class test_cache_top03(wttest.WiredTigerTestCase):
    conn_config = 'create,cache_size=100MB,in_memory=true'

    value = 'v' * 4096

    def test_in_memory(self):
        self.session.create('table:inmemory', 'key_format=S,value_format=S')
        c = self.session.open_cursor('table:inmemory')
        for i in range(2000):
            c['k%08d' % i] = self.value
        c.close()

        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()
        self.assertIn('cache top total cache bytes', out)
        self.assertTrue(any(n in out for n in table_names(self, 'inmemory')),
            'inmemory table not found in report')

# Turning the rankings on and off on a running connection, which is how they are reached in the
# field: the verbose category is part of the runtime configuration.
class test_cache_top05(wttest.WiredTigerTestCase):
    conn_config = 'create,cache_size=100MB,file_manager=(close_scan_interval=1)'

    value = 'v' * 4096

    def test_verbose_enabled_at_runtime(self):
        self.session.create('table:runtime', 'key_format=S,value_format=S')
        c = self.session.open_cursor('table:runtime')
        for i in range(2500):
            c['k%08d' % i] = self.value
        c.close()
        self.session.checkpoint()

        # Nothing has asked for the rankings yet.
        self.cleanStdout()
        self.conn.reconfigure('verbose=[cache_top]')

        deadline = time.time() + 60
        while True:
            found = 'cache top ' in self.readStdout(200000)
            self.cleanStdout()
            if found:
                break
            self.assertLess(time.time(), deadline,
                'no report after enabling the category at runtime')
            time.sleep(0.5)

        # Turning it back off is accepted, and asking directly still works.
        self.conn.reconfigure('verbose=[]')
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        self.assertIn('cache top ', self.readStdout(200000))
        self.cleanStdout()

# Details of what the report can carry: long names, other table types, and a report shared with the
# other things debug_info can print.
class test_cache_top06(wttest.WiredTigerTestCase):
    conn_config = ('create,cache_size=100MB,statistics=(all),'
        'eviction_dirty_target=60,eviction_dirty_trigger=80,'
        'eviction_updates_target=50,eviction_updates_trigger=70')

    value = 'v' * 4096

    def report_text(self):
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()
        return out

    def populate(self, uri, rows, config = 'key_format=S,value_format=S'):
        self.session.create(uri, config)
        c = self.session.open_cursor(uri)
        for i in range(rows):
            c['k%08d' % i] = self.value
        c.close()
        self.session.checkpoint()

    # A name too long for the report's buffer would lose the characters that tell two tables apart,
    # so check a long one arrives whole.
    def test_long_table_name(self):
        name = 'a_long_table_name_' + 'x' * 120
        self.populate('table:' + name, 2500)
        text = self.report_text()
        self.assertTrue(any(n in text for n in table_names(self, name)), 'long table name not found')

    # A column store is ranked the same way a row store is.
    def test_column_store(self):
        self.session.create('table:columns', 'key_format=r,value_format=S')
        c = self.session.open_cursor('table:columns')
        for i in range(2500):
            c[i + 1] = self.value
        c.close()
        self.session.checkpoint()

        text = self.report_text()
        self.assertTrue(any(n in text for n in table_names(self, 'columns')), 'columns table not found')

    # The rankings coexist with the rest of what debug_info prints.
    def test_combined_with_other_categories(self):
        self.populate('table:combined', 2500)

        self.cleanStdout()
        self.conn.debug_info('cache_top=true,handles=true')
        out = self.readStdout(200000)
        self.cleanStdout()
        self.assertIn('cache top ', out)
        self.assertIn('Data handle dump', out)

    # The rankings belong to a connection and start empty on the next one.
    def test_reset_on_reopen(self):
        self.populate('table:transient', 2500)
        text = self.report_text()
        self.assertTrue(any(n in text for n in table_names(self, 'transient')),
            'transient table not found')

        self.reopen_conn()

        out = self.report_text()
        self.assertIn('cache top ', out)
        for n in table_names(self, 'transient'):
            self.assertNotIn(n, out)

# A table whose data handle is closed and reopened in place, which is what an alter does. The
# rankings hold a pointer to the tree across that close, and the reopen resets the tree's record of
# where it sits in them.
@wttest.skip_for_hook("disagg", "session.alter is not supported for layered tables")
class test_cache_top07(wttest.WiredTigerTestCase):
    conn_config = 'create,cache_size=100MB,statistics=(all)'

    value = 'v' * 4096

    def populate(self, uri, rows, start = 0):
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        for i in range(start, start + rows):
            c['k%08d' % i] = self.value
        c.close()

    # Parse the report into {ranking: {listed, total, names}}.
    def report(self):
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()

        header_re = re.compile(r'cache top (?P<ranking>.+?): \d+ tables above \d+B '
            r'hold (?P<listed>\d+)B(?: of (?P<total>\d+)B)?$')
        entry_re = re.compile(r'^\s+\d+B (?P<name>\S+)$')

        report = {}
        ranking = None
        for line in out.splitlines():
            header = header_re.search(line)
            if header is not None:
                ranking = header.group('ranking')
                report[ranking] = {
                    'listed': int(header.group('listed')),
                    'total': None if header.group('total') is None
                        else int(header.group('total')),
                    'names': [],
                }
                continue
            entry = entry_re.match(line)
            if entry is not None and ranking is not None:
                report[ranking]['names'].append(entry.group('name'))
        return report

    # A tree must never occupy more than one slot of the same ranking, and the bytes a ranking says
    # it lists must not exceed what the whole connection holds.
    def test_no_duplicates_across_handle_reopen(self):
        self.populate('table:churn', 2500)
        self.session.checkpoint()

        # The guard below is only meaningful if the table is big enough to be ranked at all.
        before = self.report()
        self.assertTrue(
            any(n in before['total cache bytes']['names'] for n in table_names(self, 'churn')),
            'the table under test never reached a ranking')

        # An alter closes the data handle and reopens it in place, reusing the same tree.
        self.session.alter('table:churn', 'access_pattern_hint=random')
        self.populate('table:churn', 2500, start = 50000)
        self.session.checkpoint()

        after = self.report()
        self.assertTrue(after, 'no rankings were reported')
        for ranking, r in after.items():
            self.assertEqual(len(r['names']), len(set(r['names'])),
                'ranking "%s" names a table more than once: %s' % (ranking, r['names']))
            if r['total'] is not None:
                self.assertLessEqual(r['listed'], r['total'],
                    'ranking "%s" lists more bytes than the connection holds' % ranking)

# A ranking's threshold can fall far below where it stood when a tree last called in. A decayed
# value converges on a steady state rather than growing without limit, so a tree only returns to
# the ranking if its recheck value comes down with the threshold.
class test_cache_top08(wttest.WiredTigerTestCase):
    # A large cache so the ranking opens with a threshold well above what this test reads. A
    # checkpoint adjusts thresholds, and one runs at startup, so the opening bar is already a
    # fraction of cache size by the time the test begins.
    conn_config = 'create,cache_size=1GB,statistics=(all)'

    value = 'v' * 4096

    # The "recent bytes read" threshold and its entries as (bytes, name).
    def read_ranking(self):
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()

        threshold = None
        entries = []
        ranking = None
        for line in out.splitlines():
            header = re.search(
                r'cache top (?P<ranking>.+?): \d+ tables above (?P<threshold>\d+)B', line)
            if header is not None:
                ranking = header.group('ranking')
                if ranking == 'recent bytes read':
                    threshold = int(header.group('threshold'))
                continue
            entry = re.match(r'^\s+(?P<value>\d+)B (?P<name>\S+)$', line)
            if entry is not None and ranking == 'recent bytes read':
                entries.append((int(entry.group('value')), entry.group('name')))
        self.assertIsNotNone(threshold, 'no read ranking in the report')
        return threshold, entries

    def read_rows(self, uri, lo, hi):
        c = self.session.open_cursor(uri)
        for i in range(lo, hi):
            c.set_key('k%08d' % i)
            c.search()
        c.close()

    def test_read_ranking_after_threshold_falls(self):
        uri = 'table:cooling'
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        for i in range(12000):
            c['k%08d' % i] = self.value
        c.close()
        self.session.checkpoint()

        # Reopen so the reads below come from disk rather than out of cache.
        self.reopen_conn()

        # Read well under the ranking's opening threshold, so the table cannot qualify yet.
        self.read_rows(uri, 0, 150)
        opening, entries = self.read_ranking()
        self.assertEqual(entries, [],
            'the table qualified before the threshold fell, so this proves nothing: %s' % entries)

        # Nothing qualifies, so repeated reports drive the threshold down to its floor.
        for _ in range(8):
            fallen, _ = self.read_ranking()
        self.assertLess(fallen, opening)

        # Reading more must now put the table in the ranking.
        self.read_rows(uri, 6000, 6150)
        _, entries = self.read_ranking()
        names = [name for _, name in entries]
        self.assertTrue(any(n in names for n in table_names(self, 'cooling')),
            'table missing from the read ranking after the threshold fell: %s' % names)

        # It has to be there because its recheck value came down, not because it grew past the
        # threshold the ranking opened with.
        for value, name in entries:
            if name in table_names(self, 'cooling'):
                self.assertLess(value, opening)

# The same threshold fall, for a ranking read straight from a tree's counters. Here eviction on the
# tree is what brings its recheck value down, so the tree is admitted the next time it accounts for
# a page.
class test_cache_top09(wttest.WiredTigerTestCase):
    # See test_cache_top08: enough headroom that the checkpoint below cannot lift the table over
    # the bar before the test has driven the threshold down itself.
    conn_config = 'create,cache_size=1GB,statistics=(all)'

    value = 'v' * 4096

    # The "total cache bytes" threshold and its entries as (bytes, name).
    def resident_ranking(self):
        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()

        threshold = None
        entries = []
        ranking = None
        for line in out.splitlines():
            header = re.search(
                r'cache top (?P<ranking>.+?): \d+ tables above (?P<threshold>\d+)B', line)
            if header is not None:
                ranking = header.group('ranking')
                if ranking == 'total cache bytes':
                    threshold = int(header.group('threshold'))
                continue
            entry = re.match(r'^\s+(?P<value>\d+)B (?P<name>\S+)$', line)
            if entry is not None and ranking == 'total cache bytes':
                entries.append((int(entry.group('value')), entry.group('name')))
        self.assertIsNotNone(threshold, 'no resident ranking in the report')
        return threshold, entries

    def test_resident_ranking_after_threshold_falls(self):
        uri = 'table:cooling'
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        # Small enough to stay under the ranking's opening threshold.
        for i in range(300):
            c['k%08d' % i] = self.value
        c.close()
        self.session.checkpoint()

        opening, entries = self.resident_ranking()
        names = [name for _, name in entries]
        self.assertFalse(any(n in names for n in table_names(self, 'cooling')),
            'the table qualified before the threshold fell, so this proves nothing: %s' % names)

        # Nothing qualifies, so repeated reports drive the threshold down to its floor.
        for _ in range(8):
            fallen, _ = self.resident_ranking()
        self.assertLess(fallen, opening)

        # Shrink the cache so eviction runs on the tree, which is what lowers its recheck value.
        self.conn.reconfigure('cache_size=1MB')

        # Eviction is a background thread, so poll: read pages back in, which both accounts for
        # them and gives the tree a chance to be admitted.
        deadline = time.time() + 30
        while True:
            c = self.session.open_cursor(uri)
            for _ in c:
                pass
            c.close()

            _, entries = self.resident_ranking()
            names = [name for _, name in entries]
            if any(n in names for n in table_names(self, 'cooling')):
                break
            self.assertLess(time.time(), deadline,
                'table never returned to the resident ranking after the threshold fell')
            time.sleep(0.5)

        # It has to be there because its recheck value came down, not because it grew past the
        # threshold the ranking opened with.
        for value, name in entries:
            if name in table_names(self, 'cooling'):
                self.assertLess(value, opening)

# The same rankings as a connection statistic, which is reachable without verbose logging or an
# explicit request. Recomputed at the end of every checkpoint.
class test_cache_top10(wttest.WiredTigerTestCase):
    conn_config = 'create,cache_size=100MB,statistics=(all)'

    value = 'v' * 4096

    def stat(self, name):
        c = self.session.open_cursor('statistics:', None, None)
        v = c[getattr(wiredtiger.stat.conn, name)][2]
        c.close()
        return v

    def populate(self, uri, rows):
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        for i in range(rows):
            c['k%08d' % i] = self.value
        c.close()

    # A table holding most of the cache is reported as holding most of the cache.
    def test_concentration_published(self):
        self.populate('table:hog', 4000)
        self.session.checkpoint()

        inuse = self.stat('cache_top_inuse_pct')
        updates = self.stat('cache_top_updates_pct')
        inuse5 = self.stat('cache_top5_inuse_pct')
        updates5 = self.stat('cache_top5_updates_pct')

        # A percentage is a percentage, whatever the workload.
        for name in ['cache_top_inuse_pct', 'cache_top_updates_pct', 'cache_top5_inuse_pct',
            'cache_top5_updates_pct']:
            pct = self.stat(name)
            self.assertGreaterEqual(pct, 0, name)
            self.assertLessEqual(pct, 100, name)

        # The largest few are a subset of the whole ranking, so they can never account for more.
        self.assertLessEqual(inuse5, inuse)
        self.assertLessEqual(updates5, updates)

        # One table holds the cache here, so the ranked tables have to account for a real share of
        # it. Deliberately a loose bound: how much is resident depends on when eviction last ran.
        self.assertGreater(inuse, 0, 'no cache attributed to the ranked tables')

    # The statistic agrees with the report built from the same rankings.
    def test_agrees_with_report(self):
        self.populate('table:hog', 4000)
        self.session.checkpoint()

        self.cleanStdout()
        self.conn.debug_info('cache_top')
        out = self.readStdout(200000)
        self.cleanStdout()

        header = re.search(r'cache top total cache bytes: \d+ tables above \d+B '
            r'hold (?P<listed>\d+)B of (?P<total>\d+)B', out)
        self.assertIsNotNone(header, 'no resident ranking header in the report')
        listed = int(header.group('listed'))
        total = int(header.group('total'))
        self.assertGreater(total, 0)

        # The report and the statistic are separate observations of a moving cache, so compare
        # loosely: both must agree on whether the cache is concentrated.
        from_report = listed * 100 // total
        self.assertLess(abs(self.stat('cache_top_inuse_pct') - from_report), 25,
            'statistic and report disagree: %d vs %d' % (
                self.stat('cache_top_inuse_pct'), from_report))
