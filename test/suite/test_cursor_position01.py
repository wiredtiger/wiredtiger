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
#
# test_cursor_position01.py
#   WT_CURSOR.set_position and get_position on a row-store table with several levels of pages,
#   built in memory and read back from disk.

import wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios

FIRST = wiredtiger.WT_POSITION_ANCHOR_FIRST
MIDDLE = wiredtiger.WT_POSITION_ANCHOR_MIDDLE
LAST = wiredtiger.WT_POSITION_ANCHOR_LAST
PREV = wiredtiger.WT_POSITION_PREV
KEY_ONLY = wiredtiger.WT_POSITION_KEY_ONLY
CACHE_ONLY = wiredtiger.WT_POSITION_CACHE_ONLY

@wttest.skip_for_hook("disagg", "Disagg rewrites table: creates; this test assumes ordinary tables")
class test_cursor_position01(wttest.WiredTigerTestCase):
    name = 'test_cursor_position01'
    uri = 'table:' + name
    nrows = 20000
    nsamples = 50

    scenarios = make_scenarios([
        ('inmem', dict(in_memory=True)),
        ('disk', dict(in_memory=False)),
    ])

    def conn_config(self):
        config = 'statistics=(fast),cache_size=100MB'
        if self.in_memory:
            config += ',in_memory=true'
        return config

    # Raw keys: boundary keys returned in key-only mode are prefixes of record keys, which only a
    # raw format unpacks intact.
    def key(self, i):
        return b'key%08d' % i

    def value(self, key):
        return 'value-' + key.decode() + '-' * 60

    def create_table(self, uri=None):
        # Small pages give the tree several levels. An in-memory tree only deepens through
        # in-memory splits, so lower their thresholds.
        config = 'key_format=u,value_format=S,leaf_page_max=4KB,internal_page_max=4KB'
        if self.in_memory:
            config += ',memory_page_max=32KB,split_deepen_min_child=50'
        self.session.create(uri or self.uri, config)

    def populate(self, uri=None, nrows=None):
        uri = uri or self.uri
        nrows = nrows or self.nrows
        self.create_table(uri)
        c = self.session.open_cursor(uri)
        for i in range(nrows):
            c[self.key(i)] = self.value(self.key(i))
        c.close()
        if not self.in_memory:
            # Read the tree back from disk with its on-disk page shape.
            self.reopen_conn()
        return [self.key(i) for i in range(nrows)]

    def require_disk(self):
        if self.in_memory:
            self.skipTest('needs pages that can be written to and read from disk')

    def check_record(self, c, keys):
        key = c.get_key()
        self.assertIn(key, keys)
        self.assertEqual(c.get_value(), self.value(key))
        return key

    def position(self, c, pos, flags=0):
        p = wiredtiger.Position()
        p.pos = pos
        p.flags = flags
        self.assertEqual(c.set_position(p), 0)
        return p

    def position_key(self, c, pos, flags, keys):
        self.position(c, pos, flags)
        return self.check_record(c, keys)

    def stat(self, stat_key, uri=''):
        return self.get_stat(stat_key, uri=uri)

    def sample_positions(self):
        return [i / self.nsamples for i in range(self.nsamples + 1)]

    def test_get_position_walk(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        prev = -1.0
        count = 0
        while c.next() == 0:
            pos = c.get_position()
            self.assertGreaterEqual(pos, 0.0)
            self.assertLessEqual(pos, 1.0)
            self.assertGreaterEqual(pos, prev)
            prev = pos
            count += 1
        self.assertEqual(count, len(keys))

        prev = 2.0
        while c.prev() == 0:
            pos = c.get_position()
            self.assertLessEqual(pos, prev)
            prev = pos
            count -= 1
        self.assertEqual(count, 0)
        self.assertGreater(self.stat(stat.conn.cursor_get_position), 0)
        c.close()

    def test_round_trip(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        for key in keys[::97] + [keys[-1]]:
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            p = self.position(c, c.get_position())
            self.assertEqual(p.pages_skipped, 0)
            found = self.check_record(c, keys)
            if self.in_memory:
                # A record not yet written to a page maps to the nearest page slot before it.
                self.assertLessEqual(found, key)
            else:
                self.assertEqual(found, key)
        c.close()

    def test_sampling(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        prev = None
        for pos in self.sample_positions():
            p = self.position(c, pos)
            self.assertEqual(p.pages_skipped, 0)
            key = self.check_record(c, keys)
            if prev is not None:
                self.assertGreaterEqual(key, prev)
            prev = key

        # Out-of-range positions address the first and last pages.
        for pos in (-1.0, -0.001, 0.0):
            self.assertEqual(self.position_key(c, pos, 0, keys), keys[0])
        for pos in (1.0, 1.001, 2.0):
            self.assertEqual(self.position_key(c, pos, LAST | PREV, keys), keys[-1])
            if not self.in_memory:
                self.assertEqual(self.position_key(c, pos, 0, keys), keys[-1])

        # The cursor is positioned: iteration continues from the record found.
        idx = keys.index(self.position_key(c, 0.5, 0, keys))
        self.assertEqual(c.next(), 0)
        self.assertEqual(self.check_record(c, keys), keys[idx + 1])
        self.assertEqual(c.prev(), 0)
        self.assertEqual(c.prev(), 0)
        self.assertEqual(self.check_record(c, keys), keys[idx - 1])
        self.assertGreater(self.stat(stat.conn.cursor_set_position), 0)
        c.close()

    def test_anchors(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        prev_first = prev_last = None
        for pos in self.sample_positions():
            first = self.position_key(c, pos, FIRST, keys)
            last = self.position_key(c, pos, LAST | PREV, keys)
            exact = self.position_key(c, pos, 0, keys)
            middle = self.position_key(c, pos, MIDDLE, keys)
            self.assertLessEqual(first, exact)
            self.assertLessEqual(exact, last)
            self.assertLessEqual(first, middle)
            self.assertLessEqual(middle, last)
            if prev_first is not None:
                self.assertGreaterEqual(first, prev_first)
                self.assertGreaterEqual(last, prev_last)
            prev_first, prev_last = first, last

            # Unknown flag bits are ignored.
            self.assertEqual(self.position_key(c, pos, FIRST | 0x80000000, keys), first)
            self.assertEqual(self.position_key(c, pos, LAST | PREV | 0x08, keys), last)
        c.close()

    def test_key_only(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        prev_boundary = None
        for pos in self.sample_positions():
            first = self.position_key(c, pos, FIRST, keys)
            # The record before the page's first, if any.
            before = None
            if c.prev() == 0:
                before = c.get_key()

            p = self.position(c, pos, KEY_ONLY)
            self.assertEqual(p.pages_skipped, 0)
            boundary = c.get_key()

            # A key but no position, exactly the state set_key leaves.
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: c.get_value(), '/requires value be set/')
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: c.get_position(), '/requires a positioned cursor/')

            # The boundary separates the page from the records before it.
            self.assertLessEqual(boundary, first)
            if before is None:
                self.assertEqual(boundary, b'')
            elif boundary != b'':
                self.assertGreater(boundary, before)
            if prev_boundary is not None:
                self.assertGreaterEqual(boundary, prev_boundary)
            prev_boundary = boundary

            # Searching from the boundary reaches the first record of the page.
            self.assertGreaterEqual(c.search_near(), 0)
            self.assertEqual(self.check_record(c, keys), first)

        # The anchor and direction have no effect on the boundary key.
        self.position(c, 0.5, KEY_ONLY)
        boundary = c.get_key()
        for flags in (FIRST, LAST | PREV, MIDDLE, PREV):
            self.position(c, 0.5, KEY_ONLY | flags)
            self.assertEqual(c.get_key(), boundary)

        # The leftmost page has the empty key as its boundary.
        for pos in (-1.0, 0.0):
            self.position(c, pos, KEY_ONLY)
            self.assertEqual(c.get_key(), b'')
            self.assertGreater(c.search_near(), 0)
            self.assertEqual(self.check_record(c, keys), keys[0])
        c.close()

    def test_key_only_reads_no_leaf_pages(self):
        self.require_disk()
        self.populate()
        c = self.session.open_cursor(self.uri)

        # Nothing but the root is cached after reopening; the first pass reads internal pages.
        for pos in self.sample_positions():
            self.position(c, pos, KEY_ONLY)
        self.assertEqual(self.stat(stat.dsrc.cache_read_leaf, self.uri), 0)

        # With the internal pages cached, key-only positioning reads nothing at all.
        reads = self.stat(stat.conn.cache_read)
        for pos in self.sample_positions():
            self.position(c, pos, KEY_ONLY)
            self.position(c, pos, KEY_ONLY | PREV)
        self.assertEqual(self.stat(stat.conn.cache_read), reads)
        self.assertEqual(self.stat(stat.dsrc.cache_read_leaf, self.uri), 0)
        c.close()

    def test_visibility(self):
        keys = self.populate()
        lo, hi = 2 * self.nrows // 5, 3 * self.nrows // 5
        deleted = keys[lo:hi]
        c = self.session.open_cursor(self.uri)
        positions = []
        for key in (keys[lo], keys[(lo + hi) // 2], keys[hi - 1]):
            c.set_key(key)
            self.assertEqual(c.search(), 0)
            positions.append(c.get_position())
        c.reset()

        # Positions are soft: an in-memory tree reshapes as tombstones accumulate on its pages,
        # so only the on-disk tree, which does not change here, supports checking which neighbor
        # of the range is reached. A removed record is never returned in either case.
        def check_skips_range(cursor):
            for pos in positions:
                for flags, neighbor in ((0, keys[hi]), (PREV, keys[lo - 1])):
                    found = self.position_key(cursor, pos, flags, keys)
                    self.assertNotIn(found, deleted)
                    if not self.in_memory:
                        self.assertEqual(found, neighbor)

        def check_sees_range(cursor):
            for pos in positions:
                for flags in (0, PREV):
                    found = self.position_key(cursor, pos, flags, keys)
                    if not self.in_memory:
                        self.assertIn(found, deleted)

        # Remove the range in another session's transaction and leave it uncommitted.
        deleter = self.conn.open_session()
        deleter.begin_transaction()
        dc = deleter.open_cursor(self.uri)
        for key in deleted:
            dc.set_key(key)
            self.assertEqual(dc.remove(), 0)

        # The deleting transaction walks past its own removes; everyone else still sees them.
        check_skips_range(dc)
        check_sees_range(c)

        # A snapshot taken before the commit keeps seeing the range afterwards.
        reader = self.conn.open_session()
        reader.begin_transaction('isolation=snapshot')
        rc = reader.open_cursor(self.uri)
        check_sees_range(rc)
        deleter.commit_transaction()
        check_sees_range(rc)
        reader.rollback_transaction()
        check_skips_range(rc)
        check_skips_range(c)

        # Iteration from the record before the gap continues with the record after it.
        if self.position_key(c, positions[1], PREV, keys) == keys[lo - 1]:
            self.assertEqual(c.next(), 0)
            self.assertEqual(self.check_record(c, keys), keys[hi])
        rc.close()
        dc.close()
        c.close()

    def test_cache_only(self):
        self.require_disk()
        keys = self.populate()
        # A cache much smaller than the table, freshly opened: only the root is in memory.
        self.reopen_conn(config=self.conn_config().replace('100MB', '1MB'))
        c = self.session.open_cursor(self.uri)

        reads = self.stat(stat.conn.cache_read)
        notfound = skipped = 0
        p = wiredtiger.Position()
        for pos in self.sample_positions():
            for flags in (CACHE_ONLY, CACHE_ONLY | PREV, CACHE_ONLY | KEY_ONLY):
                p.pos = pos
                p.flags = flags
                ret = c.set_position(p)
                self.assertIn(ret, (0, wiredtiger.WT_NOTFOUND))
                if ret == wiredtiger.WT_NOTFOUND:
                    notfound += 1
                elif p.pages_skipped != 0:
                    skipped += 1
                    self.check_record(c, keys)
        self.assertGreater(notfound + skipped, 0)
        self.assertEqual(self.stat(stat.conn.cache_read), reads)

        # A page read by a search is found again from the cache at the record's position.
        key = keys[self.nrows // 2]
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        pos = c.get_position()
        reads = self.stat(stat.conn.cache_read)
        p = self.position(c, pos, CACHE_ONLY)
        self.assertEqual(p.pages_skipped, 0)
        self.assertEqual(self.check_record(c, keys), key)
        self.assertEqual(self.stat(stat.conn.cache_read), reads)

        # Reading the whole table through a small cache evicts most of it.
        c.reset()
        count = 0
        while c.next() == 0:
            count += 1
        self.assertEqual(count, len(keys))
        reads = self.stat(stat.conn.cache_read)
        notfound = skipped = 0
        for pos in self.sample_positions():
            p.pos = pos
            p.flags = CACHE_ONLY
            ret = c.set_position(p)
            self.assertIn(ret, (0, wiredtiger.WT_NOTFOUND))
            if ret == wiredtiger.WT_NOTFOUND:
                notfound += 1
            elif p.pages_skipped != 0:
                skipped += 1
                self.check_record(c, keys)
        self.assertGreater(notfound + skipped, 0)
        self.assertEqual(self.stat(stat.conn.cache_read), reads)
        c.close()

    def test_pages_skipped_after_truncate(self):
        self.require_disk()
        # Values that fit on the page, so whole pages inside the range are truncated without
        # being read.
        uri = 'table:' + self.name + '_trunc'
        self.session.create(uri,
            'key_format=u,value_format=S,leaf_page_max=4KB,internal_page_max=4KB')
        c = self.session.open_cursor(uri)
        for i in range(self.nrows):
            c[self.key(i)] = 'v' * 80
        c.close()
        self.session.checkpoint()
        self.reopen_conn()
        keys = [self.key(i) for i in range(self.nrows)]

        c = self.session.open_cursor(uri)
        lo, hi = self.nrows // 2, self.nrows // 2 + 1000
        c.set_key(keys[lo])
        self.assertEqual(c.search(), 0)
        lo_pos = c.get_position()
        c.set_key(keys[hi])
        self.assertEqual(c.search(), 0)
        hi_pos = c.get_position()
        c.reset()
        self.assertLess(lo_pos, hi_pos)

        start = self.session.open_cursor(uri)
        stop = self.session.open_cursor(uri)
        start.set_key(keys[lo])
        stop.set_key(keys[hi])
        self.assertEqual(self.session.truncate(None, start, stop, None), 0)
        start.close()
        stop.close()

        # Positions inside the range land on the record adjacent to it in the walk direction and
        # report the pages stepped over.
        p = wiredtiger.Position()
        for frac in (0.3, 0.5, 0.7):
            p.pos = lo_pos + frac * (hi_pos - lo_pos)
            p.flags = 0
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(c.get_key(), keys[hi + 1])
            self.assertGreater(p.pages_skipped, 0)

            p.flags = PREV
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(c.get_key(), keys[lo - 1])
            self.assertGreater(p.pages_skipped, 0)

            # Boundary keys do not depend on the state of the leaf pages.
            p.flags = KEY_ONLY
            self.assertEqual(c.set_position(p), 0)
            self.assertEqual(p.pages_skipped, 0)
            boundary = c.get_key()
            self.assertGreaterEqual(boundary, keys[lo - 1])
            self.assertLessEqual(boundary, keys[hi + 1])
        c.close()

    def test_errors(self):
        keys = self.populate()
        c = self.session.open_cursor(self.uri)
        p = wiredtiger.Position()

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.get_position(), '/requires a positioned cursor/')

        p.pos = float('nan')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.set_position(p), '/requires a numeric position/')

        # After an error the cursor is usable.
        p.pos = 0.5
        self.assertEqual(c.set_position(p), 0)
        self.check_record(c, keys)

        c.reset()
        c.set_key(keys[10])
        c.bound('bound=lower')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: c.set_position(p), '/not compatible with cursor set_position/')
        c.bound('action=clear')
        self.assertEqual(c.set_position(p), 0)
        c.close()

        rc = self.session.open_cursor(self.uri, None, 'next_random=true')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: rc.set_position(p), '/not supported by next_random cursors/')
        rc.close()

        col_uri = 'table:' + self.name + '_col'
        self.session.create(col_uri, 'key_format=r,value_format=S')
        cc = self.session.open_cursor(col_uri)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: cc.set_position(p), '/only supported by row-store objects/')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: cc.get_position(), '/only supported by row-store objects/')
        cc.close()

        empty_uri = 'table:' + self.name + '_empty'
        self.session.create(empty_uri, 'key_format=u,value_format=S')
        if not self.in_memory:
            bc = self.session.open_cursor(empty_uri, None, 'bulk')
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: bc.set_position(p), '/not supported by bulk cursors/')
            bc.close()
        self.assertGreater(self.stat(stat.conn.cursor_set_position_error), 0)
        self.assertGreater(self.stat(stat.conn.cursor_get_position_error), 0)

        # An empty table has no record but does have a leftmost boundary.
        ec = self.session.open_cursor(empty_uri)
        for pos in (0.0, 0.5, 1.0):
            p.pos = pos
            p.flags = 0
            self.assertEqual(ec.set_position(p), wiredtiger.WT_NOTFOUND)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: ec.get_key(), '/requires key be set/')
            p.flags = KEY_ONLY
            self.assertEqual(ec.set_position(p), 0)
            self.assertEqual(p.pages_skipped, 0)
            self.assertEqual(ec.get_key(), b'')
            self.assertEqual(ec.search_near(), wiredtiger.WT_NOTFOUND)

        # So does a table small enough to fit on one page.
        for i in range(5):
            ec[self.key(i)] = 'v'
        for pos in (0.0, 0.5, 1.0):
            p.pos = pos
            p.flags = KEY_ONLY
            self.assertEqual(ec.set_position(p), 0)
            self.assertEqual(ec.get_key(), b'')
            self.assertGreater(ec.search_near(), 0)
            self.assertEqual(ec.get_key(), self.key(0))
            p.flags = 0
            self.assertEqual(ec.set_position(p), 0)
            self.assertIn(ec.get_key(), [self.key(i) for i in range(5)])
        ec.close()

    def test_checkpoint_cursor(self):
        self.require_disk()
        keys = self.populate()
        self.session.checkpoint()

        # Records added after the checkpoint are not part of the checkpoint's position space.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows, self.nrows + 500):
            c[self.key(i)] = self.value(self.key(i))
        c.close()

        ckpt = self.session.open_cursor(self.uri, None, 'checkpoint=WiredTigerCheckpoint')
        prev = None
        for pos in self.sample_positions():
            key = self.position_key(ckpt, pos, 0, keys)
            if prev is not None:
                self.assertGreaterEqual(key, prev)
            prev = key
            self.assertLessEqual(ckpt.get_position(), 1.0)
        self.assertEqual(self.position_key(ckpt, 2.0, LAST | PREV, keys), keys[-1])
        self.assertEqual(ckpt.next(), wiredtiger.WT_NOTFOUND)

        ckpt.set_key(keys[self.nrows // 3])
        self.assertEqual(ckpt.search(), 0)
        self.assertEqual(self.position_key(ckpt, ckpt.get_position(), 0, keys),
            keys[self.nrows // 3])

        self.position(ckpt, 0.5, KEY_ONLY)
        boundary = ckpt.get_key()
        self.assertGreaterEqual(ckpt.search_near(), 0)
        self.assertGreaterEqual(self.check_record(ckpt, keys), boundary)
        ckpt.close()
