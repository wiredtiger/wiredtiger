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

from compact_util import compact_util
from wiredtiger import stat
from wtscenario import make_scenarios

# test_compact17.py
# Exercise first-fit block allocation against a fragmented available extent list.
#
# First-fit allocation returns the lowest-offset extent that is large enough for a request. It is
# used by files configured with block_allocation=first, and by any file for the duration of a
# compaction. Rather than walking the by-offset list, the lookup searches the by-size skiplist.
#
# These are end-to-end checks: they run both allocation policies over the same workload and
# require that the data written into reused space reads back correctly and that the file still
# verifies. They do not pin down which extent the lookup picks, nor whether a search stopped early
# -- a table of this size does not produce enough distinct extent sizes to reach the bucket cap.
# Those properties are covered deterministically by the block_first_srch unit tests in
# test_extent_list.cpp.
class test_compact17(compact_util):
    # A small leaf page keeps each discarded page close to one allocation unit, so the available
    # list ends up holding many small extents instead of a few large ones. That is the shape the
    # by-size lookup is meant to search; with larger leaf pages the file has far fewer pages, and
    # the reused-space checks below have too little to work with to say much.
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=4KB,'
    conn_config = 'cache_size=100MB,statistics=(all)'

    allocation = [
        ('first-fit', dict(block_allocation='first')),
        ('best-fit', dict(block_allocation='best')),
    ]
    scenarios = make_scenarios(allocation)

    table_numkv = 20 * 1000
    value_size = 1024

    # Compaction skips a file whose available list holds less than "free_space_target", which
    # defaults to 20MB. The workload below frees a little over that, so leaving the default in
    # place would make these tests depend on how tightly records happen to pack into a page.
    # Lower it so the binding constraint is the one being exercised: that enough of the available
    # space sits in the front of the file.
    free_space_target = 1024 * 1024
    compact_config = f'free_space_target={free_space_target}'

    def expected_value(self, key):
        # Mirrors the value compact_util.populate() writes, so the contents can be checked exactly.
        return ('%07d' % key) + '_' + 'a' * (self.value_size - 2)

    def create_table(self, uri):
        self.session.create(
            uri, self.create_params + f'block_allocation={self.block_allocation}')

    def get_reuse_bytes(self, uri):
        return self.get_stat(stat.dsrc.block_reuse_bytes, uri)

    def check_fragmented(self, uri, label):
        # These tests only exercise the by-size lookup if the available list actually holds
        # something to look up. Without this guard a change in the workload could silently empty
        # the list and leave the tests passing while covering nothing.
        #
        # The floor is the same "free_space_target" the compaction tests configure: below it
        # compaction is guaranteed to skip the file, so it is the smallest amount of available
        # space that can make this workload meaningful. Being above it is not enough to make
        # compaction run -- that also depends on how much of the space lies in the front of the
        # file -- and it says nothing about how many extents or distinct sizes the space is spread
        # over. The unit tests cover the lookup itself.
        reuse_bytes = self.get_reuse_bytes(uri)
        self.assertGreater(
            reuse_bytes, self.free_space_target, f'{label}: available list is too small')
        return reuse_bytes

    def check_contents(self, uri, expected_keys):
        # Read the whole table back and compare it against the keys that should be present. Report
        # counts and a bounded sample on failure rather than dumping thousands of rows.
        cursor = self.session.open_cursor(uri, None)
        actual = {}
        for key, value in cursor:
            actual[key] = value
        cursor.close()

        self.assertEqual(len(actual), len(expected_keys),
            f'expected {len(expected_keys)} rows, found {len(actual)}')

        missing = sorted(set(expected_keys) - set(actual))
        self.assertEqual(missing, [], f'{len(missing)} missing keys, first: {missing[:5]}')

        wrong = [key for key in expected_keys if actual[key] != self.expected_value(key)]
        self.assertEqual(wrong, [], f'{len(wrong)} keys hold the wrong value, first: {wrong[:5]}')

    def delete_runs(self, uri, step, shortest):
        # Delete a run of keys out of every step keys, leaving the rest in place. The pages a
        # single run frees are adjacent, so they merge into one extent, and because the runs vary
        # in length those extents come out at differing sizes. The live data between runs is what
        # keeps them from merging with each other. Nothing here pins down how many distinct sizes
        # that produces; it is only meant to leave the list holding more than one of them.
        cursor = self.session.open_cursor(uri, None)
        deleted = set()
        for run, base in enumerate(range(0, self.table_numkv, step)):
            for key in range(base, min(base + shortest + (run % 9) * 20, self.table_numkv)):
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
                deleted.add(key)
        cursor.close()
        return deleted

    def test_first_fit_lookup(self):
        # Reuse a fragmented available list, which is the case the by-size lookup replaces.
        uri = 'table:test_compact17_lookup'
        self.create_table(uri)

        # An empty table has nothing available for reuse.
        self.session.checkpoint()
        self.assertEqual(self.get_bytes_avail_for_reuse(uri), 0)

        self.populate(uri, 0, self.table_numkv, value_size=self.value_size)
        self.session.checkpoint()

        deleted = self.delete_runs(uri, 400, 60)
        self.session.checkpoint()

        # The list has to be fragmented for this test to be about the by-size lookup at all.
        self.check_fragmented(uri, 'after delete')

        self.check_contents(uri, set(range(self.table_numkv)) - deleted)

        # Rewrite the deleted keys, which gives the allocator a fragmented list to draw on. This
        # does not assert that any particular allocation came from the list -- only that the data
        # written back is intact once it has.
        cursor = self.session.open_cursor(uri, None)
        for key in sorted(deleted):
            cursor[key] = self.expected_value(key)
        cursor.close()
        self.session.checkpoint()

        # The reused space must hold exactly what was written to it.
        self.check_contents(uri, set(range(self.table_numkv)))

        # Verify needs exclusive access to the file. Reopening the connection is the deterministic
        # way to get it: closing cursors is not enough, because the session caches their handles.
        self.reopen_conn()
        self.session.verify(uri, None)
        self.check_contents(uri, set(range(self.table_numkv)))

    def test_compaction_relocates_pages(self):
        # Compaction switches the file to first-fit for its duration, which is the other caller of
        # the lookup. Freeing the front of the file and leaving it empty is what gives compaction
        # something to do: refilling it first makes compaction return without reviewing a page.
        uri = 'table:test_compact17_compaction'
        self.create_table(uri)

        self.populate(uri, 0, self.table_numkv, value_size=self.value_size)
        self.session.checkpoint()

        deleted = 80 * self.table_numkv // 100
        self.delete_range(uri, deleted)
        self.session.checkpoint()

        # Compaction needs free space ahead of the pages it moves, so deleting the front of the
        # file and leaving it empty is what gives it something to do. The check below is only a
        # floor on the total: it does not establish where that space sits.
        self.check_fragmented(uri, 'after delete')

        size_before = self.get_size(uri)
        self.session.compact(uri, self.compact_config)
        self.session.checkpoint()

        # Fail loudly if compaction did no work: the test would otherwise silently stop covering
        # first-fit allocation.
        progress = self.get_compact_progress_stats(uri)
        self.assertGreater(progress['pages_reviewed'], 0, progress)
        self.assertGreater(progress['pages_rewritten'], 0, progress)

        # Compaction moves pages out of the end of the file so the tail can be truncated, so a
        # pass that rewrote pages should leave the file smaller.
        self.assertLess(self.get_size(uri), size_before)

        # Relocating pages must not change what the table holds.
        self.check_contents(uri, set(range(deleted, self.table_numkv)))

        # Verify needs exclusive access to the file; see the comment in test_first_fit_lookup.
        self.reopen_conn()
        self.session.verify(uri, None)

    def test_allocation_after_reopen(self):
        # The available list is rebuilt from the checkpoint when a file is reopened, so allocations
        # made after a reopen search a list that was read off disk rather than built in memory.
        uri = 'table:test_compact17_reopen'
        self.create_table(uri)

        self.populate(uri, 0, self.table_numkv, value_size=self.value_size)
        self.session.checkpoint()
        deleted = self.delete_runs(uri, 400, 60)
        self.session.checkpoint()

        self.check_fragmented(uri, 'before reopen')

        self.reopen_conn()

        # The space freed before the reopen must still be available for reuse afterwards. The
        # exact amount can differ because adjacent extents are merged as the list is read back.
        self.check_fragmented(uri, 'after reopen')
        self.check_contents(uri, set(range(self.table_numkv)) - deleted)

        # Allocate out of the rebuilt list and check that the data lands intact.
        cursor = self.session.open_cursor(uri, None)
        for key in sorted(deleted):
            cursor[key] = self.expected_value(key)
        cursor.close()
        self.session.checkpoint()

        self.check_contents(uri, set(range(self.table_numkv)))

        self.reopen_conn()
        self.session.verify(uri, None)
