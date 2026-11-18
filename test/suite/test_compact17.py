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
# Exercise block allocation and compaction with a workload intended to leave reusable space.
#
# First-fit allocation returns the lowest-offset extent that is large enough for a request. It is
# used by files configured with block_allocation=first, and temporarily during checkpoint and
# compaction. When the lowest-offset extent is too small, the lookup compares size buckets.
#
# These are end-to-end checks: they run both allocation policies over the same workload and
# check that the table contents read back correctly and that the file still verifies. They do not
# identify where individual blocks were written. The lowest-offset selection is checked by the
# block_first_srch unit tests in test_extent_list.cpp.
class test_compact17(compact_util):
    # Small leaf pages make it easier for the deletes below to leave reusable space in smaller
    # extents. The exact extent layout depends on how blocks are allocated and merged.
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=4KB,'
    conn_config = 'cache_size=100MB,statistics=(all)'

    allocation = [
        ('first-fit', dict(block_allocation='first')),
        ('best-fit', dict(block_allocation='best')),
    ]
    scenarios = make_scenarios(allocation)

    table_numkv = 20 * 1000
    value_size = 1024

    # On the first pass, compaction skips a file with less available space than
    # "free_space_target", which defaults to 20MB. Use a smaller target so this workload does not
    # depend on reaching the default threshold.
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

    def check_reusable_space(self, uri, label):
        # Guard against a workload change that leaves too little reusable space for the test to be
        # meaningful. This checks only the total amount: it does not establish where the space
        # sits, how many extents it is split into, or whether a lookup bypasses the size buckets.
        # Use the same floor as the compaction tests, though exceeding it does not guarantee that
        # compaction runs. The unit tests cover the lookup itself.
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
        # Delete runs of varying lengths, leaving keys in place between them. This is meant to
        # leave reusable space in more than one extent, but key order does not determine the
        # physical block layout, so the resulting extent count and sizes are not fixed.
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
        # Rewrite deleted keys after leaving reusable space in the file.
        uri = 'table:test_compact17_lookup'
        self.create_table(uri)

        # An empty table has nothing available for reuse.
        self.session.checkpoint()
        self.assertEqual(self.get_bytes_avail_for_reuse(uri), 0)

        self.populate(uri, 0, self.table_numkv, value_size=self.value_size)
        self.session.checkpoint()

        deleted = self.delete_runs(uri, 400, 60)
        self.session.checkpoint()

        # Make sure the deletes left enough reusable space for the rewrite below.
        self.check_reusable_space(uri, 'after delete')

        self.check_contents(uri, set(range(self.table_numkv)) - deleted)

        # Rewrite the deleted keys while reusable space is available. This does not assert where
        # any allocation came from, only that the data written back is intact.
        cursor = self.session.open_cursor(uri, None)
        for key in sorted(deleted):
            cursor[key] = self.expected_value(key)
        cursor.close()
        self.session.checkpoint()

        # Every key must read back with the value written to it.
        self.check_contents(uri, set(range(self.table_numkv)))

        # Reopen the connection to clear cached state before verifying the file.
        self.reopen_conn()
        self.session.verify(uri, None)
        self.check_contents(uri, set(range(self.table_numkv)))

    def test_compaction_relocates_pages(self):
        # Compaction switches the file to first-fit while it runs. Leave the deleted low-key range
        # empty so the workload has reusable space for compaction to work with.
        uri = 'table:test_compact17_compaction'
        self.create_table(uri)

        self.populate(uri, 0, self.table_numkv, value_size=self.value_size)
        self.session.checkpoint()

        deleted = 80 * self.table_numkv // 100
        self.delete_range(uri, deleted)
        self.session.checkpoint()

        # The check below is only a floor on the total: it does not establish where the space
        # sits.
        self.check_reusable_space(uri, 'after delete')

        size_before = self.get_size(uri)
        self.session.compact(uri, self.compact_config)
        self.session.checkpoint()

        # Fail if compaction did not review and rewrite pages, so the test does not pass without
        # exercising compaction.
        progress = self.get_compact_progress_stats(uri)
        self.assertGreater(progress['pages_reviewed'], 0, progress)
        self.assertGreater(progress['pages_rewritten'], 0, progress)

        # Check that this workload reclaims space, rather than only rewriting pages.
        self.assertLess(self.get_size(uri), size_before)

        # Relocating pages must not change what the table holds.
        self.check_contents(uri, set(range(deleted, self.table_numkv)))

        # Reopen the connection to clear cached state before verifying the file.
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

        self.check_reusable_space(uri, 'before reopen')

        self.reopen_conn()

        # Check that reusable space remains available after reopening.
        self.check_reusable_space(uri, 'after reopen')
        self.check_contents(uri, set(range(self.table_numkv)) - deleted)

        # Rewrite the deleted keys after reopening and check that the data is intact.
        cursor = self.session.open_cursor(uri, None)
        for key in sorted(deleted):
            cursor[key] = self.expected_value(key)
        cursor.close()
        self.session.checkpoint()

        self.check_contents(uri, set(range(self.table_numkv)))

        self.reopen_conn()
        self.session.verify(uri, None)
