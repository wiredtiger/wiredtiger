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

import time
import wttest
from wiredtiger import stat

# Regression test for WT-18427: a non-forced WT_SESSION::drop of an already-clean (checkpointed)
# table used to close its dhandle by walking and freeing every page resident in cache,
# synchronously, while holding the schema and dhandle-list write locks. The fix marks a clean
# tree's dhandle dead instead and defers the cache discard to the sweep server, so the drop call
# itself never fully closes the handle.
#
# A clean tree has nothing dirty to flush either way, so comparing the backing file's bytes before
# and after the drop cannot tell the two behaviors apart: neither path writes to the file. The
# property that does tell them apart is whether the file's own dhandle is torn down as part of the
# drop call. Dropping a simple table closes two dhandles: the table-layer one (always closed
# synchronously, its type isn't subject to the clean/dirty distinction the fix makes) and the
# underlying file/btree one (the one the fix defers to sweep when the tree is clean). So the
# "btrees currently open" statistic drops by 1 across a clean-tree drop with the fix applied, and
# by 2 without it. Checking it immediately after drop() returns needs no sleep or polling: the call
# has already completed, so whatever it was going to do to that counter has already happened.
class test_drop_cache_discard01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=1G,statistics=(all)'

    value = 'a' * 200

    def populate(self, uri, rows):
        self.session.create(uri, 'key_format=Q,value_format=S')
        cursor = self.session.open_cursor(uri, None, None)
        for i in range(rows):
            cursor[i] = self.value
        cursor.close()

    def btree_open(self):
        cursor = self.session.open_cursor('statistics:', None, None)
        value = cursor[stat.conn.btree_open][2]
        cursor.close()
        return value

    def test_clean_drop_defers_handle_close(self):
        """
        A non-forced drop of an already-clean table must not tear down its file/btree handle as
        part of the drop call: that handle is marked dead and left for sweep, so btree_open drops
        by exactly 1 (the table-layer handle only) immediately after the drop returns, not by 2.
        """
        uri = 'table:test_drop_cache_discard01_clean'
        self.populate(uri, 10_000)

        # Give the oldest id time to catch up: otherwise checkpoint's own reconciliation can find
        # the last insert not yet globally visible, skip it, and leave the tree modified again
        # right after marking it clean.
        time.sleep(1)
        self.session.checkpoint()

        btree_open_before = self.btree_open()
        self.session.drop(uri, None)
        btree_open_after = self.btree_open()

        self.assertEqual(btree_open_after, btree_open_before - 1,
            f'btree_open changed by {btree_open_after - btree_open_before} across a clean-tree '
            f'drop ({btree_open_before} before, {btree_open_after} after) -- expected exactly -1: '
            'the file/btree handle should have been marked dead and deferred to sweep, not closed '
            'synchronously')

    def test_busy_checkpoint_handle_leaves_live_handle_usable(self):
        """
        Dropping a table locks the live handle before any checkpoint handles for the same URI,
        then closes the whole set only once every handle in it is confirmed lockable. If a
        checkpoint handle turns out to be busy, the live handle -- already locked by that point --
        must not have been closed or marked dead: that step is irreversible, so committing it
        before the rest of the set is confirmed available would strand a dead handle behind a drop
        that reports failure. Check this directly: drop must fail EBUSY while a checkpoint cursor
        is open, and the table must still be fully open and usable afterward.
        """
        uri = 'table:test_drop_cache_discard01_busy_checkpoint'
        self.populate(uri, 100)
        time.sleep(1)
        self.session.checkpoint('name=wt18427ckpt')

        # Open a checkpoint cursor from a second session so it stays open across the drop attempt.
        session2 = self.conn.open_session()
        ckpt_cursor = session2.open_cursor(uri, None, 'checkpoint=wt18427ckpt')

        self.assertTrue(self.raisesBusy(lambda: self.session.drop(uri, None)),
            'expected drop to fail with EBUSY while a checkpoint cursor is open')

        # Nothing was destroyed: the live table is still fully open and usable.
        cursor = self.session.open_cursor(uri, None, None)
        cursor.set_key(0)
        self.assertEqual(cursor.search(), 0)
        cursor.close()

        ckpt_cursor.close()
        session2.close()

        # With the checkpoint handle free, the drop succeeds.
        self.session.drop(uri, None)

    def test_dirty_drop_still_fails(self):
        """
        A non-forced drop of a table with committed but uncheckpointed content must still fail
        with EBUSY: the fix only changes what happens to an already-clean tree.
        """
        uri = 'table:test_drop_cache_discard01_dirty'
        self.populate(uri, 1)

        self.assertTrue(self.raisesBusy(lambda: self.session.drop(uri, None)),
            'expected drop to fail with EBUSY on a dirty (uncheckpointed) table')

if __name__ == '__main__':
    wttest.run()
