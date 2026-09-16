#!/usr/bin/env python3
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

import wiredtiger, wttest

# test_cursor_remove_stale_position_col.py
#   WT_CURSOR.remove on a cursor that's already positioned (rather than freshly searched) must
#   still return not-found for a record that's already gone, the same as a remove that searches
#   first -- whether "already gone" means the record never had a value on the page at all, or an
#   earlier, already-visible remove left only a real value with a stop time window behind.
class test_cursor_remove_stale_position_col(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor_remove_stale_position_col'

    def create_and_populate(self):
        self.session.create(self.uri, 'key_format=r,value_format=S')
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, 11):
            c[i] = 'value%d' % i
        self.session.commit_transaction()
        return c

    def evict(self, key):
        """Force the page holding key back out of cache."""
        evict_cursor = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        evict_cursor.set_key(key)
        self.assertEqual(evict_cursor.search(), 0)
        evict_cursor.reset()
        evict_cursor.close()

    def check_remove_on_stale_position_rejected(self, key):
        """
        Position a cursor on key via an uncommitted value only visible at read-uncommitted, then
        remove through that same cursor after the value disappears without ever repositioning it.
        The remove must return not-found rather than silently creating a tombstone.
        """
        # Session A writes a phantom value into the record but never commits it.
        sessionA = self.conn.open_session()
        cA = sessionA.open_cursor(self.uri)
        sessionA.begin_transaction()
        cA.set_key(key)
        cA.set_value('phantom')
        cA.update()

        # Session B, reading uncommitted outside any explicit transaction, positions on that same,
        # still-uncommitted value. The cursor stays positioned on the record from here on.
        sessionB = self.conn.open_session()
        sessionB.reconfigure('isolation=read-uncommitted')
        cB = sessionB.open_cursor(self.uri)
        cB.set_key(key)
        self.assertEqual(cB.search(), 0)

        # Session A rolls back, so the value B just saw no longer exists anywhere.
        sessionA.rollback_transaction()

        # B removes through the same still-positioned cursor, without a set_key in between. Despite
        # the stale position, this must behave exactly like a remove that searches first: the
        # record is already gone, so it returns not-found rather than creating a spurious tombstone.
        sessionB.begin_transaction('isolation=snapshot')
        self.assertEqual(cB.remove(), wiredtiger.WT_NOTFOUND)
        sessionB.rollback_transaction()

        # Reconciling the page must not find anything wrong: nothing was ever removed.
        self.session.checkpoint()

    def test_remove_already_positioned_on_nonexistent_record(self):
        c = self.create_and_populate()

        # Remove the record and checkpoint with nothing holding the remove back from global
        # visibility, so its on-disk cell becomes an explicit "deleted" placeholder rather than a
        # real value.
        self.session.begin_transaction()
        c.set_key(5)
        c.remove()
        self.session.commit_transaction()
        c.close()
        self.session.checkpoint()

        # Force the page back out of cache so record 5 is backed only by that on-disk placeholder,
        # with no leftover in-memory update.
        self.evict(1)

        self.check_remove_on_stale_position_rejected(5)

    def test_remove_already_positioned_on_already_deleted_value(self):
        c = self.create_and_populate()

        # Hold a transaction open with an old snapshot before the real remove below, so that when
        # we checkpoint, the remove isn't yet globally visible and the on-page cell keeps the
        # original value with a stop time window rather than collapsing to a "deleted" placeholder.
        sessionOld = self.conn.open_session()
        sessionOld.begin_transaction()

        self.session.begin_transaction()
        c.set_key(5)
        c.remove()
        self.session.commit_transaction()
        c.close()
        self.session.checkpoint()

        # Force the page back out of cache: only the on-page cell (value5 with an embedded stop
        # time window) is left, no in-memory update.
        self.evict(1)

        self.check_remove_on_stale_position_rejected(5)

        sessionOld.rollback_transaction()
