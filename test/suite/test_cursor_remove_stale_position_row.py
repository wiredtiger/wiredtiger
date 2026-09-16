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

# test_cursor_remove_stale_position_row.py
#   WT_CURSOR.remove on a cursor that's already positioned (rather than freshly searched) must
#   still return not-found for a key that's already deleted, the same as a remove that searches
#   first -- even when "already deleted" is represented as an on-page value with a stop time
#   window rather than an on-page cell with no value at all.
class test_cursor_remove_stale_position_row(wttest.WiredTigerTestCase):
    uri = 'table:test_cursor_remove_stale_position_row'

    def test_remove_already_positioned_on_already_deleted_row(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        c['k5'] = 'value5'
        self.session.commit_transaction()

        # Hold a transaction open with an old snapshot before the real remove below, so that when
        # we checkpoint, the remove isn't yet globally visible and the on-page cell keeps the
        # original value with a stop time window rather than being omitted outright.
        sessionOld = self.conn.open_session()
        sessionOld.begin_transaction()

        self.session.begin_transaction()
        c.set_key('k5')
        c.remove()
        self.session.commit_transaction()
        c.close()
        self.session.checkpoint()

        # Force the page back out of cache: only the on-page cell (value5 with an embedded stop
        # time window) is left, no in-memory update.
        evict_cursor = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        evict_cursor.set_key('k5')
        self.assertEqual(evict_cursor.search(), wiredtiger.WT_NOTFOUND)
        evict_cursor.reset()
        evict_cursor.close()

        # Session A writes a phantom value into the row's update slot but never commits it.
        sessionA = self.conn.open_session()
        cA = sessionA.open_cursor(self.uri)
        sessionA.begin_transaction()
        cA.set_key('k5')
        cA.set_value('phantom')
        cA.update()

        # Session B, reading uncommitted outside any explicit transaction, positions on that same,
        # still-uncommitted value. The cursor stays positioned on the record from here on.
        sessionB = self.conn.open_session()
        sessionB.reconfigure('isolation=read-uncommitted')
        cB = sessionB.open_cursor(self.uri)
        cB.set_key('k5')
        self.assertEqual(cB.search(), 0)

        # Session A rolls back, so the value B just saw no longer exists anywhere -- the key was
        # already deleted before A ever touched it, and A's own attempt just rolled back too.
        sessionA.rollback_transaction()

        # B removes through the same still-positioned cursor, without a set_key in between. Despite
        # the stale position, this must behave exactly like a remove that searches first: the key
        # is already deleted, so it returns not-found rather than stacking a second, illegitimate
        # delete marker on top of the one already on the page.
        sessionB.begin_transaction('isolation=snapshot')
        self.assertEqual(cB.remove(), wiredtiger.WT_NOTFOUND)
        sessionB.rollback_transaction()

        # Reconciling the page must not find anything wrong: nothing was ever removed a second time.
        self.session.checkpoint()

        sessionOld.rollback_transaction()
