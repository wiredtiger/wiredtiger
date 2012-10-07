#!/usr/bin/env python
#
# Public Domain 2008-2012 WiredTiger, Inc.
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
from helper import key_populate, value_populate

# A standalone test case that exercises overflow address-deleted cells.
class test_ovfl_delete(wttest.WiredTigerTestCase):
    # This is a btree layer test, test files, ignore tables.
    uri = 'file:test_ovfl_del'

    # Use a small page size because we want to create overflow items
    config = 'leaf_page_max=512,value_format=S,key_format='

    # Test row-store and variable-length column-store.
    scenarios = [
        ('recno', dict(keyfmt='r')),
        ('string', dict(keyfmt='S')),
        ]

    nentries = 1000

    # test_ovfl_deleted routine:
    #   Create an object with lots of address-deleted cells on disk.
    #   Recover the object, and discard the address-deleted cells.
    def test_ovfl_deleted(self):
        # Create the object
        self.session.create(self.uri, self.config + self.keyfmt)
        cursor = self.session.open_cursor(self.uri, None)
        for i in range(1, self.nentries):
            cursor.set_key(key_populate(cursor, i))
            cursor.set_value(value_populate(cursor, i) + 'abcdef' * 100)
            cursor.insert()
        cursor.close()

        # Verify the object, force it to disk, and verify the on-disk version.
        self.session.verify(self.uri)
        self.reopen_conn()
        self.session.verify(self.uri)

        # Create a new session and start a transaction to force the upcoming
        # checkpoint operation to write address-deleted cells to disk.
        tmp_session = self.conn.open_session(None)
        tmp_session.begin_transaction("isolation=snapshot")

        # Update a bunch of entries.
        cursor = self.session.open_cursor(self.uri, None)
        for i in range(1, self.nentries, 17):
            cursor.set_key(key_populate(cursor, i))
            cursor.set_value(value_populate(cursor, i) + 'update')
            cursor.update()
        cursor.close()

        # Checkpoint, forcing address-deleted cells to be written.
        self.session.checkpoint()

        # Crash/reopen the connection and verify the object.
        self.reopen_conn()
        self.session.verify(self.uri)

        # Open a cursor and update a record (to dirty the tree, else we won't
        # mark pages with address-deleted cells dirty), then walk the tree so
        # we get a good look at all the leaf pages and address-deleted cells.
        cursor = self.session.open_cursor(self.uri, None)
        cursor.set_key(key_populate(cursor, 5))
        cursor.set_value("changed value")
        cursor.update()
        cursor.reset()
        for key,val in cursor:
            continue
        cursor.close()

        # Checkpoint, freeing the overflow chunks.
        self.session.checkpoint()

        # Force the object to disk and verify the on-disk version.
        self.reopen_conn()
        self.session.verify(self.uri)


if __name__ == '__main__':
    wttest.run()
