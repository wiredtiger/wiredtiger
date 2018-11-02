#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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
from wtdataset import SimpleDataSet, SimpleIndexDataSet
from wtdataset import SimpleLSMDataSet, ComplexDataSet, ComplexLSMDataSet
from wtscenario import make_scenarios

# test_prepare_cursor01.py
#    WT_CURSOR position tests with prepared transactions
class test_prepare_cursor01(wttest.WiredTigerTestCase):

    keyfmt = [
        ('integer', dict(keyfmt='i')),
    ]
    types = [
        ('table-simple', dict(uri='table', ds=SimpleDataSet)),
    ]

    iso_types = [
        ('isolation_read_committed', dict(isolation='read-committed')),
        ('isolation_snapshot', dict(isolation='snapshot'))
    ]
    scenarios = make_scenarios(types, keyfmt, iso_types)

    def skip(self):
        return self.keyfmt == 'r' and \
            (self.ds.is_lsm() or self.uri == 'lsm')

    # Do an insert and confirm no key, value or position remains.
    def test_cursor_prepare_insert(self):
        if self.skip():
            return

        # Build an object.
        uri = self.uri + ':test_prepare_cursor01'
        ds = self.ds(self, uri, 50, key_format=self.keyfmt)
        ds.populate()
        session = self.conn.open_session()
        cursor = session.open_cursor(uri, None)

        prep_session = self.conn.open_session()
        prep_cursor = prep_session.open_cursor(uri, None)

        # Scenario-1 : next / prev with insert as prepared updated.
        # Begin of Scenario-1.
        # Insert key 52, by leaving gap for 51, so that prev testing can
        # use key 51.
        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(52))
        prep_cursor.set_value(ds.value(52))
        prep_cursor.insert()
        prep_session.prepare_transaction("prepare_timestamp=100")

        # Cursor points to key 50.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(50))
        self.assertEquals(cursor.search(), 0)
        # Next key will be 52, in prepared state.
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.next())
        prep_session.commit_transaction("commit_timestamp=100")

        self.assertEquals(cursor.next(), 0)
        self.assertEquals(cursor.get_key(), ds.key(52))
        session.commit_transaction()

        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(51))
        prep_cursor.set_value(ds.value(51))
        prep_cursor.insert()
        prep_session.prepare_transaction("prepare_timestamp=100")

        # Cursor points to key 52.
        # Search, so that new snapshot includes key 51.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(52))
        self.assertEquals(cursor.search(), 0)
        # Prev key will be 51, in prepared state.
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.prev())
        prep_session.commit_transaction("commit_timestamp=100")

        self.assertEquals(cursor.prev(), 0)
        self.assertEquals(cursor.get_key(), ds.key(51))
        session.commit_transaction()

        # End of Scenario-1.

        # Scenario-2 : next / prev with update as prepared updated.
        # Begin of Scenario-2.
        # Update key 52
        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(52))
        prep_cursor.set_value(ds.value(152))
        prep_cursor.update()
        prep_session.prepare_transaction("prepare_timestamp=200")

        # Cursor points to key 51.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(51))
        self.assertEquals(cursor.search(), 0)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.next())
        prep_session.commit_transaction("commit_timestamp=200")

        self.assertEquals(cursor.next(), 0)
        self.assertEquals(cursor.get_key(), ds.key(52))
        session.commit_transaction()

        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(51))
        prep_cursor.set_value(ds.value(151))
        prep_cursor.update()
        prep_session.prepare_transaction("prepare_timestamp=200")

        # Cursor points to key 52.
        # Search, so that new snapshot includes key 51.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(52))
        self.assertEquals(cursor.search(), 0)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.prev())
        prep_session.commit_transaction("commit_timestamp=200")

        self.assertEquals(cursor.prev(), 0)
        self.assertEquals(cursor.get_key(), ds.key(51))
        session.commit_transaction()

        # End of Scenario-2.

        # Scenario-3 : next / prev with remove as prepared updated.
        # Begin of Scenario-3.
        # Remove key 52
        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(52))
        prep_cursor.remove()
        prep_session.prepare_transaction("prepare_timestamp=300")

        # Cursor points to key 51.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(51))
        self.assertEquals(cursor.search(), 0)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.next())
        prep_session.commit_transaction("commit_timestamp=300")

        # Key 52 is deleted and it is the last one, so should get NOT FOUND.
        self.assertEquals(cursor.next(), wiredtiger.WT_NOTFOUND)
        session.commit_transaction()

        prep_session.begin_transaction()
        prep_cursor.set_key(ds.key(50))
        prep_cursor.remove()
        prep_session.prepare_transaction("prepare_timestamp=300")

        # Cursor points to key 51.
        # Search, so that new snapshot includes key 50, which is removed.
        session.begin_transaction('isolation=' + self.isolation)
        cursor.set_key(ds.key(51))
        self.assertEquals(cursor.search(), 0)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.prev())
        prep_session.commit_transaction("commit_timestamp=300")

        # Key 50 is deleted, cursor was pointing to 50 now so should get 49.
        self.assertEquals(cursor.prev(), 0)
        self.assertEquals(cursor.get_key(), ds.key(49))
        session.commit_transaction()

        # End of Scenario-3.

if __name__ == '__main__':
    wttest.run()
