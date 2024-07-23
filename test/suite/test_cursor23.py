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
# test_cursor23.py
#   Test cursor get_raw_key_value using complex schema

import wiredtiger, wttest
from wtdataset import SimpleDataSet, SimpleLSMDataSet
from wtscenario import make_scenarios

class test_cursor23(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('file-S', dict(type='file:', keyfmt='S', valfmt='S', dataset=SimpleDataSet)),
        ('lsm-S', dict(type='lsm:', keyfmt='S', valfmt='S', dataset=SimpleDataSet)),
        ('table-S', dict(type='table:', keyfmt='S', valfmt='S', dataset=SimpleDataSet)),
        ('table-S-lsm', dict(type='table:', keyfmt='S', valfmt='S', dataset=SimpleLSMDataSet)),
    ])

    def check_get_key_and_value(self, cursor, expected_key, expected_value):
        key = cursor.get_key()
        value = cursor.get_value()
        self.assertEquals(key, expected_key)
        self.assertEquals(value, expected_value)

    def check_get_raw_key_value(self, cursor, expected_key, expected_value):
        (key, value) = cursor.get_raw_key_value()
        self.assertEquals(key, expected_key)
        self.assertEquals(value, expected_value)

    def test_cursor23(self):
        uri = self.type + "test_cursor23"
        ds = self.dataset(self, uri, 100, key_format=self.keyfmt, value_format=self.valfmt)
        ds.populate()

        cursor = self.session.open_cursor(uri)

        # Check the data using get_key() and get_value()
        self.session.begin_transaction()
        cursor.reset()
        for i in range(1, 10):
            cursor.next()
            self.check_get_key_and_value(cursor=cursor, expected_key=f'{i:015d}'.format(i), expected_value=f'{i}: abcdefghijklmnopqrstuvwxyz'.format(i))
        self.session.commit_transaction()

        # Check the data using get_raw_key_and_value()
        self.session.begin_transaction()
        cursor.reset()
        for i in range(1, 10):
            cursor.next()
            self.check_get_raw_key_value(cursor=cursor, expected_key=f'{i:015d}'.format(i), expected_value=f'{i}: abcdefghijklmnopqrstuvwxyz'.format(i))
        self.session.commit_transaction()

        cursor.close()
        self.session.close()
