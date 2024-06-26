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
# test_search_near03.py
#       Test search_near's behaviour with prepared updates

import wttest
from wiredtiger import WiredTigerError
from wtscenario import make_scenarios

class test_search_near03(wttest.WiredTigerTestCase):
    uri = 'file:test_search_near03'

    key_format_values = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('var', dict(key_format='r', value_format='I')),
        ('row', dict(key_format='Q', value_format='I')),
    ]

    scenarios = make_scenarios(key_format_values)

    def test_prepared_conflict_exact_match(self):
        self.session.create(self.uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = 1
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(1))

        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.uri)
        session2.begin_transaction()
        cursor2.set_key(1)
        self.assertRaisesException(WiredTigerError,
            lambda: cursor2.search_near(),
            exceptionString='/conflict with a prepared update/')

    def test_prepared_conflict_both_sides(self):
        # FLCS returns implict record
        if self.value_format == '8t':
            return

        self.session.create(self.uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = 1
        cursor[3] = 1
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(1))

        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.uri)
        session2.begin_transaction()
        cursor2.set_key(2)
        self.assertRaisesException(WiredTigerError,
            lambda: cursor2.search_near(),
            exceptionString='/conflict with a prepared update/')

    def test_prepared_conflict_left_side(self):
        # FLCS returns implict record
        if self.value_format == '8t':
            return

        self.session.create(self.uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = 1
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(1))

        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.uri)
        session2.begin_transaction()
        cursor2[3] = 1
        session2.commit_transaction()

        session2.begin_transaction()
        cursor2.set_key(2)
        cursor2.search_near()
        self.assertEqual(cursor2.get_key(), 3)

    def test_prepared_conflict_right_side(self):
        # FLCS returns implict record
        if self.value_format == '8t':
            return

        self.session.create(self.uri, 'key_format={},value_format={}'.format(self.key_format, self.value_format))
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[3] = 1
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(1))

        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(self.uri)
        session2.begin_transaction()
        cursor2[1] = 1
        session2.commit_transaction()

        session2.begin_transaction()
        cursor2.set_key(2)
        cursor2.search_near()
        self.assertEqual(cursor2.get_key(), 1)
