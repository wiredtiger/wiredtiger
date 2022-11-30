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
# test_count01.py
#   Test the WT_SESSION->count API in a variety of scenarios during the
#   reconciliation flow before the row count is persisted to disk.
#

import wttest
from wtdataset import ComplexDataSet, SimpleDataSet
from wtscenario import make_scenarios

class test_count01(wttest.WiredTigerTestCase):
    tablename = 'test_count01'

    format = [
        ('integer-row', dict(key_format='i', value_format='i')),
        ('string-row', dict(key_format='S', value_format='S')),
        ('col-var', dict(key_format='r', value_format='i')),
        ('string-col-var', dict(key_format='r', value_format='S')),
        ('col-fix', dict(key_format='r', value_format='8t')),
    ]

    types = [
        ('file', dict(uri='file:' + tablename, ds=SimpleDataSet)),
        ('table', dict(uri='table:' + tablename, ds=SimpleDataSet)),
    ]
    scenarios = make_scenarios(format, types)

    def create_key(self, i):
        if self.key_format == 'S':
            return str(i)
        return i

    # Test that the row count matches the number of records inserted.
    def test_count_api(self):
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(self.uri, create_params)
        c = self.session.open_cursor(self.uri, None, None)

        size='allocation_size=512,internal_page_max=512'
        ds = self.ds(self, self.uri, 3000, config=size, key_format=self.key_format, value_format=self.value_format)

        # Insert some values and persist them to disk.
        for i in range (1, 2001):
            if self.value_format == 'S':
                value = str(i)
            elif self.value_format == '8t':
                value = i % 100
            else:
                value = i

            c[self.create_key(i)] = value
        c.close()

        self.session.checkpoint()

        # When the page_stats_2022 feature flag is enabled, this assertion will be correct.
        # self.assertEqual(self.session.count(self.uri), 2000)

    # Test that the row count is correctly recorded as zero after inserting a number of records
    # and then deleting all of them.
    def test_count_api_delete_all(self):
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(self.uri, create_params)
        c = self.session.open_cursor(self.uri, None, None)

        size='allocation_size=512,internal_page_max=512'
        ds = SimpleDataSet(self, self.uri, 3000, config=size, key_format=self.key_format, value_format=self.value_format)
        for i in range (1, 2001):
            if self.value_format == 'S':
                value = str(i)
            elif self.value_format == '8t':
                value = i % 100
            else:
                value = i

            c[self.create_key(i)] = value

        for i in range (1, 2001):
            c.set_key(self.create_key(i))
            c.remove()
        c.close()

        # Persist the empty table to disk.
        self.session.checkpoint()

        # As fixed length column store may implicitly create records, don't check the
        # record count as it may not necessarily be accurate.
        # if self.value_format != '8t':
        #     self.assertEqual(self.session.count(self.uri), 0)

    def test_count_api_empty(self):
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(self.uri, create_params)
        c = self.session.open_cursor(self.uri, None, None)

        size='allocation_size=512,internal_page_max=512'
        ds = SimpleDataSet(self, self.uri, 3000, config=size, key_format=self.key_format, value_format=self.value_format)
        c.close()

        # Persist the empty table to disk.
        self.session.checkpoint()

        # As fixed length column store may implicitly create records, don't check the
        # record count as it may not necessarily be accurate.
        # When the page_stats_2022 feature flag is enabled, this assertion will be correct.
        # if self.value_format != '8t':
        #     self.assertEqual(self.session.count(self.uri), 0)

if __name__ == '__main__':
    wttest.run()
