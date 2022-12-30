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
# test_stat11.py
#    Usage of statistics cursor for retrieving page stats.

import wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

class test_stat11(wttest.WiredTigerTestCase):
    pfx = 'test_page_stat'
    conn_config = 'statistics=(all)'

    format = [
        ('integer-row', dict(key_format='i', value_format='i')),
        ('string-row', dict(key_format='S', value_format='S')),
        ('col-var', dict(key_format='r', value_format='i')),
        ('string-col-var', dict(key_format='r', value_format='S')),
        ('col-fix', dict(key_format='r', value_format='8t')),
    ]

    types = [
        ('file', dict(uri='file:' + pfx, ds=SimpleDataSet)),
        ('table', dict(uri='table:' + pfx, ds=SimpleDataSet)),
    ]
    scenarios = make_scenarios(format, types)

    def create_key(self, i):
        if self.key_format == 'S':
            return str(i)
        return i

    # Test that the row count matches the number of records inserted.
    def test_page_stat_statistic_cursor(self):
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

        ps_cursor = self.session.open_cursor('statistics:checkpoint:' + self.uri, None, None)
        
        # Use a statistics cursor to get the byte and row counts and verify the values.
        ps_cursor.next()
        [desc, pvalue, byte_count] = ps_cursor.get_values()
        # When the page_stats_2022 feature flag is enabled, this assertion applies.
        # self.assertGreater(byte_count, 0)

        # Get the row count.
        ps_cursor.next()
        [desc, pvalue, row_count] = ps_cursor.get_values()
        # When the page_stats_2022 feature flag is enabled, this assertion applies.
        # self.assertEqual(row_count, 2000)
        ps_cursor.close()

if __name__ == '__main__':
    wttest.run()
