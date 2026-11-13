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

# [TEST_TAGS]
# checkpoint:checkpoint_cleanup
# [END_TAGS]

import wttest
from wiredtiger import stat

# test_cc07.py
# Checkpoint cleanup statistics describe the most recent cleanup pass. A
# metadata checkpoint taken for a schema change must not add to them.
class test_cc07(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),log=(enabled=false)'

    def get_stat(self, stat_id):
        cursor = self.session.open_cursor('statistics:')
        try:
            return cursor[stat_id][2]
        finally:
            cursor.close()

    def test_schema_change(self):
        uri = 'table:cc07'
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(1000):
            cursor[i] = 'value'
        cursor.close()

        self.session.checkpoint()
        duration = self.get_stat(stat.conn.cc_duration)
        handles = self.get_stat(stat.conn.cc_handle_processed)
        self.assertGreater(handles, 0)

        # Creating a table syncs the metadata with a checkpoint. That is a
        # separate, internal operation and must not be reported as cleanup of
        # the data files.
        self.session.create('table:cc07_second', 'key_format=i,value_format=S')

        self.assertEqual(self.get_stat(stat.conn.cc_handle_processed), handles,
            'checkpoint cleanup handles changed by a metadata checkpoint')
        self.assertEqual(self.get_stat(stat.conn.cc_duration), duration,
            'checkpoint cleanup duration changed by a metadata checkpoint')

if __name__ == '__main__':
    wttest.run()
