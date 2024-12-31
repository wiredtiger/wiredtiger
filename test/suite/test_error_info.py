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

import errno
import wiredtiger, wttest, time
from wtdataset import SimpleDataSet

# test_error_info.py
#   Test that the placeholder get_last_error() session API returns placeholder error values.
class test_error_info(wttest.WiredTigerTestCase):

    table_name1 = 'test_error_infoa.wt'
    table_name2 = 'test_error_infob.wt'

    def create_table(self, tablename):
        format = 'key_format=S,value_format=S'
        extra_params = ',allocation_size=512,' +\
            'internal_page_max=16384,leaf_page_max=131072'
        self.pr('create_table')
        self.session.create('table:' + tablename, format + extra_params)

    def cursor_s(self, tablename, key):
        cursor = self.session.open_cursor('table:' + tablename, None, None)
        cursor.set_key(key)
        return cursor

    def cursor_ss(self, tablename, key, val):
        cursor = self.cursor_s(tablename, key)
        cursor.set_value(val)
        return cursor

    def api_call_with_no_error(self):
        """
        Create a table, add a key, get it back
        """
        self.create_table(self.table_name2)
        inscursor = self.cursor_ss(self.table_name2, 'key1', 'value1')
        inscursor.insert()
        inscursor.close()
        getcursor = self.cursor_s(self.table_name2, 'key1')
        getcursor.search()
        getcursor.close()

    def api_call_with_error(self):
        expectMessage = 'unknown configuration key'
        with self.expectedStderrPattern(expectMessage):
            try:
                self.session.create('table:' + self.table_name1, 'expect_this_error,okay?')
            except wiredtiger.WiredTigerError as e:
                self.assertTrue(str(e).find('nvalid argument') >= 0)

    def assert_error_equal(self, err_val, sub_level_err_val, err_msg_val):
        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, err_val)
        self.assertEqual(sub_level_err, sub_level_err_val)
        self.assertEqual(err_msg, err_msg_val)

    def test_api_call_with_EINVAL(self):
        self.assert_error_equal(0, wiredtiger.WT_NONE, "")
        self.api_call_with_error()
        self.assert_error_equal(errno.EINVAL, wiredtiger.WT_NONE, "unknown configuration key 'expect_this_error'")

    def test_api_call_with_no_error(self):
        self.assert_error_equal(0, wiredtiger.WT_NONE, "")
        self.api_call_with_no_error()
        self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")

    def test_api_call_alternating(self):
        self.assert_error_equal(0, wiredtiger.WT_NONE, "")
        self.api_call_with_no_error()
        self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")
        self.api_call_with_error()
        self.assert_error_equal(errno.EINVAL, wiredtiger.WT_NONE, "unknown configuration key 'expect_this_error'")
        self.api_call_with_no_error()
        self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")
        self.api_call_with_error()
        self.assert_error_equal(errno.EINVAL, wiredtiger.WT_NONE, "unknown configuration key 'expect_this_error'")
        self.api_call_with_no_error()
        self.assert_error_equal(0, wiredtiger.WT_NONE, "last API call was successful")
