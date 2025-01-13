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

    table_name1 = 'table:test_error_infoa.wt'
    table_name2 = 'table:test_error_infob.wt'
    table_name3 = 'table:test_error_infoc.wt'

    repeat_call = False
    ERROR_INFO_EMPTY = ""
    ERROR_INFO_SUCCESS = "last API call was successful"
    EINVAL_message = "unknown configuration key 'expect_this_error'"

    def cursor_s(self, tablename, key):
        cursor = self.session.open_cursor(tablename, None, None)
        cursor.set_key(key)
        return cursor

    def api_call_with_success(self):
        """
        Create a table, add a key, get it back
        """
        self.session.create(self.table_name1, 'key_format=S,value_format=S')
        inscursor = self.cursor_s(self.table_name1, 'key1')
        inscursor.set_value('value1')
        inscursor.insert()
        inscursor.close()
        getcursor = self.cursor_s(self.table_name1, 'key1')
        getcursor.search()
        getcursor.close()

    def api_call_with_EINVAL(self):
        expectMessage = 'unknown configuration key'
        with self.expectedStderrPattern(expectMessage):
            try:
                self.session.create(self.table_name2, 'expect_this_error,okay?')
            except wiredtiger.WiredTigerError as e:
                self.assertTrue(str(e).find('nvalid argument') >= 0)

    def api_call_with_EBUSY(self):
        if self.repeat_call:
            self.session.rollback_transaction()
            self.session.checkpoint()
            self.session.drop(self.table_name3, None)
        else:
            self.repeat_call = True
        self.session.create(self.table_name3, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.table_name3)
        self.session.begin_transaction()
        cursor.set_key('key')
        cursor.set_value('value')
        self.assertEqual(cursor.update(), 0)
        cursor.close()
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.drop(self.table_name3, None))

    def assert_error_equal(self, err_val, sub_level_err_val, err_msg_val):
        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, err_val)
        self.assertEqual(sub_level_err, sub_level_err_val)
        self.assertEqual(err_msg, err_msg_val)

    def test_api_call_with_EINVAL(self):
        self.api_call_with_EINVAL()
        self.assert_error_equal(errno.EINVAL, wiredtiger.WT_NONE, self.EINVAL_message)

    def test_api_call_with_EBUSY(self):
        self.api_call_with_EBUSY()
        self.assert_error_equal(errno.EBUSY, wiredtiger.WT_NONE, self.ERROR_INFO_EMPTY)

    def test_api_call_with_success(self):
        self.api_call_with_success()
        self.assert_error_equal(0, wiredtiger.WT_NONE, self.ERROR_INFO_SUCCESS)

    def test_api_call_alternating(self):
        self.assert_error_equal(0, wiredtiger.WT_NONE, self.ERROR_INFO_EMPTY)
        self.test_api_call_with_success()
        self.test_api_call_with_EINVAL()
        self.test_api_call_with_EBUSY()
        self.test_api_call_with_success()
        self.test_api_call_with_EBUSY()
        self.test_api_call_with_EINVAL()
        self.test_api_call_with_success()

    def test_api_call_doubling(self):
        self.assert_error_equal(0, wiredtiger.WT_NONE, self.ERROR_INFO_EMPTY)
        self.test_api_call_with_success()
        self.test_api_call_with_success()
        self.test_api_call_with_EINVAL()
        self.test_api_call_with_EINVAL()
        self.test_api_call_with_EBUSY()
        self.test_api_call_with_EBUSY()
