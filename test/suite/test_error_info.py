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

import unittest

# test_error_info.py
#   Test that the placeholder get_last_error() session API returns placeholder error values.
class test_error_info(wttest.WiredTigerTestCase):

    table_name1 = 'table:test_error'

    def test_uncommitted_data(self):
        self.session.create(self.table_name1, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.table_name1)
        self.session.begin_transaction()
        cursor.set_key('key')
        cursor.set_value('value')
        cursor.update()
        cursor.close()

        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.drop(self.table_name1, None))

        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, 16)
        self.assertEqual(sub_level_err, wiredtiger.WT_UNCOMMITTED_DATA)
        self.assertEqual(err_msg, "the table has uncommitted data and can not be dropped yet")

    def test_dirty_data(self):
        self.session.create('table:test_error', 'key_format=S,value_format=S')
        cursor = self.session.open_cursor('table:test_error')
        self.session.begin_transaction()
        cursor.set_key('key')
        cursor.set_value('value')
        cursor.update()
        self.session.commit_transaction()
        cursor.close()

        time.sleep(1)

        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: self.session.drop(self.table_name1, None))

        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, 16)
        self.assertEqual(sub_level_err, wiredtiger.WT_DIRTY_DATA)
        self.assertEqual(err_msg, "the table has dirty data and can not be dropped yet")

    def test_error_info(self):
        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, 0)
        self.assertEqual(sub_level_err, wiredtiger.WT_NONE)
        self.assertEqual(err_msg, "WT_NONE: No additional context")

    def test_invalid_config(self):
        gotException = False
        expectMessage = 'unknown configuration key'
        with self.expectedStderrPattern(expectMessage):
            try:
                self.pr('expect an error message...')
                self.session.create('table:' + self.table_name1,
                                    'expect_this_error,okay?')
            except wiredtiger.WiredTigerError as e:
                gotException = True
                self.pr('got expected exception: ' + str(e))
                self.assertTrue(str(e).find('nvalid argument') >= 0)
        self.assertTrue(gotException, msg = 'expected exception')

        err, sub_level_err, err_msg = self.session.get_last_error()
        self.assertEqual(err, errno.EINVAL)
        self.assertEqual(sub_level_err, wiredtiger.WT_NONE)
        self.assertEqual(err_msg, "unknown configuration key 'expect_this_error'")
