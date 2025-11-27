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

from suite_subprocess import suite_subprocess
import wttest

# test_hs33.py
# Test that we can reproduce the key exist in historical only, and lead to a verify failed.
# Timeline Chart (A:1 -> A value at timestamp 1) :
# |        |   t1  |  t10   |  t20  |   t28     |  t30      |
# | OP     | Ins A |  Upd A |       |  Del A    |           |
# | DS     |   -   |  A:10  |  A:10 | A:10(Tomb)|   -       |
# | HS     |   -   |   -    |   -   | A:(10-20) | A:(10-20) |
# | Stable |   1   |   1    |   20  |    20     |  30       |
# | Oldest |   1   |   1    |   1   |    1      |  30       |
class test_hs34(wttest.WiredTigerTestCase, suite_subprocess):

    def test_hs_recovery(self):
        # Pin oldest and stable timestamp to 1.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))

        # Create a table.
        uri = 'table:tablehs34'
        self.session.create(uri,"key_format=S,value_format=S")

        # Insert
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor["a"] = "a"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(10))

        self.session.begin_transaction()
        cursor["a"] = "b"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(20))

        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        # Force eviction.
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        self.session.begin_transaction("ignore_prepare=true")
        evict_cursor.set_key("a")
        evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        self.session.rollback_transaction()

        # Move data to HS and everything is clean.
        self.session.checkpoint()

        # Do the delete.
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor.set_key("a")
        cursor.remove()
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(28))
        cursor.close()

        # Set the oldest timestamp to 30, and this will make the deletion of a globally visible.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(30) +
            ',stable_timestamp=' + self.timestamp_str(30))

        # Force eviction to key of a, because it's globally visible, then it will be safely removed
        # from the data store (DS). But the history store (HS) still has the record.
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        self.session.begin_transaction("ignore_prepare=true")
        evict_cursor.set_key("a")
        evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        self.session.rollback_transaction()

        self.session.checkpoint()

        # Close the connection without timestamps, oldest timestamp will not be stored in the metadata.
        self.close_conn('use_timestamp=false')

        # Run wt tool verify command, and this will report:
        # The key a in the history store but not in the data store.
        self.runWt(['-r', '-m', 'verify'],
            outfilename='verify.out', errfilename="verify.err", failure=True, reopensession=False)
        self.check_empty_file('verify.out')
        self.check_file_contains('verify.err',
            'the associated history store key a was not found in the data store file:tablehs34.wt')