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

import os, time, wiredtiger, wttest

StorageSource = wiredtiger.StorageSource  # easy access to constants

# test_oligarch04.py
#    Add enough content to trigger a checkpoint in the stable table.
class test_oligarch04(wttest.WiredTigerTestCase):
    nitems = 50000
    uri_base = "test_oligarch04"
    # conn_config = 'log=(enabled),verbose=[oligarch:5]'
    conn_config = 'log=(enabled),statistics=(all),statistics_log=(wait=1,json=true,on_close=true),oligarch=(role="leader")'
    # conn_config = 'log=(enabled)'

    uri = "oligarch:" + uri_base

    # Load the directory store extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', 'dir_store')

    # Test inserting a record into an oligarch tree
    def test_oligarch04(self):
        base_create = 'key_format=S,value_format=S,stable_prefix=.,storage_source=dir_store'
        os.mkdir('foo') # Hard coded to match library for now.
        os.mkdir('bar') # Hard coded to match library for now.

        self.pr("create oligarch tree")
        self.session.create(self.uri, base_create)

        self.pr('opening cursor')
        cursor = self.session.open_cursor(self.uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            if i % 10000 == 0:
                time.sleep(1)

        cursor.reset()

        self.pr('opening cursor')
        cursor.close()
        time.sleep(1)

        item_count = 0
        self.pr('read cursor saw: ' + str(item_count))
        cursor = self.session.open_cursor(self.uri, None, None)
        self.pr('read cursor saw: ' + str(item_count))
        while cursor.next() == 0:
            item_count += 1

        self.pr('read cursor saw: ' + str(item_count))
        self.assertEqual(item_count, self.nitems * 3)
        cursor.close()
