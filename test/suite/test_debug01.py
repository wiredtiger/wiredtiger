#!/usr/bin/env python
#
# Public Domain 2014-2017 MongoDB, Inc.
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
# test_debug01.py
#   Timestamps: debug settings
#

from suite_subprocess import suite_subprocess
import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_debug01(wttest.WiredTigerTestCase, suite_subprocess):
    def test_commit_timestamp(self):
        #if not wiredtiger.timestamp_build() or not wiredtiger.diagnostic_build():
        #    self.skipTest('requires a timestamp and diagnostic build')
        if not wiredtiger.timestamp_build():
            self.skipTest('requires a timestamp build')

        base = 'debug01'
        base_uri = 'file:' + base
        uri_always = base_uri + '.always.wt'
        uri_def = base_uri + '.def.wt'
        uri_never = base_uri + '.never.wt'
        uri_none = base_uri + '.none.wt'

        cfg = 'key_format=S,value_format=S'
        cfg_always = cfg + ',debug=(commit_timestamp=always)'
        cfg_def = cfg
        cfg_never = cfg + ',debug=(commit_timestamp=never)'
        cfg_none = cfg + ',debug=(commit_timestamp=none)'

        # Create a data item at a timestamp
        self.session.create(uri_always, cfg_always)
        self.session.create(uri_def, cfg_def)
        self.session.create(uri_never, cfg_never)
        self.session.create(uri_none, cfg_none)

        # Insert a data item at timestamp 2.  This should work for all
        # except the never table.
        c_always = self.session.open_cursor(uri_always)
        c_def = self.session.open_cursor(uri_def)
        c_none = self.session.open_cursor(uri_none)
        self.session.begin_transaction()
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(1))
        c_always['key1'] = 'value1'
        c_def['key1'] = 'value1'
        c_none['key1'] = 'value1'
        self.session.commit_transaction()
        c_always.close()
        c_def.close()
        c_none.close()

        # Commit a transaction with a timestamp containing the never table.
        # This should return an error.
        msg = "/timestamp set on this transaction/"
        c_never = self.session.open_cursor(uri_never)
        self.session.begin_transaction()
        self.session.timestamp_transaction(
            'commit_timestamp=' + timestamp_str(1))
        c_never['key1'] = 'value1'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:self.assertEquals(self.session.commit_transaction(), 0), msg)
        c_never.close()

        # Insert a data item with no timestamp.  This should work for all
        # except the always table.
        c_def = self.session.open_cursor(uri_def)
        c_never = self.session.open_cursor(uri_never)
        c_none = self.session.open_cursor(uri_none)
        self.session.begin_transaction()
        c_def['key2'] = 'value2'
        c_never['key2'] = 'value2'
        c_none['key2'] = 'value2'
        self.session.commit_transaction()
        c_never.close()
        c_def.close()
        c_none.close()

        # Commit a transaction without a timestamp containing the always table.
        # This should return an error.
        msg = "/none set on this transaction/"
        c_always = self.session.open_cursor(uri_always)
        self.session.begin_transaction()
        c_always['key2'] = 'value2'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:self.assertEquals(self.session.commit_transaction(), 0), msg)
        c_always.close()

if __name__ == '__main__':
    wttest.run()
