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
# test_cursor07.py
# Log cursors
#

import fnmatch, os, shutil, run, time
from suite_subprocess import suite_subprocess
from wiredtiger import stat
from wtscenario import make_scenarios
import wttest

class test_cursor07(wttest.WiredTigerTestCase, suite_subprocess):
    logmax = "100K"
    tablename1 = 'test_cursor07_log'
    tablename2 = 'test_cursor07_nolog'
    tablename3 = 'test_cursor07_nologtxn'
    uri1 = 'table:' + tablename1
    uri2 = 'table:' + tablename2
    uri3 = 'table:' + tablename3
    #  A large number of keys will force a log file change which will
    # test that scenario for log cursors.
    nkeys = 7000

    scenarios = make_scenarios([
        ('regular', dict(reopen=False)),
        ('reopen', dict(reopen=True))
    ])
    # Enable logging for this test.
    def conn_config(self):
        return 'log=(archive=false,enabled,file_max=%s),' % self.logmax + \
            'transaction_sync="(method=dsync,enabled)"'

    def test_log_cursor(self):
        # print "Creating %s with config '%s'" % (self.uri, self.create_params)
        create_params = 'key_format=i,value_format=S'
        create_nolog_params = 'key_format=i,value_format=S,log=(enabled=false)'
        self.session.create(self.uri1, create_params)
        c1 = self.session.open_cursor(self.uri1, None)
        self.session.create(self.uri2, create_nolog_params)
        c2 = self.session.open_cursor(self.uri2, None)
        self.session.create(self.uri3, create_nolog_params)
        c3 = self.session.open_cursor(self.uri3, None)

        # A binary value.
        value = u'\u0001\u0002abcd\u0003\u0004'
        value_nolog = u'\u0001\u0002dcba\u0003\u0004'

        # We want to test both adding data to a table that is not logged
        # that is part of the same transaction as a table that is logged
        # as well as in its own transaction.
        self.session.begin_transaction()
        for k in range(self.nkeys):
            c1[k] = value
            c3[k] = value_nolog
        self.session.commit_transaction()
        c1.close()
        c3.close()

        self.session.begin_transaction()
        for k in range(self.nkeys):
            c2[k] = value_nolog
        self.session.commit_transaction()
        c2.close()

        if self.reopen:
            self.reopen_conn()

        # Check for these values via a log cursor
        c = self.session.open_cursor("log:", None)
        count = 0
        while c.next() == 0:
            # lsn.file, lsn.offset, opcount
            keys = c.get_key()
            # txnid, rectype, optype, fileid, logrec_key, logrec_value
            values = c.get_value()
            try:
                if value in str(values[5]):   # logrec_value
                    count += 1
                self.assertFalse(value2 in str(values[5]))
            except:
                pass
        c.close()
        self.assertEqual(count, self.nkeys)

if __name__ == '__main__':
    wttest.run()
