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

import os, re, struct
import wiredtiger, wttest
from helper import WiredTigerCursor
from wtscenario import make_scenarios

# test_verify.py
#    Utilities: wt verify
class test_verify(wttest.WiredTigerTestCase):

    uri = 'table:verify03'

    conn_config = 'log=(enabled=true),statistics=(all),statistics_log=(json,wait=1,on_close=true,sources=[file:])'

    format = [
        ('flat', dict(is_fault=False)),
        ('json', dict(is_fault=True)),
    ]
    scenarios = make_scenarios(format)

    def test_duplicate_logs(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.session.begin_transaction()
        with WiredTigerCursor(self.session, self.uri) as c:
            for i in range(1000):
                c[f'key_{i}'] = f'val_{i}'
        self.session.commit_transaction()
        
        self.reopen_conn()
        if self.is_fault:
            os.environ["WT_INJECT_CHCK_SUM_FAULT"] = "1"
        self.reopen_conn()

        print('hi')