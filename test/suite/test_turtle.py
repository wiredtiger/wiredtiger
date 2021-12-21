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

import wiredtiger, wttest
from wtscenario import make_scenarios
from helper import simulate_crash_restart

# test_turtle.py
# The following test is to validate the turtle file and to ensure it 
# contains the correct key-value pairs.

class test_turtle(wttest.WiredTigerTestCase):
    # session_config = 'isolation=snapshot'
    uri = 'table:test_turtle'
    nrows = 1000

    def init_values(self):
            self.val = 'aaaa'
            self.key_format = 'i'
            self.value_format = 'S'

    def turtle_values(self):        
        self.WT_METADATA_VERSION  = "WiredTiger version"
     
    def test_turtle(self):
        self.init_values()
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)

        self.session.create(self.uri, create_params)
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        for i in range(1, self.nrows + 1):
            cursor[i] = self.val
        self.session.commit_transaction()
        self.session.checkpoint()

        self.check_turtle()

    def check_turtle(self):
        with open('WiredTiger.turtle', 'r') as f:
            lines = f.read().splitlines()
            
            for i in range(len(lines)):
                if lines[i] == self.WT_METADATA_VERSION:
                    break

        return