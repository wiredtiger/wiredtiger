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

import re, struct
import wttest
from helper import WiredTigerCursor

# test_verify.py
#    Utilities: wt verify
class test_verify(wttest.WiredTigerTestCase):

    uri = 'table:verify03'

    conn_config = 'log=(enabled=true),statistics=(all),statistics_log=(json,wait=1,on_close=true,sources=[file:])'

    WT_TURTLE_FILE_NAME = "WiredTiger.turtle"
    WT_LOG_FILE = "WiredTigerLog.0000000%03d"

    def extract_key_from_turtle(self, line, key):
        m = re.search(key + r'=\(\s*(\d+)\s*,\s*(\d+)\s*\)', line)
        if not m:
            return None
        file_num, offset = map(int, m.groups())
        return (file_num, offset)

    def inject_faulty_to_log(self, id):
        # Read checkpoint lsn
        with open(self.WT_TURTLE_FILE_NAME, 'r') as f:
            lines = f.read().splitlines()
            self.turtle_file = lines
        for line in lines:
            value = self.extract_key_from_turtle(line, 'checkpoint_lsn')
            if value is not None:
                break
        self.assertTrue(value is not None, "Checkpoint lsn is missing in turtle file")
        _, offset = value
        with open(self.WT_LOG_FILE % id, 'r+b') as f:
            f.seek(offset)
            f.write(struct.pack('<I', 0xFFFFFFFF))

    def test_duplicate_logs(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.session.begin_transaction()
        with WiredTigerCursor(self.session, self.uri) as c:
            for i in range(1000):
                c[f'key_{i}'] = f'val_{i}'
        self.session.commit_transaction()
        
        for i in range(10):
            try:
                self.close_conn()
                self.inject_faulty_to_log(i+1)
                with self.expectedStdoutPattern("orrupted record length oversize at position"):
                    self.open_conn()
            except Exception as e:
                print(e)
