#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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
# test_las05.py
#   Test file_max functionality for the lookaside table to ensure that we panic
#   when the specified amount is exceeded.
#

import os, unittest, subprocess, sys, wiredtiger, wttest
from wtscenario import make_scenarios

def las_workload(las_file_max):
    uri = 'table:test_las05'

    conn = wiredtiger.wiredtiger_open(
        None, 'create=true,cache_size=50MB,cache_overflow=(file_max={})'
        .format(las_file_max))

    session = conn.open_session()
    session.create(uri, 'key_format=S,value_format=S')
    cursor = session.open_cursor(uri)

    for key in range(1000):
        cursor.set_key(str(key))
        cursor.set_value(os.urandom(1000000))
        cursor.insert()

    session.checkpoint()

    session2 = conn.open_session()
    session2.begin_transaction('isolation=snapshot')

    for key in range(1000):
        cursor.set_key(str(key))
        cursor.set_value(os.urandom(1000000))
        cursor.insert()

    session2.rollback_transaction()

class test_las05(unittest.TestCase):
    def test_las(self):
        python_path = ':'.join(sys.path)
        env = os.environ.copy()
        env["PYTHONPATH"] = python_path;
        las_workload_program = \
                          'import test_las05;' \
                          'test_las05.las_workload(\'100MB\');'
        subprocess.check_call(['python', '-c', las_workload_program], env=env)
