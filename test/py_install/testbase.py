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

# A quick sanity test of an installation via 'pip install wiredtiger'.

import wiredtiger, shutil, os
from wiredtiger import wiredtiger_open, wiredtiger_version

wthome = "WTPY_TEST"
shutil.rmtree(wthome, ignore_errors=True)
os.mkdir(wthome)
conn = wiredtiger_open(wthome, "create")
session = conn.open_session()
session.create('table:foo', 'key_format=S,value_format=i')
c = session.open_cursor('table:foo')
c['A'] = 100
c['B'] = 200
c['C'] = 300
print('Expect 200 = ' + str(c['B']))
if c['B'] != 200:
    raise Exception('BAD RESULT')
c.close()
session.close()
conn.close()

print(wiredtiger_version())
print('testbase success.')
