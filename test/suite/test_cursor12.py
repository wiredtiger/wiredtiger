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

import wiredtiger, wttest
from wtdataset import SimpleDataSet, ComplexDataSet, ComplexLSMDataSet
from wtscenario import make_scenarios

# test_cursor12.py
#    Test cursor modify call
class test_cursor12(wttest.WiredTigerTestCase):
    uri = 'table:cursor12'

    def test_modify(self):
        """
        Create entries, and modify a portion of each
        """
        self.session.create(self.uri, 'key_format=S,value_format=u')
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor['ABC'] = '\x01\x02\x03\x04'
        cursor['DEF'] = '\x11\x12\x13\x14'
        mods = []
        mod = wiredtiger.Modify('\xA1\xA2\xA3\xA4', 1, 1)
        mods.append(mod)
        mod = wiredtiger.Modify('\xB1\xB2\xB3\xB4', 1, 1)
        mods.append(mod)
        cursor.set_key('ABC')
        self.KNOWN_FAILURE('WT_CURSOR.modify() is not yet implemented')
        cursor.modify(mods)
        print str(cursor['ABC'])
        cursor.set_key('DEF')
        cursor.modify(mods)
        print str(cursor['DEF'])

if __name__ == '__main__':
    wttest.run()
