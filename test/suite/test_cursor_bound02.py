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

# test_cursor_bound02.py
#    Test that setting bounds of different key formats works in the cursor bound API. Also make
# sure that WiredTiger complains when the upper and lower bounds overlap.
class test_cursor_bound02(wttest.WiredTigerTestCase):
    file_name = 'test_cursor_bound02'

    types = [
        ('file', dict(uri='file:', use_index = False)),
        ('table', dict(uri='table:', use_index = False)),
    ]

    key_format_values = [
        ('string', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
        ('int', dict(key_format='i', value_format='S')),
        ('bytes', dict(key_format='u', value_format='S')),
        ('composite_string', dict(key_format='SSS', value_format='S')),
        ('composite_int_string', dict(key_format='iS', value_format='S')),
        ('composite_complex', dict(key_format='iSru', value_format='S')),
    ]

    scenarios = make_scenarios(types, key_format_values)
 
    def gen_key(self, i):
        tuple_key = []
        for key in self.key_format:
            if key == 'S' or key == 'u':
                tuple_key.append('key' + str(i))
            elif key == "r":
                tuple_key.append(self.recno(i))
            elif key == "i":
                tuple_key.append(i)
        
        if (len(self.key_format) == 1):
            return tuple_key[0]
        else:
            return tuple(tuple_key)

    def test_bound_api(self):
        uri = self.uri + self.file_name
        create_params = 'value_format={},key_format={}'.format(self.value_format, self.key_format)
        self.session.create(uri, create_params)
        cursor = self.session.open_cursor(uri)

        cursor.set_key(self.gen_key(20))
        cursor.bound("bound=lower")
        cursor.set_key(self.gen_key(90))
        cursor.bound("bound=upper")

        cursor.set_key(self.gen_key(10))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: cursor.bound("bound=upper"), '/Invalid argument/')

        cursor.set_key(self.gen_key(99))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: cursor.bound("bound=lower"), '/Invalid argument/')

        
if __name__ == '__main__':
    wttest.run()
