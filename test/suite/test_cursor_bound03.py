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
#    Test that setting bounds of different key formats works in the cursor bound API. Make
# sure that WiredTiger complains when the upper and lower bounds overlap and that clearing the 
# bounds through the bound API and reset calls work appriopately.
class test_cursor_bound03(wttest.WiredTigerTestCase):
    file_name = 'test_cursor_bound03'

    types = [
        ('file', dict(uri='file:', use_colgroup=False)),
        ('table', dict(uri='table:', use_colgroup=False)),
    ]

    key_format_values = [
        ('string', dict(key_format='S')),
        # ('var', dict(key_format='r')),
        ('int', dict(key_format='i')),
        ('bytes', dict(key_format='u')),
    ]

    inclusive = [
        ('inclusive', dict(inclusive=True)),
        # ('no-inclusive', dict(inclusive=False))
    ]
    scenarios = make_scenarios(types, key_format_values, inclusive)
 
    def gen_key(self, i):
        tuple_key = []
        for key in self.key_format:
            if key == 'S' or key == 'u':
                tuple_key.append(str(i))
            elif key == "r":
                tuple_key.append(self.recno(i))
            elif key == "i":
                tuple_key.append(i)
        
        if (len(self.key_format) == 1):
            return tuple_key[0]
        else:
            return tuple(tuple_key)
            
    def genvalue(self, i):
        return 'value' + str(i)

    def set_bounds(self, cursor, bound_config):
        inclusive_config = ",inclusive=false" if self.inclusive == False else ""
        self.assertEqual(cursor.bound("bound={0}{1}".format(bound_config, inclusive_config)), 0)

    def create_session_and_cursor(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format={}'.format(self.key_format)
        self.session.create(uri, create_params)

        cursor = self.session.open_cursor(uri)

        for i in range(40,60):
            cursor.set_key(self.gen_key(i))
            cursor[self.gen_key(i)] = self.genvalue(i)     

        return cursor   
                 
    def test_bound_next_early_exit(self):
        cursor = self.create_session_and_cursor()
        self.tty("NEXT TESTS----------------")
        #Test bound API: Early exit works with next traversal call.
        cursor.set_key(self.gen_key(50))
        cursor.set_value("1000")
        self.set_bounds(cursor,"upper")
        cursor.insert()

        #Upper bound set, default inclusive options works.
        while True:
            nextret = cursor.next()
            self.tty("next ret:")
            self.tty(str(nextret))
            
            if nextret != 0:
                break
            key = cursor.get_key()
            self.tty(str(key))
            self.tty(str(50))
            if self.inclusive:
                self.assertTrue(int(key) <= int(50))

            else:
                self.assertTrue(int(key) < int(50))        
        
        return cursor


    def test_bound_prev_early_exit(self):
        cursor = self.create_session_and_cursor()
        self.tty("PREV TESTS----------------")

        #Test bound API: Early exit works with next traversal call.
        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"lower")

        #Lower bound set, default inclusive options works.
        while True:
            prevret = cursor.prev()
            self.tty("prev ret:")
            self.tty(str(prevret))
            if prevret != 0:
                break
            key = cursor.get_key()
            self.tty("prev key\n")
            self.tty(str(key))
            self.assertTrue(int(key) >= int(55))

        return cursor
        
    # def test_bound_early_exit(self):
    #     cursor = self.create_session_and_cursor()
    #     cursor = self.test_bound_next_early_exit(cursor)
    #     cursor = self.test_bound_prev_early_exit(cursor)
    #     cursor.close()

if __name__ == '__main__':
    wttest.run()