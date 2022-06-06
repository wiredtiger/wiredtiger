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

# test_cursor_bound03.py
# Test that setting bounds of different key formats works in the cursor bound API. Make
# sure that WiredTiger complains when the upper and lower bounds overlap and that clearing the 
# bounds through the bound API and reset calls work appriopately.
class test_cursor_bound03(wttest.WiredTigerTestCase):
    file_name = 'test_cursor_bound03'

    types = [
        ('file', dict(uri='file:',use_colgroup=False)),
        ('table', dict(uri='table:',use_colgroup=False))
    ]

    key_format_values = [
        ('string', dict(key_format='S',value_format='S')),
        ('var', dict(key_format='r',value_format='S')),
        ('fix', dict(key_format='r',value_format='8t')),
        ('int', dict(key_format='i',value_format='S')),
        #('bytes', dict(key_format='u',value_format='S')),
    ]

    config = [
        ('inclusive-evict', dict(inclusive=True,evict=True)),
        #('no-inclusive-evict', dict(inclusive=False,evict=True)),
        ('inclusive', dict(inclusive=True,evict=False)),
        #('no-inclusive', dict(inclusive=False,evict=False))      
    ]

    scenarios = make_scenarios(types, key_format_values, config)
 
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
            
    def gen_value(self, i):
        return 'value' + str(i)

    def set_bounds(self, cursor, bound_config):
        inclusive_config = ",inclusive=false" if self.inclusive == False else ""
        self.assertEqual(cursor.bound("bound={0}{1}".format(bound_config, inclusive_config)), 0)

    def create_session_and_cursor(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format={}'.format(self.key_format)
        self.session.create(uri, create_params)

        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(40, 60):
            cursor[self.gen_key(i)] = self.gen_value(i) 
        self.session.commit_transaction()

        if (self.evict):
            evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
            for i in range(40, 60):
                evict_cursor.set_key(self.gen_key(i))
                evict_cursor.search()
                evict_cursor.reset() 

        return cursor

    # Need to also test if we return WT_NOTFOUND.
    def cursor_traversal_bound(self, cursor, lower_key, upper_key, next, expected_count):
        if (upper_key):
            cursor.set_key(self.gen_key(upper_key))
            self.set_bounds(cursor,"upper")
        
        if (lower_key):
            cursor.set_key(self.gen_key(lower_key))
            self.set_bounds(cursor,"lower")
        
        # if (upper_key):
        #     #Set upper bound to test that cursor positioning works.
        #     cursor.set_key(self.gen_key(upper_key))
        #     self.set_bounds(cursor,"upper")
            
        #     ret = cursor.prev()
        #     if ret != 0:
        #         return
        #     key = cursor.get_key()

        #     if(self.inclusive):
        #         self.assertEqual(int(key), int(upper_key))
        #     else:
        #         self.assertEqual(int(key), int(upper_key-1))


        count = ret = 0
        while True:
            if (next):
                ret = cursor.next()
            else:
                ret = cursor.prev()
            self.assertTrue(ret == 0 or ret == wiredtiger.WT_NOTFOUND)
            if ret == wiredtiger.WT_NOTFOUND:
                break
            count += 1
            key = cursor.get_key()
            
            if self.inclusive:
                if (lower_key):
                    self.assertTrue(self.gen_key(lower_key) <= self.gen_key(key))
                if (upper_key):
                    self.assertTrue(key <= self.gen_key(upper_key))
            else:
                if (lower_key):
                    self.assertTrue(self.gen_key(lower_key) < self.gen_key(key))
                if (upper_key):
                    self.assertTrue(self.gen_key(key) < self.gen_key(upper_key))
        self.assertEqual(expected_count, count)
        self.assertEqual(cursor.bound("action=clear"), 0)
    
    def test_bound_next_early_exit(self):
        cursor = self.create_session_and_cursor()
        self.tty("NEXT TESTS----------------")

        cursor.set_key(self.gen_key(50))
        self.set_bounds(cursor,"upper")
        
        #Upper bound set, default inclusive options works.
        while True:
            ret = cursor.next()
            self.tty("next ret:")
            self.tty(str(ret))
            
            if ret != 0:
                break
            key = cursor.get_key()
            if self.inclusive:
                self.assertTrue(int(key) <= int(50))
            else:
                self.assertTrue(int(key) < int(50))        

        cursor.close()

    def test_bound_prev_early_exit(self):
        cursor = self.create_session_and_cursor()

        # Lower bound set, default inclusive options works.
        #self.cursor_traversal_bound(cursor, None, 50, False)
        #self.cursor_traversal_bound(cursor, 45, 50, False)
        self.cursor_traversal_bound(cursor, 35, None, False)
        self.cursor_traversal_bound(cursor, 50, None, False)

        # cursor.set_key(self.gen_key(70))
        # self.set_bounds(cursor,"upper")
        # self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)

        cursor.close()

if __name__ == '__main__':
    wttest.run()