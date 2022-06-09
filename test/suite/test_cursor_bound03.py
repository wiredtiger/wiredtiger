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
# Test the next() and prev() calls in the cursor bound API. There are three main scenarios that are
# tested in this python test.
#   1. Test general use cases of bound API, including setting lower bounds and upper bounds.
#   2. Test combination scenarios of using next() and prev() together.
#   3. Test clearing bounds and special scenarios of the cursor API usage.
class test_cursor_bound03(wttest.WiredTigerTestCase):
    file_name = 'test_cursor_bound03'
    start_key = 20
    end_key = 80

    types = [
        ('file', dict(uri='file:')),
        ('table', dict(uri='table:'))
    ]

    key_format_values = [
        ('string', dict(key_format='S',value_format='S')),
        ('var', dict(key_format='r',value_format='S')),
        #('fix', dict(key_format='r',value_format='8t')),
        ('int', dict(key_format='i',value_format='S')),
        ('bytes', dict(key_format='u',value_format='S')),
        ('composite_string', dict(key_format='SSS',value_format='S')),
        ('composite_int_string', dict(key_format='iS',value_format='S')),
        ('composite_complex', dict(key_format='iSru',value_format='S')),
    ]

    config = [
        ('inclusive-evict', dict(lower_inclusive=True,upper_inclusive=True,evict=True)),
        ('inclusive-no-evict', dict(lower_inclusive=True,upper_inclusive=True,evict=False))
    ]

    direction = [
        ('prev', dict(next=False)),
        ('next', dict(next=True)),
    ]

    scenarios = make_scenarios(types, key_format_values, config, direction)
 
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

    def check_key(self, i):
        list_key = []
        for key in self.key_format:
            if key == 'S':
                list_key.append(str(i))
            elif key == "r":
                list_key.append(self.recno(i))
            elif key == "i":
                list_key.append(i)
            elif key == "u":
                list_key.append(str(i).encode())
        
        if (len(self.key_format) == 1):
            return list_key[0]
        else:
            return list_key
            
    def gen_value(self, i):
        if (self.value_format == "8t"):
            return i
        else:
            return 'value' + str(i)

    def set_bounds(self, cursor, bound_config):
        inclusive_config = ""
        if ((bound_config == "lower" and self.lower_inclusive == False) or (bound_config == "upper" and self.upper_inclusive == False)):
            inclusive_config = ",inclusive=false"
        self.assertEqual(cursor.bound("bound={0}{1}".format(bound_config, inclusive_config)), 0)

    def create_session_and_cursor(self):
        uri = self.uri + self.file_name
        create_params = 'value_format={},key_format={}'.format(self.value_format, self.key_format)
        self.session.create(uri, create_params)

        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(self.start_key, self.end_key + 1):
            cursor[self.gen_key(i)] = self.gen_value(i) 
        self.session.commit_transaction()

        if (self.evict):
            evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
            for i in range(self.start_key, self.end_key):
                evict_cursor.set_key(self.gen_key(i))
                evict_cursor.search()
                evict_cursor.reset() 
        return cursor

    def cursor_traversal_bound(self, cursor, lower_key, upper_key, next=None, expected_count=None):
        if next == None:
            next = self.direction

        start_range = self.start_key
        end_range = self.end_key
        if (upper_key):
            end_range = min(end_range, upper_key)
            cursor.set_key(self.gen_key(upper_key))
            self.set_bounds(cursor,"upper")
        
        if (lower_key):
            start_range = max(start_range, lower_key)
            cursor.set_key(self.gen_key(lower_key))
            self.set_bounds(cursor,"lower")

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

            if (lower_key):
                self.assertTrue(self.check_key(lower_key) <= key)
            if (upper_key):
                self.assertTrue(key <= self.check_key(upper_key))

        count = max(count - 1, 0)
        if (expected_count != None):
            self.assertEqual(expected_count, count)
        else:
            self.assertEqual(end_range - start_range, count)

    def test_bound_special_scenario(self):
        cursor = self.create_session_and_cursor()

        # Test bound api: Test upper bound clearing with only lower bounds.
        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(45))
        cursor.reset()

        # Test bound api: Test lower bound clearing with lower bounds works.
        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(self.start_key))
        cursor.reset()

        # Test bound api: Test lower bound setting with positioned cursor.
        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(45))
        cursor.set_key(self.gen_key(40))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"lower"), '/Invalid argument/')
        cursor.reset()

        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(45))
        cursor.set_key(self.gen_key(90))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"lower"), '/Invalid argument/')
        cursor.reset()

        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(45))
        cursor.set_key(self.gen_key(10))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"lower"), '/Invalid argument/')
        cursor.reset()

        # Test bound api: Test upper bound setting with positioned cursor.
        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(self.start_key))
        cursor.set_key(self.gen_key(60))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"upper"), '/Invalid argument/')
        cursor.reset()

        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(self.start_key))
        cursor.set_key(self.gen_key(90))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"upper"),
            '/Invalid argument/')
        cursor.reset()

        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(self.start_key))
        cursor.set_key(self.gen_key(10))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: self.set_bounds(cursor,"upper"),
            '/Invalid argument/')
        cursor.reset()

        # Test bound api: Test inclusive lower bound setting with positioned cursor.
        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(55))
        inclusive_config = "false" if self.lower_inclusive else "true"
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: cursor.bound("bound=upper,inclusive={0}".format(inclusive_config)),
            '/Invalid argument/')
        cursor.reset()
        
        # Test bound api: Test inclusive upper bound setting with positioned cursor.
        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(self.start_key))
        inclusive_config = "false" if self.upper_inclusive else "true"
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: cursor.bound("bound=upper,inclusive={0}".format(inclusive_config)),
            '/Invalid argument/')
        cursor.reset()

        # Test bound api: Test inclusive upper bound setting with positioned cursor.
        cursor.set_key(self.gen_key(55))
        self.set_bounds(cursor,"lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(55))
        self.assertEqual(cursor.bound("action=clear"), 0)
        self.cursor_traversal_bound(cursor, None, None, True, self.end_key - 55 - 1)


    def test_bound_combination_scenario(self):
        cursor = self.create_session_and_cursor()

        # Test bound api: Test that prev() works after next() traversal.
        self.cursor_traversal_bound(cursor, 45, 50, self.direction, 5)
        self.assertEqual(cursor.prev(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(50))  
        cursor.reset()

        # Test bound api: Test that next() works after prev() traversal.
        self.cursor_traversal_bound(cursor, 45, 50, self.direction, 5)
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.check_key(45)) 

    def test_bound_general_scenario(self):
        cursor = self.create_session_and_cursor()

        # Test bound api: Test early exit works with upper bound.
        self.cursor_traversal_bound(cursor, None, 50)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal works with lower bound.
        self.cursor_traversal_bound(cursor, 45, None)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal with both bounds.
        self.cursor_traversal_bound(cursor, 45, 50)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal with lower bound (out of data range).
        self.cursor_traversal_bound(cursor, self.start_key - 5, None)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal with upper bound (out of data range).
        self.cursor_traversal_bound(cursor, None, 95)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal with both bounds (out of data range).
        self.cursor_traversal_bound(cursor, 10, 95)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test traversal with both bounds with no data in range.
        self.cursor_traversal_bound(cursor, 95, 99, self.direction, 0)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test that clearing bounds works.
        self.cursor_traversal_bound(cursor, 45, 50, self.direction, 5)
        self.assertEqual(cursor.bound("action=clear"), 0)
        self.cursor_traversal_bound(cursor, None, None, True)
        self.assertEqual(cursor.reset(), 0)

        # Test bound api: Test upper bound clearing with only lower bounds.
        cursor.set_key(self.gen_key(50))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=upper")
        self.cursor_traversal_bound(cursor, None, None, self.direction, self.end_key - 50)

        cursor.bound("action=clear,bound=lower")
        self.cursor_traversal_bound(cursor, None, None)
        
        # Test bound api: Test that changing upper bounds works.
        self.cursor_traversal_bound(cursor, None, 50)
        self.cursor_traversal_bound(cursor, None, 55)
        self.assertEqual(cursor.bound("action=clear"), 0)


    def test_bound_prev_early_exit(self):
        cursor = self.create_session_and_cursor()
        # Test bound api: Test that changing upper bounds works (out of data range).
        self.cursor_traversal_bound(cursor, None, 50)
        self.cursor_traversal_bound(cursor, None, 95)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test that changing upper bounds works into data range.
        self.cursor_traversal_bound(cursor, None, 95)
        self.cursor_traversal_bound(cursor, None, 50)
        self.assertEqual(cursor.bound("action=clear"), 0)

        # Test bound api: Test that changing lower bounds works.
        self.cursor_traversal_bound(cursor, 50, None)
        self.cursor_traversal_bound(cursor, 45, None)
        self.assertEqual(cursor.bound("action=clear"), 0)
        self.assertEqual(cursor.bound("action=clear"), 0)

        cursor.close()

if __name__ == '__main__':
    wttest.run()
    