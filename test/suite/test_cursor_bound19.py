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

import wiredtiger, wttest, ctypes
from wtscenario import make_scenarios
from wtbound import bound_base

# test_cursor_bound18.py
#    Basic cursor bound API\ validation.
class test_cursor_bound18(bound_base):
    file_name = 'test_cursor_bound18'
    use_index = True

    
    types = [
        ('table', dict(uri='table:', use_colgroup=False)),
        #('colgroup', dict(uri='table:', use_colgroup=True))
    ]

    key_formats = [
        ('string', dict(key_format='S')),
        # ('var', dict(key_format='r')),
        # ('int', dict(key_format='i')),
        # ('bytes', dict(key_format='u')),
        # ('composite_string', dict(key_format='SSS')),
        # ('composite_int_string', dict(key_format='iS')),
        # ('composite_complex', dict(key_format='iSru')),
    ]

    value_formats = [
        ('string', dict(value_format='S')),
        # ('int', dict(value_format='i')),
        # ('bytes', dict(value_format='u')),
        # ('composite_string', dict(value_format='SSS')),
        # ('composite_int_string', dict(value_format='iS')),
        # ('composite_complex', dict(value_format='iSru')),
    ]

    config = [
        ('no-evict', dict(evict=False)),
        # ('evict', dict(evict=True))
    ]

    scenarios = make_scenarios(types, key_formats, value_formats, config)

    def create_session_and_cursor(self, cursor_config=None):
        uri = self.uri + self.file_name
        create_params = 'value_format={},key_format={}'.format(self.value_format, self.key_format)
        create_params += self.gen_create_param()
        self.session.create(uri, create_params)

        # Add in column group.
        if self.use_colgroup:
            for i in range(0, len(self.value_format)):
                create_params = 'columns=(v{0}),'.format(i)
                suburi = 'colgroup:{0}:g{1}'.format(self.file_name, i)
                self.session.create(suburi, create_params)

        cursor = self.session.open_cursor(uri, None, cursor_config)
        self.session.begin_transaction()
        count = self.start_key
        for i in range(self.start_key, self.end_key + 1):
            cursor[self.gen_key(i)] = self.gen_val(count)
            # Increase count on every even interval to produce duplicate values.
            if (i % 2 == 0): 
                count = count + 1
        self.session.commit_transaction()

        if (self.evict):
            evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
            for i in range(self.start_key, self.end_key):
                evict_cursor.set_key(self.gen_key(i))
                evict_cursor.search()
                evict_cursor.reset() 
            evict_cursor.close()

        self.index = True
        return cursor      

    def test_cursor_index_bounds(self):
        cursor = self.create_session_and_cursor()
        cursor.close()
        # I need to test that modifications to the keys of the normal table or colgroups should not 
        # work because of bounds.


        # Test Index index_cursors bound API support.
        suburi = "index:" + self.file_name + ":i0"
        start = 0
        columns_param = "columns=("
        for _ in self.value_format:
            columns_param += "v{0},".format(str(start)) 
            start += 1
        columns_param += ")"
        self.session.create(suburi, columns_param)


        index_cursor = self.session.open_cursor("index:" + self.file_name + ":i0")


        # # Set bounds at lower key 30 and upper key at 50.
        # self.set_bounds(index_cursor, 30, "lower")
        # self.set_bounds(index_cursor, 40, "upper")
        # self.cursor_traversal_bound(index_cursor, 30, 40, True, 22)
        # self.cursor_traversal_bound(index_cursor, 30, 40, False, 22)
        
        # # Test basic search near scenarios.
        # # index_cursor.set_key(self.gen_key(20))
        # # self.assertEqual(index_cursor.search_near(), 1)
        # # self.assertEqual(index_cursor.get_key(), self.check_key(30))

        # # index_cursor.set_key(self.gen_key(35))
        # # self.assertEqual(index_cursor.search_near(), 0)
        # # self.assertEqual(index_cursor.get_key(), self.check_key(35))

        # # index_cursor.set_key(self.gen_key(60))
        # # self.assertEqual(index_cursor.search_near(), -1)
        # # self.assertEqual(index_cursor.get_key(), self.check_key(40))

        # # Test basic search scnarios.
        # index_cursor.set_key(self.gen_key(20))
        # self.assertEqual(index_cursor.search(), wiredtiger.WT_NOTFOUND)
        
        # index_cursor.set_key(self.gen_key(35))
        # self.assertEqual(index_cursor.search(), 0)

        # index_cursor.set_key(self.gen_key(50))
        # self.assertEqual(index_cursor.search(), wiredtiger.WT_NOTFOUND)

        # # Test that cursor resets the bounds.
        # self.assertEqual(index_cursor.reset(), 0)
        # self.cursor_traversal_bound(index_cursor, None, None, True, 60)
        # self.cursor_traversal_bound(index_cursor, None, None, False, 60)

        # # Test that cursor action clear works and clears the bounds.
        # self.set_bounds(index_cursor, 30, "lower")
        # self.set_bounds(index_cursor, 50, "upper")
        # self.assertEqual(index_cursor.bound("action=clear"), 0)
        # self.cursor_traversal_bound(index_cursor, None, None, True, 60)
        # self.cursor_traversal_bound(index_cursor, None, None, False, 60)

        # Test special index case: Lower bound with exclusive
        self.set_bounds(index_cursor, 30, "lower", False)
        self.set_bounds(index_cursor, 40, "upper", True)
        self.cursor_traversal_bound(index_cursor, 30, 40, True, 20)
        self.cursor_traversal_bound(index_cursor, 30, 40, False, 20)
        
        # index_cursor.set_key(self.gen_key(20))
        # self.assertEqual(index_cursor.search_near(), 1)
        # self.assertEqual(index_cursor.get_key(), self.check_key(31))

        # index_cursor.set_key(self.gen_key(30))
        # self.assertEqual(index_cursor.search(), wiredtiger.WT_NOTFOUND)

        # Test special index case: Test setting the upper bound limit on the largest possible value,
        # since WT internally increments the byte array by one.
        # Not sure how we can test this yet.
               
if __name__ == '__main__':
    wttest.run()
