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
<<<<<<< HEAD

import wiredtiger, wttest
from wtscenario import make_scenarios
from wtbound import bound_base

# test_cursor_bound14.py
# Test write operation calls on a bounded cursor. Test general use cases of bound API,
# including setting lower bounds and upper bounds.
class test_cursor_bound14(bound_base):
    file_name = 'test_cursor_bound14'

    types = [
        ('file', dict(uri='file:', use_colgroup=False)),
        ('table', dict(uri='table:', use_colgroup=False)),
        # FIXME-WT-9738: Uncomment once bug with remove operations on a bounded cursor with colgroups is fixed.
        #('colgroup', dict(uri='table:', use_colgroup=True))
    ]

    key_formats = [
        ('string', dict(key_format='S')),
        ('var', dict(key_format='r')),
        ('int', dict(key_format='i')),
        ('bytes', dict(key_format='u')),
        ('composite_string', dict(key_format='SSS')),
        ('composite_int_string', dict(key_format='iS')),
        ('composite_complex', dict(key_format='iSru')),
    ]

    value_formats = [
        ('string', dict(value_format='S')),
        # FIX-ME-WT-9589: Fix bug complex colgroups not returning records within bounds.
        # ('complex-string', dict(value_format='SS')),
    ]

    cursor_config = [
        ('overwrite', dict(cursor_config="")),
        ('no-overwrite', dict(cursor_config="overwrite=false")),
    ]
    config = [
        ('lower-bounds-evict', dict(lower_bounds=True,upper_bounds=False,evict=True)),
        ('upper-bounds-evict', dict(lower_bounds=False,upper_bounds=True,evict=True)),
        ('both-bounds-evict', dict(lower_bounds=True,upper_bounds=True,evict=True)),
        ('lower-bounds-no-evict', dict(lower_bounds=True,upper_bounds=False,evict=True)),
        ('upper-bounds-no-evict', dict(lower_bounds=False,upper_bounds=True,evict=True)),
        ('both-bounds-no-evict', dict(lower_bounds=True,upper_bounds=True,evict=False)),
    ]

    scenarios = make_scenarios(types, key_formats, value_formats, config, cursor_config)

    def test_bound_data_operations(self):
        cursor = self.create_session_and_cursor(self.cursor_config)

        cursor.set_key(self.gen_key(10))
        cursor.set_value(self.gen_val(100))
        self.assertEqual(cursor.insert(), 0)

        cursor.set_key(self.gen_key(95))
        cursor.set_value(self.gen_val(100))
        self.assertEqual(cursor.insert(), 0)
        
        if (self.lower_bounds):
            self.set_bounds(cursor, 45, "lower", self.lower_inclusive)
        if (self.upper_bounds):
            self.set_bounds(cursor, 50, "upper", self.upper_inclusive)

        # Test bound API: test inserting key outside of bounds.
        cursor.set_key(self.gen_key(15))
        cursor.set_value(self.gen_val(120))
        if (self.lower_bounds):
            self.assertRaisesHavingMessage(
                wiredtiger.WiredTigerError, lambda: cursor.insert(), '/item not found/')
        else:
            self.assertEqual(cursor.insert(), 0)

        cursor.set_key(self.gen_key(90))
        cursor.set_value(self.gen_val(120))
        if (self.upper_bounds):
            self.assertRaisesHavingMessage(
                wiredtiger.WiredTigerError, lambda: cursor.insert(), '/item not found/')
        else:
            self.assertEqual(cursor.insert(), 0)

        # Test bound API: test updating an existing key outside of bounds.
        cursor.set_key(self.gen_key(10))
        cursor.set_value(self.gen_val(120))
        if (self.lower_bounds):
            self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.update(), 0)
        

        cursor.set_key(self.gen_key(95))
        cursor.set_value(self.gen_val(120))
        if (self.upper_bounds):
            self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.update(), 0)

        # Test bound API: test reserve on an existing key with bounds.
        self.session.begin_transaction()
        cursor.set_key(self.gen_key(10))
        if (self.lower_bounds):
            self.assertRaisesHavingMessage(
                wiredtiger.WiredTigerError, lambda: cursor.reserve(), '/item not found/')
        else:
            self.assertEqual(cursor.reserve(), 0)

        cursor.set_key(self.gen_key(95))
        if (self.upper_bounds):
            self.assertRaisesHavingMessage(
                wiredtiger.WiredTigerError, lambda: cursor.reserve(), '/item not found/')
        else:
            self.assertEqual(cursor.reserve(), 0)
        self.session.commit_transaction()

        # Test bound API: test modifies on an existing key outside of bounds.
        if (not self.use_colgroup):
            self.session.begin_transaction()
            cursor.set_key(self.gen_key(10))
            mods = [wiredtiger.Modify("2", 0, 1)]
            if (self.lower_bounds):
                self.assertEqual(cursor.modify(mods), wiredtiger.WT_NOTFOUND)
            else:
                self.assertEqual(cursor.modify(mods), 0)

            cursor.set_key(self.gen_key(95))
            mods = [wiredtiger.Modify("2", 0, 1)]
            if (self.upper_bounds):
                self.assertEqual(cursor.modify(mods), wiredtiger.WT_NOTFOUND)
            else:
                self.assertEqual(cursor.modify(mods), 0)
            self.session.commit_transaction()
        
        # Test bound API: test removing on an existing key outside of bounds.
        cursor.set_key(self.gen_key(10))
        if (self.lower_bounds):
            self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.remove(), 0)

        cursor.set_key(self.gen_key(95))
        if (self.upper_bounds):
            self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.remove(), 0)

        # Test update a key on the boundary of bounds.
        cursor.set_key(self.gen_key(45))
        cursor.set_value(self.gen_val(120))
        if (self.lower_bounds):
            if (self.lower_inclusive):
                self.assertEqual(cursor.update(), 0)
            else:
                self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.update(), 0)

        cursor.set_key(self.gen_key(50))
        cursor.set_value(self.gen_val(120))
        if (self.upper_bounds):
            if (self.upper_inclusive):
                self.assertEqual(cursor.update(), 0)
            else:
                self.assertEqual(cursor.update(), wiredtiger.WT_NOTFOUND)
        else:
            self.assertEqual(cursor.update(), 0)
        cursor.close()

if __name__ == '__main__':
    wttest.run()
=======
#

import wttest
from wtscenario import make_scenarios
from wtbound import set_prefix_bound, bound_base

# test_cursor_bound14.py
# This test checks that a search_near call with the prefix key
# configuration will correctly find a key even in cases where the key
# range is split across multiple pages.
# This test has been migrated to use cursor bounds logic.
class test_cursor_bound14(bound_base):
    key_format_values = [
        ('var_string', dict(key_format='S')),
        ('byte_array', dict(key_format='u')),
    ]

    eviction = [
        ('eviction', dict(eviction=True)),
        ('no eviction', dict(eviction=False)),
    ]

    scenarios = make_scenarios(key_format_values)
    
    def check_key(self, key):
        if self.key_format == 'u':
            return key.encode()
        elif self.key_format == '10s':
            return key.ljust(10, "\x00")
        else:
            return key

    def test_cursor_bound(self):
        uri = 'table:test_cursor_bound'
        self.session.create(uri, 'key_format={},value_format=S'.format(self.key_format))
        cursor = self.session.open_cursor(uri)
        cursor2 = self.session.open_cursor(uri, None, "debug=(release_evict=true)")
        # Basic character array.
        l = "abcdefghijklmnopqrstuvwxyz"

        # Insert keys aaa -> aaz with timestamp 200.
        prefix = "aa"
        self.session.begin_transaction()
        for k in range (0, 25):
            key = prefix + l[k]
            cursor[key] = key
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(200))

        # Insert key aaz with timestamp 50.
        self.session.begin_transaction()
        cursor[prefix + "z"] = prefix + "z"
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))

        if self.eviction:
            # Evict the whole range.
            for k in range (0, 26):
                cursor2.set_key(prefix + l[k])
                self.assertEqual(cursor2.search(), 0)
                self.assertEqual(cursor2.reset(), 0)
        
        # Begin transaction at timestamp 250, all keys should be visible.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(250))
        cursor3 = self.session.open_cursor(uri)
        cursor3.reset()

        # Test with only lower bound set.
        self.assertEqual(self.set_bounds(cursor3, "aab", "lower"), 0) 
        cursor3.set_key("ab")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaz"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "aac", "lower"), 0) 
        cursor3.set_key("ab")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaz"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "aaz", "lower", True), 0) 
        cursor3.set_key("aaz")
        self.assertEqual(cursor3.search_near(), 0)
        self.assertEqual(cursor3.get_key(), self.check_key("aaz"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "a", "lower"), 0) 
        cursor3.set_key("aa")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaa"))
        cursor3.reset()

        # Test with only upper bound set.
        self.assertEqual(self.set_bounds(cursor3, "aac", "upper", True), 0) 
        cursor3.set_key("aad")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aac"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "aaz", "upper"), 0) 
        cursor3.set_key("aac")
        self.assertEqual(cursor3.search_near(), 0)
        self.assertEqual(cursor3.get_key(), self.check_key("aac"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "ac", "upper"), 0) 
        cursor3.set_key("aa")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaa"))
        cursor3.reset()

        # Test with both bounds set.
        self.assertEqual(self.set_bounds(cursor3, "aaa", "lower"), 0) 
        self.assertEqual(self.set_bounds(cursor3, "aad", "upper"), 0) 
        cursor3.set_key("aae")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aad"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "aaa", "lower"), 0) 
        self.assertEqual(self.set_bounds(cursor3, "aae", "upper"), 0) 
        cursor3.set_key("aad")
        self.assertEqual(cursor3.search_near(), 0)
        self.assertEqual(cursor3.get_key(), self.check_key("aad"))
        cursor3.reset()

        self.assertEqual(self.set_bounds(cursor3, "aac", "lower", True), 0) 
        self.assertEqual(self.set_bounds(cursor3, "aaz", "upper", True), 0) 
        cursor3.set_key("aab")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key("aac"))
        cursor3.reset()

        # Test with prefix bounds set.
        # Search near for aaza, with prefix bounds aaa should return the closest visible key: aaz.
        set_prefix_bound(self, cursor3, "aaz")
        self.session.breakpoint()
        cursor3.set_key("aaza")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaz"))
        cursor3.reset()

        # Search near for ab, with prefix bounds "aaa" should return the closest visible key: aaa.
        set_prefix_bound(self, cursor3, "aaa")
        self.session.breakpoint()
        cursor3.set_key("ab")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaa"))
        cursor3.reset()

        # Search near for aac, should return the closest visible key: aac.
        set_prefix_bound(self, cursor3, "a")
        self.session.breakpoint()
        cursor3.set_key("aac")
        self.assertEqual(cursor3.search_near(), 0)
        self.assertEqual(cursor3.get_key(), self.check_key("aac"))
        cursor3.reset()

        # Search near for aa, should return the closest visible key: aaa.
        set_prefix_bound(self, cursor3, "aaa")
        self.session.breakpoint()
        cursor3.set_key("aa")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaa"))
        cursor3.reset()

        cursor3.close()
        self.session.commit_transaction()
>>>>>>> Fix search near exact bug
