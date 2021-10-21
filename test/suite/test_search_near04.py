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
#

import wttest
from wtscenario import make_scenarios

# test_search_near04.py
class test_search_near04(wttest.WiredTigerTestCase):
    key_format_values = [
        ('var_string', dict(key_format='S')),
        ('byte_array', dict(key_format='u')),
    ]

    scenarios = make_scenarios(key_format_values)

    def check_key(self, key):
        if self.key_format == 'u':
            return key.encode()
        else:
            return key

    def test_search_near(self):
        uri = 'table:test_search_near'
        self.session.create(uri, 'key_format={},value_format=S'.format(self.key_format))

        # Make the keys big enough to span over multiple pages.
        # key_size can be set to to a lower value so only one page is used and search_near works.
        key_size = 200

        cursor = self.session.open_cursor(uri)
        cursor2 = self.session.open_cursor(uri, None, "debug=(release_evict=true)")
        
        # Basic character array.
        l = "abcdefghijklmnopqrstuvwxyz"

        # Insert keys aaa -> aaz with timestamp 200.
        prefix = "aa"
        self.session.begin_transaction()
        for k in range (0, 25):
            key = prefix + l[k]
            cursor[key * key_size] = key * key_size
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(200))

        # Insert key aaz with timestamp 50.
        self.session.begin_transaction()
        key = prefix + "z"
        cursor[key * key_size] = key * key_size
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(50))

        # Evict the whole range.
        # If eviction is not performed, things stay in memory and it works fine.
        for k in range (0, 26):
            cursor2.set_key((prefix + l[k]) * key_size)
            self.assertEqual(cursor2.search(), 0)
            self.assertEqual(cursor2.reset(), 0)

        # Start a transaction at timestamp 100, aaz should be the only key that is visible.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(100))
        cursor3 = self.session.open_cursor(uri)

        # Prefix search is disabled by default.
        # Search near should always return the only visible key.
        expected_key = "aaz" * key_size
        cursor3.set_key("aa")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key(expected_key))

        cursor3.set_key("az")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key(expected_key))

        cursor3.set_key("aaz" * key_size)
        self.assertEqual(cursor3.search_near(), 0)
        self.assertEqual(cursor3.get_key(), self.check_key(expected_key))

        cursor3.set_key("aazab")
        self.assertEqual(cursor3.search_near(), -1)
        self.assertEqual(cursor3.get_key(), self.check_key(expected_key))

        # Enable prefix search.
        cursor3.reconfigure("prefix_search=true")
        
        # cursor3.reconfigure("abc=true")

        # The only visible key is aaz.
        # It fails if we try look for a prefix that matches keys that are on the first page.
        # Keys that are on the first page start with "aa". Hence if we look for the prefix "a" or
        # "aa", we will need to traverse the first page and we will reach the end of the page before
        # getting to the only visible one which is on another page.
        # If we try to look for "aaz", in our case, we will be on the correct page and we will be
        # able to find the visible key.
        cursor3.set_key("a")
        # cursor3.set_key("aa")
        # cursor3.set_key("aaz")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key(expected_key))

        cursor3.close()
        self.session.commit_transaction()