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

import wiredtiger, wttest
from wtscenario import make_scenarios
from wtbound import set_prefix_bound

# test_cursor_bound12.py
# Check the search_near API returns the correct key according to visibility rules and its
# configuration.
# When cursor bounds aren't set, the search_near API should always return the closest key (in
# lexicographic order) as long as one exists and is visible.
# When cursor bounds are set, in addition to the previous rules, search_near returns a key that
# is within the given bounds.
# This test has been migrated to use cursor bound logic.
class test_cursor_bound12(wttest.WiredTigerTestCase):
    key_format_values = [
        #('fixed_string', dict(key_format='10s')),
        ('var_string', dict(key_format='S')),
        #('byte_array', dict(key_format='u')),
    ]

    eviction = [
        ('eviction', dict(eviction=True)),
        #('no eviction', dict(eviction=False)),
    ]

    scenarios = make_scenarios(key_format_values, eviction)

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

        # Insert key aazab with timestamp 250.
        self.session.begin_transaction()
        cursor[prefix + "zab"] = prefix + "zab"
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(250))

        if self.eviction:
            # Evict the whole range.
            for k in range (0, 26):
                cursor2.set_key(prefix + l[k])
                self.assertEqual(cursor2.search(), 0)
                self.assertEqual(cursor2.reset(), 0)
            cursor2.set_key(prefix + "zab")
            self.assertEqual(cursor2.search(), 0)
            self.assertEqual(cursor2.reset(), 0)

        # Start a transaction at timestamp 100, aaz should be the only key that is visible.
        cursor3 = self.session.open_cursor(uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(250))
        
        set_prefix_bound(self, cursor3, "aaz")
        
        self.session.breakpoint()
        cursor3.close()

        # Start a transaction at timestamp 250, all keys should be visible.
        cursor3 = self.session.open_cursor(uri)

        # Search near for aa, should return the closest visible key: aaa.
        cursor3.set_key("aa")
        self.assertEqual(cursor3.search_near(), 1)
        self.assertEqual(cursor3.get_key(), self.check_key("aaa"))
