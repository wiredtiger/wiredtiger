#!/usr/bin/env python
#
# Public Domain 2008-2012 WiredTiger, Inc.
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

import os
import wiredtiger, wttest

# test_config04.py
#    Individually test config options
class test_config04(wttest.WiredTigerTestCase):
    table_name1 = 'test_config04'
    nentries = 100

    K = 1024
    M = K * K
    G = K * M
    T = K * G

    # Each test needs to set up its connection in its own way,
    # so override these methods to do nothing
    def setUpConnectionOpen(self, dir):
        return None

    def setUpSessionOpen(self, conn):
        return None

    def populate_and_check(self):
        """
        Create entries, and read back in a cursor: key=string, value=string
        """
        create_args = 'key_format=S,value_format=S'
        self.session.create("table:" + self.table_name1, create_args)
        cursor = self.session.open_cursor('table:' + self.table_name1, None, None)
        for i in range(0, self.nentries):
            cursor.set_key(str(1000000 + i))
            cursor.set_value('value' + str(i))
            cursor.insert()
        i = 0
        cursor.reset()
        for key, value in cursor:
            self.assertEqual(key, str(1000000 + i))
            self.assertEqual(value, ('value' + str(i)))
            i += 1
        self.assertEqual(i, self.nentries)
        cursor.close()

    def common_test(self, configextra):
        """
        Call wiredtiger_open and run a simple test.
        configextra are any extra configuration strings needed on the open.
        """
        configarg = 'create'
        if configextra != None:
            configarg += ',' + configextra
        self.conn = wiredtiger.wiredtiger_open('.', configarg)
        self.session = self.conn.open_session(None)
        self.populate_and_check()

    def common_cache_size_test(self, sizestr, size):
        self.common_test('cache.size=' + sizestr)
        cursor = self.session.open_cursor('statistics:', None, None)
        cursor.set_key(wiredtiger.stat.cache_bytes_max)
        self.assertEqual(cursor.search(), 0)
        got_cache = cursor.get_values()[2]
        self.assertEqual(got_cache, size)

    def test_bad_config(self):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: wiredtiger.wiredtiger_open('.', 'not_valid,another_bad=10'),
            "wiredtiger_open: Unknown configuration key"
            " found: 'not_valid': Invalid argument\n")

    def test_cache_size_number(self):
        # Use a number without multipliers
        # 1M is the minimum, we'll ask for 1025 * 1024
        cache_size_str = str(1025 * 1024)
        self.common_cache_size_test(cache_size_str, 1025*self.K)

    def test_cache_size_K(self):
        # Kilobyte sizing test
        # 1M is the minimum, so ask for that using K notation.
        self.common_cache_size_test('1024K', 1024*self.K)

    def test_cache_size_M(self):
        # Megabyte sizing test
        self.common_cache_size_test('30M', 30*self.M)

    def test_cache_size_G(self):
        # Gigabyte sizing test
        # We are specifying the maximum the cache can grow,
        # not the initial cache amount, so small tests like
        # this can still run on smaller machines.
        self.common_cache_size_test('7G', 7*self.G)

    def test_cache_size_T(self):
        # Terabyte sizing test
        # We are specifying the maximum the cache can grow,
        # not the initial cache amount, so small tests like
        # this can still run on smaller machines.
        self.common_cache_size_test('2T', 2*self.T)

    def test_cache_too_small(self):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: wiredtiger.wiredtiger_open('.', 'create,cache.size=900000'),
            "/Value too small for key 'size' the minimum is/")

    def test_cache_too_large(self):
        T11 = 11 * self.T  # 11 Terabytes
        configstr = 'create,cache.size=' + str(T11)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: wiredtiger.wiredtiger_open('.', configstr),
            "/Value too large for key 'size' the maximum is/")

    def test_eviction(self):
        self.common_test('eviction_target=84,eviction_trigger=94')
        # Note

    def test_eviction_bad(self):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda:
            wiredtiger.wiredtiger_open('.', 'create,eviction_target=91,' +
                                       'eviction_trigger=81'),
            "/eviction target must be lower than the eviction trigger/")

    def test_eviction_bad2(self):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda:
            wiredtiger.wiredtiger_open('.', 'create,eviction_target=86,' +
                                       'eviction_trigger=86'),
            "/eviction target must be lower than the eviction trigger/")

    def test_hazard_max(self):
        # Note: There isn't any direct way to know that this was set.
        self.common_test('hazard_max=50')

    def test_session_max(self):
        # Note: There isn't any direct way to know that this was set,
        # but we'll have a separate functionality test to test for
        # this indirectly.
        self.common_test('session_max=99')

    def test_multiprocess(self):
        self.common_test('multiprocess')
        # TODO: how do we verify that it was set?

    def test_error_prefix(self):
        self.common_test('error_prefix="MyOwnPrefix"')
        # TODO: how do we verify that it was set?

    def test_logging(self):
        # Note: this will have functional tests in the future.
        self.common_test('logging')

    def test_transactional(self):
        # Note: this will have functional tests in the future.
        self.common_test('transactional')

if __name__ == '__main__':
    wttest.run()
