#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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

# test_stat08.py
#    Session statistics for cache writes and reads.
class test_stat08(wttest.WiredTigerTestCase):

    nentries = 5200
    # Small cache and stats colleciton enabled.
    conn_config = 'cache_size=1MB,statistics=(all)'
    # Make the values about 200 bytes. That's little more than 1MB of
    # data for 5200 records, triggering writes to the disk. Reading this
    # data would also trigger disk reads.
    entry_value = "abcde" * 40

    def test_session_stats(self):
        self.session = self.conn.open_session()
        self.session.create("table:test_stat08",
                            "key_format=i,value_format=S")
        cursor =  self.session.open_cursor('table:test_stat08', None, None)
        # Write the entries.
        for i in range(0, self.nentries):
            cursor[i] = self.entry_value
        cursor.reset()

        # Read the entries.
        for key, value in cursor:
            self.pr('read %d -> %s' % (key, value))
        cursor.reset()

        stat_cur = self.session.open_cursor('statistics:session', None, None)
        while stat_cur.next() == 0:
            [desc, pvalue, value] = stat_cur.get_values()
            self.printVerbose(2, '  stat: \'' + desc + '\', \'' +
                              pvalue + '\', ' + str(value))
            if desc == 'session: bytes read into cache' or \
               desc == 'session: bytes written from cache' or \
               desc == 'session: page read from disk to cache time (usecs)' or \
               desc == 'session: page write from cache to disk time (usecs)':
                self.assertTrue(value > 0)

        # Session stats cursor reset should set all the stats values to zero.
        stat_cur.reset()
        while stat_cur.next() == 0:
            [desc, pvalue, value] = stat_cur.get_values()
            self.assertTrue(value == 0)

        # Write 1000 more records to the disk.
        stat_cur.reset()
        for i in range(0, 1000):
            cursor[i + self.nentries] = self.entry_value
        self.session.checkpoint()

        while stat_cur.next() == 0:
            [desc, pvalue, value] = stat_cur.get_values()
            if desc == 'session: page write from cache to disk time (usecs)' or \
               desc == 'session: bytes written from cache':
                self.assertTrue(value > 0)

if __name__ == '__main__':
    wttest.run()
