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

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# test_checkpoint41.py
#    A checkpoint of a multi-level tree must record, for every internal page it writes, the
#    addresses its children were written to by that same checkpoint. Build trees deep enough to
#    have several internal levels, checkpoint them repeatedly with reconciliation spread across
#    worker threads, and read each checkpoint back through a checkpoint cursor.
class test_checkpoint41(wttest.WiredTigerTestCase):

    thread_values = [
        ('single', dict(checkpoint_threads=1)),
        ('parallel', dict(checkpoint_threads=4)),
    ]
    format_values = [
        ('row', dict(key_format='i', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]

    scenarios = make_scenarios(thread_values, format_values)

    # A small cache keeps eviction running against the trees the checkpoint is walking, which is
    # what discards the clean pages a checkpoint leaves behind as it goes.
    def conn_config(self):
        return ('cache_size=50MB,statistics=(all)'
                f',checkpoint_threads={self.checkpoint_threads}')

    # Tiny pages give a tree several internal levels for a modest amount of data, so the
    # checkpoint has to write internal pages that reference other internal pages.
    def create_config(self):
        return (f'key_format={self.key_format},value_format={self.value_format}'
                ',leaf_page_max=4KB,internal_page_max=4KB,memory_page_max=16KB')

    def test_checkpoint_internal_page_addresses(self):
        ntables = 4
        nrows = 8000
        nrounds = 4
        uris = [f'table:ckpt41_{i}' for i in range(ntables)]
        value = 'v' * 300

        for uri in uris:
            self.session.create(uri, self.create_config())

        # Each round rewrites a disjoint slice of every table, so each checkpoint finds dirty
        # leaves spread across the whole key range rather than clustered under one parent.
        expected = {}
        for rnd in range(nrounds):
            for uri in uris:
                cursor = self.session.open_cursor(uri)
                for i in range(rnd, nrows, nrounds):
                    cursor[i + 1] = f'{rnd}-{value}'
                    expected[(uri, i + 1)] = f'{rnd}-{value}'
                cursor.close()

            self.session.checkpoint()
            self.check_checkpoint(expected)

        # The tree must be deep enough that the checkpoint wrote internal pages, or the ordering
        # this test covers was never exercised.
        self.assertGreater(self.get_stat(stat.conn.checkpoint_pages_visited_internal), 0)

        # Reopening leaves the tables with no open handles, which verify requires, and checks
        # that the checkpoint reads back from disk.
        self.reopen_conn()
        for uri in uris:
            self.session.verify(uri)
        self.check_checkpoint(expected)

    def check_checkpoint(self, expected):
        seen = 0
        for uri in {u for u, _ in expected}:
            cursor = self.session.open_cursor(uri, None, 'checkpoint=WiredTigerCheckpoint')
            for key, value in cursor:
                self.assertEqual(value, expected[(uri, key)],
                                 f'{uri} key {key} read back from the checkpoint as {value}')
                seen += 1
            cursor.close()
        self.assertEqual(seen, len(expected))

    def get_stat(self, stat_key):
        cursor = self.session.open_cursor('statistics:', None, None)
        value = cursor[stat_key][2]
        cursor.close()
        return value

if __name__ == '__main__':
    wttest.run()
