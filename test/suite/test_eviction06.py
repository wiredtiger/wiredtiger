#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, and distribute this software for
# any purpose, with or without fee, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#

import wiredtiger
import wttest


class test_eviction06(wttest.WiredTigerTestCase):
    conn_config = (
        'cache_size=10MB,statistics=(all),eviction=(threads_min=1,threads_max=1),'
        'eviction_updates_trigger=20,eviction_updates_target=10,'
        'eviction_dirty_trigger=95,eviction_dirty_target=90')
    uri = 'table:eviction06'

    def test_walk_dominating_update_tree(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        value = 'a' * 1024
        for i in range(20000):
            cursor[i] = value
        cursor.close()

        for _ in range(10):
            cursor = self.session.open_cursor(self.uri)
            self.session.begin_transaction()
            for i in range(0, 20000, 2):
                cursor[i] = value
            self.session.commit_transaction()
            cursor.close()

        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.eviction_server_walk_dominating_cache, 0)


if __name__ == '__main__':
    wttest.run()
