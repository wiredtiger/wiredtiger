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

import os
import wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios

class test_stat21(wttest.WiredTigerTestCase):
    buckets = [('default', dict(buckets=None))] + [
        (str(n), dict(buckets=n)) for n in (1, 3, 4, 23, 64, 1024)]
    modes = [(mode, dict(mode=mode)) for mode in ('none', 'fast', 'all')]
    scenarios = make_scenarios(buckets, modes)

    def conn_config(self):
        config = 'statistics=({}),statistics_log=(wait=0)'.format(self.mode)
        if self.buckets is not None:
            config += ',statistics_dhandle_buckets={}'.format(self.buckets)
        return config

    def read_stat(self, uri, key, config='statistics=(fast)'):
        cursor = self.session.open_cursor('statistics:' + uri, None, config)
        value = cursor[key][2]
        cursor.close()
        return value

    def test_statistics(self):
        uris = ['table:stat21_{}'.format(i) for i in range(3)]
        for uri in uris:
            self.session.create(uri, 'key_format=i,value_format=i')

        if self.mode == 'none':
            self.conn.reconfigure('statistics=(all)')

        sessions = [self.conn.open_session() for _ in range(35)]
        for i, session in enumerate(sessions):
            for uri in uris:
                cursor = session.open_cursor(uri)
                cursor[i] = i
                cursor.close()

        for uri in uris:
            self.assertEqual(self.read_stat(uri, stat.dsrc.cursor_insert), len(sessions))
            self.assertEqual(self.read_stat(uri, stat.dsrc.cursor_insert,
                'statistics=(fast,clear)'), len(sessions))
            self.assertEqual(self.read_stat(uri, stat.dsrc.cursor_insert), 0)

        self.assertGreaterEqual(self.read_stat('', stat.conn.cursor_insert), 3 * len(sessions))

        for session in sessions:
            session.close()

        self.session.checkpoint('name=stat21')
        for uri in uris:
            self.assertGreater(self.read_stat(uri, stat.dsrc.block_checkpoint_size), 0)
            if self.mode != 'fast':
                for _ in range(2):
                    self.assertEqual(self.read_stat(uri, stat.dsrc.btree_entries,
                        'statistics=(all)'), len(sessions))
                self.assertEqual(self.read_stat(uri, stat.dsrc.btree_entries,
                    'statistics=(all),checkpoint=stat21'), len(sessions))

        with self.expectedStderrPattern('unknown configuration key'):
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: self.conn.reconfigure('statistics_dhandle_buckets=3'))

        self.reopen_conn()
        cursor = self.session.open_cursor(uris[0])
        cursor[100] = 100
        cursor.close()
        if self.mode == 'none':
            self.conn.reconfigure('statistics=(fast)')
            cursor = self.session.open_cursor(uris[0])
            cursor[101] = 101
            cursor.close()
        self.assertGreater(self.read_stat(uris[0], stat.dsrc.cursor_insert), 0)

class test_stat21_basecfg(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(fast),statistics_log=(wait=0),statistics_dhandle_buckets=7'

    def test_open_only_buckets(self):
        with open('WiredTiger.basecfg') as basecfg:
            self.assertNotIn('statistics_dhandle_buckets', basecfg.read())

        uri = 'table:stat21_basecfg'
        self.session.create(uri, 'key_format=i,value_format=i')
        for i, config in enumerate(('statistics=(fast),statistics_dhandle_buckets=64',
            'statistics=(fast)')):
            self.reopen_conn(config=config)
            cursor = self.session.open_cursor(uri)
            cursor[i] = i
            cursor.close()
            stats = self.session.open_cursor('statistics:' + uri)
            self.assertEqual(stats[stat.dsrc.cursor_insert][2], 1)
            stats.close()

class test_stat21_invalid(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        (str(value), dict(value=value)) for value in ('-1', '0', '1025', 'abc', '1.5')])

    def test_invalid_buckets(self):
        os.mkdir('invalid')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.wiredtiger_open('invalid',
                'create,statistics_dhandle_buckets={}'.format(self.value)),
            '/statistics_dhandle_buckets/')
