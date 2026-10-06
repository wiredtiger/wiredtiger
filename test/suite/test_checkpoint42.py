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
#
# test_checkpoint42.py
#   Bulk loads finish while checkpoints run in a loop. A bulk cursor close updates the metadata of
#   its file, and every checkpoint that starts after the close must include the loaded rows. After
#   a crash, a table is either fully loaded or empty, and every table closed before the start of the
#   last completed checkpoint is fully loaded.

import threading, time
import wttest
from helper import simulate_crash_restart
from wtscenario import make_scenarios

@wttest.skip_for_hook("disagg", "bulk cursors are not supported on layered tables")
@wttest.skip_for_hook("tiered", "crash restart copies a local home directory")
class test_checkpoint42(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=100MB'
    nloaders = 4
    nrows = 200
    duration = 5

    scenarios = make_scenarios([
        ('row', dict(key_format='i', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ])

    def uri(self, loader, n):
        return 'table:ckpt42_{}_{}'.format(loader, n)

    def value(self, key):
        return 'value{}'.format(key)

    def loader(self, loader, done, lock, closed, errors):
        try:
            session = self.conn.open_session()
            n = 0
            while not done.is_set():
                uri = self.uri(loader, n)
                session.create(uri,
                    'key_format={},value_format={}'.format(self.key_format, self.value_format))
                cursor = session.open_cursor(uri, None, 'bulk')
                for key in range(1, self.nrows + 1):
                    cursor[key] = self.value(key)
                cursor.close()
                with lock:
                    closed.append(uri)
                n += 1
            session.close()
        except Exception as e:
            errors.append(e)

    def checkpointer(self, done, lock, closed, guaranteed, counts, errors):
        try:
            session = self.conn.open_session()
            while not done.is_set():
                # Tables closed before the checkpoint starts must be in it.
                with lock:
                    nclosed = len(closed)
                session.checkpoint()
                guaranteed[0] = nclosed
                counts[0] += 1
            session.close()
        except Exception as e:
            errors.append(e)

    def test_checkpoint_bulk_close_race(self):
        done = threading.Event()
        lock = threading.Lock()
        closed = []
        guaranteed = [0]
        counts = [0]
        errors = []

        threads = [threading.Thread(target=self.loader, args=(i, done, lock, closed, errors))
            for i in range(self.nloaders)]
        threads.append(threading.Thread(
            target=self.checkpointer, args=(done, lock, closed, guaranteed, counts, errors)))
        for t in threads:
            t.start()
        time.sleep(self.duration)
        done.set()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertGreater(counts[0], 0)
        self.assertGreater(guaranteed[0], 0)
        self.pr('{} checkpoints, {} bulk loads, {} before the last checkpoint'.format(
            counts[0], len(closed), guaranteed[0]))

        simulate_crash_restart(self, '.', 'RESTART')

        # A table created after the last checkpoint does not exist after the restart.
        for i, uri in enumerate(closed):
            nrows = self.count(uri, i < guaranteed[0])
            if i < guaranteed[0]:
                self.assertEqual(nrows, self.nrows, uri)
            else:
                self.assertIn(nrows, (None, 0, self.nrows), uri)
            if nrows is not None:
                self.verifyUntilSuccess(self.session, uri)

    def count(self, uri, must_exist):
        meta = self.session.open_cursor('metadata:')
        meta.set_key(uri)
        exists = meta.search() == 0
        meta.close()
        if not exists:
            self.assertFalse(must_exist, uri)
            return None
        cursor = self.session.open_cursor(uri)
        nrows = 0
        for key, value in cursor:
            self.assertEqual(value, self.value(key))
            nrows += 1
        cursor.close()
        return nrows
