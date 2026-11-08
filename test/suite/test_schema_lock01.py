#!/usr/bin/env python3
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
# test_schema_lock01.py
#   Reopen swept handles from several threads while other threads create and drop tables and
#   take checkpoints.

import os, random, threading, time
import wiredtiger, wttest, wtthread
from wiredtiger import stat
from wtscenario import make_scenarios

@wttest.skip_for_hook("disagg", "Disagg doesn't sweep layered dhandles")
class test_schema_lock01(wttest.WiredTigerTestCase):
    conn_config = 'cache_cursors=false,statistics=(all),' + \
        'file_manager=(close_handle_minimum=0,close_idle_time=1,close_scan_interval=1)'

    ntables = 200
    nkeys = 20
    nreaders = 4
    nextra = 8
    # Readers work on one slice of the tables at a time so the other slices go idle long enough
    # to be swept, then come back to them.
    nslices = 4
    slice_secs = 1.5
    min_run_secs = 8
    deadline_secs = 60

    scenarios = make_scenarios([
        ('no_checkpoint', dict(checkpoint_thread=False)),
        ('checkpoint', dict(checkpoint_thread=True)),
    ])

    def stable_uri(self, i):
        return 'table:stable%03d' % i

    def extra_uri(self, i):
        return 'table:extra%d' % i

    def expected_value(self, table, key):
        return 'value-%d-%s' % (table, key)

    def schema_error(self, e):
        # Opening a table that is being dropped or created, or dropping a table that is in use.
        msg = str(e)
        return 'No such file' in msg or 'busy' in msg

    def reader(self, tid, start):
        rng = random.Random(self.seed + tid)
        session = self.conn.open_session()
        tables_per_slice = self.ntables // self.nslices
        try:
            while not self.stop.is_set():
                if rng.randrange(10) == 0:
                    try:
                        c = session.open_cursor(self.extra_uri(rng.randrange(self.nextra)))
                        c.next()
                        c.close()
                    except wiredtiger.WiredTigerError as e:
                        if not self.schema_error(e):
                            raise
                    continue

                cur_slice = int((time.monotonic() - start) / self.slice_secs) % self.nslices
                table = cur_slice * tables_per_slice + rng.randrange(tables_per_slice)
                c = session.open_cursor(self.stable_uri(table))
                key = 'k%04d' % rng.randrange(self.nkeys)
                self.assertEqual(c[key], self.expected_value(table, key))
                n = len(self.written[tid])
                key = 'r%d-%d' % (tid, n)
                c[key] = self.expected_value(table, key)
                c.close()
                self.written[tid].append((table, key))
        except Exception as e:
            self.failures.append('reader %d: %r' % (tid, e))
        finally:
            session.close()

    def schema_worker(self):
        rng = random.Random(self.seed + 1000)
        session = self.conn.open_session()
        try:
            while not self.stop.is_set():
                uri = self.extra_uri(rng.randrange(self.nextra))
                try:
                    session.create(uri, 'key_format=S,value_format=S')
                    c = session.open_cursor(uri)
                    for i in range(10):
                        c['k%d' % i] = 'v%d' % i
                    c.close()
                except wiredtiger.WiredTigerError as e:
                    if not self.schema_error(e):
                        raise
                # A drop fails with EBUSY while a reader has the table open or while the table has
                # updates that are not yet globally visible. After a checkpoint only the first case
                # remains.
                while not self.stop.is_set():
                    try:
                        session.drop(uri)
                        self.schema_ops += 1
                        break
                    except wiredtiger.WiredTigerError as e:
                        if not self.schema_error(e):
                            raise
                        session.checkpoint()
        except Exception as e:
            self.failures.append('schema worker: %r' % e)
        finally:
            session.close()

    def checkpointer(self):
        session = self.conn.open_session()
        try:
            while not self.stop.is_set():
                session.checkpoint()
                self.checkpoints += 1
                # Continuous checkpoints would keep the sweep server from closing handles.
                time.sleep(0.3)
        except Exception as e:
            self.failures.append('checkpoint: %r' % e)
        finally:
            session.close()

    def test_schema_lock(self):
        self.seed = int(os.environ.get('WT_TEST_SEED', time.time_ns() % (2 ** 32)))
        self.pr('seed %d' % self.seed)
        self.ignoreStderrPatternIfExists('No such file|busy')

        for t in range(self.ntables):
            uri = self.stable_uri(t)
            self.session.create(uri, 'key_format=S,value_format=S')
            c = self.session.open_cursor(uri)
            for k in range(self.nkeys):
                key = 'k%04d' % k
                c[key] = self.expected_value(t, key)
            c.close()

        self.stop = threading.Event()
        self.failures = []
        self.written = [[] for _ in range(self.nreaders)]
        self.schema_ops = 0
        self.checkpoints = 0

        close_base = self.get_stat(stat.conn.dh_sweep_expired_close)
        read_base = self.get_stat(stat.conn.lock_schema_read_count)
        write_base = self.get_stat(stat.conn.lock_schema_write_count)

        start = time.monotonic()
        threads = [wtthread.Thread(target=self.reader, args=(i, start))
            for i in range(self.nreaders)]
        threads.append(wtthread.Thread(target=self.schema_worker))
        if self.checkpoint_thread:
            threads.append(wtthread.Thread(target=self.checkpointer))
        for t in threads:
            t.start()

        # Each table has a table and a file handle. More expired closes than there are handles
        # means at least one handle was swept, reopened and swept again. Each drop waits for a
        # checkpoint, so also wait for the schema worker and checkpointer to make progress.
        target = 2 * self.ntables + 1
        try:
            while True:
                closed = self.get_stat(stat.conn.dh_sweep_expired_close) - close_base
                elapsed = time.monotonic() - start
                progress = self.schema_ops > 0 and (
                    not self.checkpoint_thread or self.checkpoints > 0)
                if (closed >= target and progress and elapsed >= self.min_run_secs) or \
                    self.failures:
                    break
                self.assertLess(elapsed, self.deadline_secs,
                    'seed %d: after %d seconds %d expired closes (expected %d), '
                    '%d schema ops, %d checkpoints' %
                    (self.seed, elapsed, closed, target, self.schema_ops, self.checkpoints))
                time.sleep(0.5)
        finally:
            self.stop.set()
            for t in threads:
                t.join()

        self.pr('expired closes %d, schema ops %d, checkpoints %d, reader ops %s' %
            (closed, self.schema_ops, self.checkpoints, [len(w) for w in self.written]))
        self.assertEqual(self.failures, [], 'seed %d' % self.seed)
        self.assertGreater(self.schema_ops, 0, 'seed %d' % self.seed)
        if self.checkpoint_thread:
            self.assertGreater(self.checkpoints, 0, 'seed %d' % self.seed)
        self.assertGreater(
            self.get_stat(stat.conn.lock_schema_read_count), read_base, 'seed %d' % self.seed)
        self.assertGreater(
            self.get_stat(stat.conn.lock_schema_write_count), write_base, 'seed %d' % self.seed)

        for t in range(self.ntables):
            c = self.session.open_cursor(self.stable_uri(t))
            for k in range(self.nkeys):
                key = 'k%04d' % k
                self.assertEqual(c[key], self.expected_value(t, key), 'seed %d' % self.seed)
            c.close()
        for tid in range(self.nreaders):
            self.assertGreater(len(self.written[tid]), 0, 'seed %d' % self.seed)
            for table, key in self.written[tid]:
                c = self.session.open_cursor(self.stable_uri(table))
                self.assertEqual(c[key], self.expected_value(table, key),
                    'seed %d: reader %d key %s' % (self.seed, tid, key))
                c.close()

if __name__ == '__main__':
    wttest.run()
