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

# test_layered_async_stepdown09.py
#    Stress the async step-down transition: one thread walks the connection through every
#    step-down phase while a workload thread hammers creates, drops, inserts and reads the whole
#    way through. The workload records its own ground truth, tolerates only the errors the
#    transition is allowed to inflict, and after a step-up every surviving table must serve its
#    exact contents to the leader and to a fresh follower. Runs in both the schema-epoch and the
#    epoch-less world.

import itertools, random, threading, time, wiredtiger, wtthread, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_async_stepdown09(
  LayeredStepdownMixin, wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__

    table_config = 'key_format=S,value_format=S'

    # Both worlds run with precise checkpoints, which disaggregated storage expects even from
    # clients that never publish. Only the schema epochs differ between the two worlds.
    base = 'statistics=(all),precise_checkpoint=true,'
    conn_config = base + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = base + 'disaggregated=(role="follower",lose_all_my_data=true)'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    worlds = [
      ('epoch', dict(use_epochs=True)),
      ('legacy', dict(use_epochs=False)),
    ]
    scenarios = make_scenarios(disagg_storages, worlds)

    # How long the workload runs against each phase of the transition.
    phase_sleep = 10.0 if wttest.islongtest() else 1.0

    # Bound the table population so the final verification stays cheap.
    table_cap = 100 if wttest.islongtest() else 40

    def uri(self, name):
        return f'layered:{self.test_name}_{name}'

    # A commit timestamp. The cutoff comes from the same counter, so every timestamp handed out
    # after the step-down timestamp is set lies above it and the timestamp validations cannot
    # fire: the only commit failure left is the straddle rollback, which is the point.
    def alloc_ts(self):
        with self.ts_lock:
            return next(self.ts_counter)

    def setup_stress_state(self):
        self.ts_lock = threading.Lock()
        self.ts_counter = itertools.count(10)
        self.epoch_counter = itertools.count(20)
        self.name_counter = itertools.count()
        # Ground truth, only ever written by the workload thread: uri -> the per-commit history
        # and the publish epoch. The expected rows of any snapshot replay from the history.
        self.tables = {}
        self.seed_uris = set()
        self.audited_drops = set()
        self.worker_errors = []
        self.cutoff = None
        self.demoting = False
        self.done = threading.Event()

    # Publish a schema change in the epoch world, which the epoch-less world has no notion of.
    # Returns the epoch used, allocating a fresh one unless the caller pins it.
    def publish_if_epochs(self, uri, epoch=None, session=None):
        if not self.use_epochs:
            return 0
        if epoch is None:
            epoch = next(self.epoch_counter)
        self.publish(uri, epoch, session=session)
        return epoch

    def setup_seed_tables(self):
        """
        Create a few tables the step-down finds already covered by a checkpoint. In the epoch
        world these are the only tables the workload may write below the eventual cutoff: a
        checkpoint refuses stable data on a table it does not cover.
        """
        if self.use_epochs:
            self.set_stable_epoch(10)
        self.set_global_ts(1, 1)

        for _ in range(3):
            uri = self.uri(f'w{next(self.name_counter)}')
            self.session.create(uri, self.table_config)
            epoch = self.publish_if_epochs(uri)
            rows = {f'k{n}': 'seed' for n in range(5)}
            ts = self.alloc_ts()
            self.write_at(uri, rows, ts)
            self.tables[uri] = {'history': [(ts, rows)], 'epoch': epoch}
            self.seed_uris.add(uri)

        if self.use_epochs:
            self.set_stable_epoch(max(info['epoch'] for info in self.tables.values()))
        self.leader_checkpoint(self.alloc_ts())

    def workload_create(self, wsession, rng):
        if len(self.tables) >= self.table_cap:
            return
        uri = self.uri(f'w{next(self.name_counter)}')
        wsession.create(uri, self.table_config)
        epoch = self.publish_if_epochs(uri, session=wsession)
        self.tables[uri] = {'history': [], 'epoch': epoch}

    def workload_drop(self, wsession, rng):
        if len(self.tables) <= 2:
            return
        uri = rng.choice(list(self.tables))
        try:
            # A single attempt, never retried: a table holding unpublished data stays EBUSY until
            # a checkpoint no part of the workload will take, so a retry loop would live-lock.
            wsession.drop(uri, None)
        except wiredtiger.WiredTigerError as e:
            # Tolerate EBUSY only: unpublished data, or a checkpoint holding the data handle.
            if not self.is_busy(e):
                raise
            return
        info = self.tables.pop(uri)
        # Only a subset of drops is guaranteed to leave the shared metadata, so only that subset
        # is audited. A drop on the demoted node has no relay to carry it back to a leader in
        # this single-process harness. The epoch world replays every queued remove after a
        # step-up, but the epoch-less step-up rebuilds shared metadata from a best-effort local
        # metadata scan, which cannot see a table dropped after the final leader checkpoint.
        if not (self.demoting or (not self.use_epochs and self.cutoff is not None)):
            self.audited_drops.add(uri)
        # A drop of an uncovered create is published at the create's own epoch, so the two queue
        # entries cancel and the covering checkpoint cannot meet a create whose drop was
        # published above it. A covered seed table's create left the queue long ago, so its drop
        # takes a fresh epoch, which the final covering checkpoint reaches.
        self.publish_if_epochs(
            uri, epoch=None if uri in self.seed_uris else info['epoch'], session=wsession)

    # The table an insert may target. Before the step-down timestamp exists, a commit may land
    # below the eventual cutoff, and in the epoch world stable data on an uncovered table is
    # refused by the checkpoint. Once the cutoff is set, every commit timestamp lies above it
    # and any table is fair game.
    def insert_target(self, rng):
        with self.ts_lock:
            window_open = self.cutoff is not None
        if self.use_epochs and not window_open:
            candidates = [u for u in self.tables if u in self.seed_uris]
        else:
            candidates = list(self.tables)
        return rng.choice(candidates) if candidates else None

    # Commit the running transaction at a fresh timestamp. Allocating and committing under the
    # lock, serialized against the step-down thread's timestamp calls, means the commit timestamp
    # can never trail the cutoff or the advancing stable timestamp.
    def commit_at_next_ts(self, wsession):
        with self.ts_lock:
            ts = next(self.ts_counter)
            wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        return ts

    def workload_insert(self, wsession, rng):
        uri = self.insert_target(rng)
        if uri is None:
            return
        kvs = {f'k{rng.randrange(100)}': f'v{n}' for n in range(rng.randrange(1, 11))}

        cursor = wsession.open_cursor(uri)
        wsession.begin_transaction()
        ts = None
        resolved = False
        try:
            for k, v in kvs.items():
                cursor[k] = v
            # Hold some transactions open so they are in flight when a phase boundary lands on
            # them, rather than every transaction beginning and committing inside one phase.
            if rng.random() < 0.3:
                time.sleep(rng.uniform(0, 0.2))
            resolved = True
            if rng.random() < 0.1:
                wsession.rollback_transaction()
            else:
                ts = self.commit_at_next_ts(wsession)
        except wiredtiger.WiredTigerError as e:
            # Tolerate WT_ROLLBACK only: a straddle of the step-down boundary, or a conflict. A
            # failed commit has already resolved the transaction, an earlier failure has not.
            if not self.is_rollback(e):
                raise
            if not resolved:
                wsession.rollback_transaction()
        cursor.close()
        if ts is not None:
            self.tables[uri]['history'].append((ts, kvs))

    # The rows a snapshot at read_ts is entitled to see, replayed from the commit history. With
    # no timestamp, everything the workload ever committed.
    def rows_at(self, info, read_ts=None):
        rows = {}
        for ts, kvs in info['history']:
            if read_ts is None or ts <= read_ts:
                rows.update(kvs)
        return rows

    # Pick a snapshot for a read and the rows it must see: either the newest state without a
    # timestamp, or an exact historical snapshot at one of the table's own commit timestamps.
    # Both are exact: the workload thread is the only writer, so nothing moves underneath the
    # expectation.
    def read_snapshot(self, info, rng):
        if info['history'] and rng.random() < 0.5:
            ts = rng.choice(info['history'])[0]
            return self.rows_at(info, ts), 'read_timestamp=' + self.timestamp_str(ts)
        return self.rows_at(info), None

    def workload_read(self, wsession, rng):
        if not self.tables:
            return
        uri = rng.choice(list(self.tables))
        expected, config = self.read_snapshot(self.tables[uri], rng)

        cursor = wsession.open_cursor(uri)
        wsession.begin_transaction(config)
        # Hold some snapshots open across a phase boundary before reading through them.
        if rng.random() < 0.2:
            time.sleep(rng.uniform(0, 0.2))
        actual = None
        try:
            actual = {k: v for k, v in cursor}
        except wiredtiger.WiredTigerError as e:
            if not self.is_rollback(e):
                raise
        wsession.rollback_transaction()
        cursor.close()
        if actual is not None:
            self.assertEqual(actual, expected, f'{uri} served the wrong rows')

    def workload_move_stable(self, wsession, rng):
        # Keep the stable timestamp moving underneath the workload, the way a live system would.
        # Only before the step-down timestamp exists: once it is set, stable is the transition's
        # to manage, and it must not advance past the cutoff. A fresh counter value keeps every
        # later commit above stable, and taking the lock keeps the call ordered against the
        # transition's own timestamp calls.
        with self.ts_lock:
            if self.cutoff is None:
                self.conn.set_timestamp(
                    'stable_timestamp=' + self.timestamp_str(next(self.ts_counter)))

    def workload(self):
        """
        Hammer a random mix of operations until told to stop. Any error beyond the small set the
        transition is allowed to inflict is recorded and fails the test after the join, since an
        assertion raised on this thread cannot fail the test by itself.
        """
        ops = {
          'insert': (50, self.workload_insert),
          'read': (25, self.workload_read),
          'create': (12, self.workload_create),
          'drop': (8, self.workload_drop),
          'stable': (5, self.workload_move_stable),
        }
        wsession = self.conn.open_session('')
        rng = random.Random(42)
        op = None
        try:
            while not self.done.is_set():
                time.sleep(0.002)
                op = rng.choices(list(ops), weights=[w for w, _ in ops.values()])[0]
                ops[op][1](wsession, rng)
        except Exception as e:
            self.worker_errors.append(f'{op}: {e!r}')
        wsession.close()

    def step_down_in_phases(self):
        """
        Walk the connection through every phase of the planned step-down, pausing after each so
        the workload runs against it: the open window, the stable advance, the step-down
        checkpoint and the demotion itself.
        """
        time.sleep(self.phase_sleep)
        with self.ts_lock:
            self.cutoff = next(self.ts_counter)
            self.set_step_down_ts(self.cutoff)
        time.sleep(self.phase_sleep)
        with self.ts_lock:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.cutoff))
        time.sleep(self.phase_sleep)
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        time.sleep(self.phase_sleep)
        self.demoting = True
        self.step_down()
        time.sleep(self.phase_sleep)

    # Step back up and take checkpoints covering everything the workload published. The second
    # checkpoint matters for drops, which are two-phase: the first checkpoint trims, the next one
    # makes the drop durable in the shared metadata.
    def step_up_and_cover(self):
        self.step_up()
        if self.use_epochs:
            self.set_stable_epoch(next(self.epoch_counter))
        final_ts = self.alloc_ts()
        self.leader_checkpoint(final_ts)
        self.leader_checkpoint(self.alloc_ts())
        return final_ts

    # Every surviving table serves its exact rows through the given session.
    def assert_rows_served(self, final_ts, session, where):
        for uri, info in self.tables.items():
            self.assertEqual(self.read_kvs_at(uri, final_ts, session=session),
                self.rows_at(info), f'{uri} served the wrong rows on the {where}')

    # Every surviving table got its stable constituent built, no table exists beyond the
    # expected set, and every audited drop left the shared metadata.
    def verify_leader_state(self, final_ts):
        self.assert_rows_served(final_ts, self.session, 'leader')
        for uri in self.tables:
            self.assertTrue(self.stable_constituent_exists(self.conn, uri),
                f'{uri} has no stable constituent after the covering checkpoint')
        self.assert_no_unexpected_tables(self.conn, list(self.tables))
        for uri in self.audited_drops:
            self.assertFalse(self.uri_in_shared_metadata(self.conn, uri),
                f'dropped {uri} still advertised in the shared metadata')

    # A fresh follower picking up the covering checkpoint serves every surviving table's rows.
    def verify_follower_reads(self, final_ts):
        conn_follow, session_follow = self.open_follower()
        self.assert_rows_served(final_ts, session_follow, 'fresh follower')
        self.close_follower(conn_follow, session_follow)

    def test_stepdown_under_workload(self):
        self.setup_stress_state()
        self.setup_seed_tables()

        worker = wtthread.Thread(target=self.workload)
        worker.start()
        try:
            self.step_down_in_phases()
        finally:
            self.done.set()
            worker.join()

        self.ignoreStderrPatternIfExists(
            'straddled the step-down timestamp|must be checkpointed before it can be dropped|'
            'currently holding the data handle')
        self.assertEqual(self.worker_errors, [])

        final_ts = self.step_up_and_cover()
        self.verify_leader_state(final_ts)
        self.verify_follower_reads(final_ts)
