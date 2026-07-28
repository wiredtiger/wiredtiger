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

# Concurrency stress for snapshot reads on a follower racing checkpoint
# pickups: reader threads at snapshot isolation without a read timestamp
# continuously open fresh cursors while checkpoints stream through the
# deferral queue. Every reader transaction must observe one atomic version
# across all keys, repeatably, and with deferral enabled must never be
# refused. Any torn, drifting, or vanishing read fails the test.

import threading
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

NKEYS = 10
NREADERS = 4
NVERSIONS = 40

@disagg_test_class
class test_layered_follower21(wttest.WiredTigerTestCase):
    test_name = __qualname__

    uri = f'layered:{test_name}'
    table_config = 'key_format=S,value_format=S'
    conn_base_config = ',create,statistics=(all),'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def follower_config(self):
        return self.extensionsConfig() + self.conn_base_config + \
            'disaggregated=(role="follower",checkpoint_deferral=true)'

    def put_version(self, session, version, ts):
        # All keys move to the new version in one transaction: readers must
        # never see a mix.
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        for i in range(NKEYS):
            cursor[f'key_{i}'] = f'v_{version}'
        session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        cursor.close()

    def reader(self, conn, stop, errors):
        try:
            session = conn.open_session('')
            while not stop.is_set():
                session.begin_transaction()
                seen = None
                # Two passes over all keys, each through a freshly opened
                # cursor, so every pass exercises a stable bind.
                for _ in range(2):
                    cursor = session.open_cursor(self.uri)
                    for i in range(NKEYS):
                        value = cursor[f'key_{i}']
                        if seen is None:
                            seen = value
                        elif value != seen:
                            errors.append(f'torn or drifting read: {value} != {seen}')
                            stop.set()
                    cursor.close()
                session.rollback_transaction()
            session.close()
        except wiredtiger.WiredTigerError as e:
            # With deferral enabled a pickup must never refuse a reader.
            errors.append(f'reader refused or failed: {e}')
            stop.set()

    def test_readers_race_pickups(self):
        self.session.create(self.uri, self.table_config)
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')
        self.put_version(self.session, 0, 10)
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        conn_follow = self.wiredtiger_open('follower', self.follower_config())
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, self.table_config)
        self.put_version(session_follow, 0, 10)
        self.disagg_advance_checkpoint(conn_follow)

        stop = threading.Event()
        errors = []
        readers = [threading.Thread(target=self.reader, args=(conn_follow, stop, errors))
                   for _ in range(NREADERS)]
        for t in readers:
            t.start()

        # Stream versions and checkpoints through the deferral queue while
        # the readers run.
        try:
            for version in range(1, NVERSIONS):
                ts = 10 + version
                self.put_version(self.session, version, ts)
                self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
                self.session.checkpoint()
                self.put_version(session_follow, version, ts)
                self.disagg_advance_checkpoint(conn_follow)
                if stop.is_set():
                    break
        finally:
            stop.set()
            for t in readers:
                t.join()

        self.assertEqual(errors, [])

        session_follow.close()
        conn_follow.close()
