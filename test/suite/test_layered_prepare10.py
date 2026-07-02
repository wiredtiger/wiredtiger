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

from contextlib import closing, contextmanager
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
import wttest
from wiredtiger import WiredTigerError


@disagg_test_class
class test_layered_prepare10(wttest.WiredTigerTestCase):
    """
    A reader stalls mid-walk on a prepared update, leaving the ingest cursor with no key but still
    positioned. While it is stalled, the parked stable key is deleted by a newer checkpoint that
    the follower picks up. Once the conflict resolves, the walk must still return every other
    visible key.
    """

    uri = f"layered:{__qualname__}"
    BASE_CONFIG = "statistics=(all),precise_checkpoint=true,preserve_prepared=true"
    conn_config = BASE_CONFIG + ',disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)

    # Stable keys serve all scenarios.
    STABLE_KEYS = (4, 6, 8)
    scenarios = make_scenarios(
        disagg_storages,
        [
            # Merged walk 1, [3 prepared], 4, 5, 6, 8. Returns 1, then stalls on the prepare,
            # with the stable block (4, 6, 8) still ahead, so a dropped stable key is observable.
            (
                "forward",
                dict(
                    forward=True,
                    ingest=(1, 5),
                    prepared=3,
                    all_keys=[1, 4, 5, 6, 8],
                ),
            ),
            # Mirror is 11, [9 prepared], 8, 7, 6, 4. Only ingest and prepared flip per direction.
            (
                "backward",
                dict(
                    forward=False,
                    ingest=(7, 11),
                    prepared=9,
                    all_keys=[11, 8, 7, 6, 4],
                ),
            ),
        ],
    )

    def setUp(self):
        super().setUp()
        self._setup_leader()
        self._setup_follower()

    def _setup_leader(self):
        oldest = self.timestamp_str(10)
        self.conn.set_timestamp(f"oldest_timestamp={oldest},stable_timestamp={oldest}")
        self.session.create(self.uri, "key_format=i,value_format=S")
        stable_ts = 20
        for key in self.STABLE_KEYS:
            self._commit(self.session, key, "stable", stable_ts)
        self._leader_checkpoint(stable_ts)

    def _setup_follower(self):
        self.follower = self.wiredtiger_open(
            "follower",
            f"{self.extensionsConfig()},create,{self.BASE_CONFIG},"
            f'disaggregated=(role="follower")',
        )
        self.disagg_advance_checkpoint(self.follower)
        with closing(self.follower.open_session()) as session:
            for key in self.ingest:
                self._commit(session, key, "ingest", 22)

    def _commit(self, session, key, value, ts):
        with (
            wttest.open_cursor(session, self.uri) as cursor,
            self.transaction(session=session, commit_timestamp=ts),
        ):
            cursor[key] = value

    def _remove(self, session, key, ts):
        with (
            wttest.open_cursor(session, self.uri) as cursor,
            self.transaction(session=session, commit_timestamp=ts),
        ):
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)

    def _leader_checkpoint(self, stable):
        self.conn.set_timestamp(f"stable_timestamp={self.timestamp_str(stable)}")
        self.session.checkpoint()

    def _prepared_session(self):
        session = self.follower.open_session()
        session.begin_transaction()
        with wttest.open_cursor(session, self.uri) as cursor:
            cursor[self.prepared] = "prepared"
        session.prepare_transaction(
            f"prepare_timestamp={self.timestamp_str(25)},"
            f"prepared_id={self.prepared_id_str(1)}"
        )
        return session

    @contextmanager
    def _prepare_stalled_reader(self, seen_keys, ts):
        """Walk a reader onto the prepared key so the ingest cursor stalls with no key."""
        with (
            closing(self._prepared_session()) as prep_session,
            closing(self.follower.open_session()) as reader,
            self.transaction(
                session=reader,
                read_timestamp=ts,
                rollback=True,
            ),
            closing(reader.open_cursor(self.uri)) as read_cursor,
        ):
            step = read_cursor.next if self.forward else read_cursor.prev

            # Surface the first key, then stall advancing onto the prepared update.
            self.assertEqual(step(), 0)
            seen_keys.append(read_cursor.get_key())
            self.assertRaisesException(
                WiredTigerError,
                step,
                "/conflict with a prepared update/",
            )

            try:
                yield
            finally:
                prep_session.rollback_transaction(
                    f"rollback_timestamp={self.timestamp_str(35)}"
                )
            while step() == 0:
                seen_keys.append(read_cursor.get_key())

    def test_reopen_notfound_while_ingest_stalled(self):
        """The parked stable key is deleted in a newer checkpoint."""
        # The stable cursor parks on the first stable key the walk reaches in this direction.
        parked_key = self.STABLE_KEYS[0] if self.forward else self.STABLE_KEYS[-1]

        # Simulate the oplog delete landing in the follower's ingest before the read (a tombstone),
        # so the key stays masked regardless of the stable checkpoint.
        delete_ts = 28
        with closing(self.follower.open_session()) as session:
            self._remove(session, parked_key, delete_ts)

        seen_keys = []
        read_ts = 30
        with self._prepare_stalled_reader(seen_keys, read_ts):
            # The same delete lands in a newer checkpoint the follower then picks up mid-stall.
            self._remove(self.session, parked_key, delete_ts)
            self._leader_checkpoint(read_ts)
            self.disagg_advance_checkpoint(self.follower)

        # The deleted key should not be visible.
        expected = [k for k in self.all_keys if k != parked_key]
        self.assertEqual(seen_keys, expected, "the walk lost a visible key")


if __name__ == "__main__":
    wttest.run()
