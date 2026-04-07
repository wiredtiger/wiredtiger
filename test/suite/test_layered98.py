import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered98.py
#
# Regression tests: when two transactions share an open layered cursor and the read_timestamp
# changes between them, according to the current implementation, the "alternate" constituent cursor
# (the one not selected as current on the previous call, left parked at the next position)
# must be re-searched under the new transaction's snapshot before its cached value is used;
# otherwise it silently returns a stale result.
#
# Four scenarios cover the full (next/prev) x (stable-alternate/ingest-alternate)
# matrix.

@disagg_test_class
class test_layered98(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered98'

    disagg_storages = gen_disagg_storages('test_layered98', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + ',disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    # --------------------------------------------------------------------------
    # Scenario 1: next(), stable is alternate, read_timestamp drops.
    #
    #   Stable (checkpoint)       Ingest (follower)
    #   +-----+----+-------+      +-----+----+-------+
    #   | key | ts | value |      | key | ts | value |
    #   +-----+----+-------+      +-----+----+-------+
    #   |   2 |  1 |   1   |      |   1 |  3 |   3   |
    #   |   2 |  2 |   2   |      +-----+----+-------+
    #   +-----+----+-------+
    #
    #   T1 (read_timestamp=3): next() -> key=1, value=3.
    #   T2 (read_timestamp=1): next() -> key=2, value=1.
    # --------------------------------------------------------------------------
    def test_talbe_scan_with_different_read_ts_stable_next(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        # Leader: write key=2 at ts=1 then update at ts=2.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['2'] = '1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        self.session.begin_transaction()
        c['2'] = '2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        c.close()

        # Checkpoint and pick it up on the follower.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(2))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Follower ingest: key=1 sorts before key=2.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf['1'] = '3'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(3))
        cf.close()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(3))
        cursor = self.session_follow.open_cursor(self.uri)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), '1')
        self.assertEqual(cursor.get_value(), '3')
        self.session_follow.commit_transaction()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(1))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), '2')
        self.assertEqual(cursor.get_value(), '1')  # bug: returns '2'
        cursor.close()
        self.session_follow.commit_transaction()

    # --------------------------------------------------------------------------
    # Scenario 2: prev(), stable is alternate, read_timestamp drops.
    #
    #   Stable (checkpoint)       Ingest (follower)
    #   +-----+----+-------+      +-----+----+-------+
    #   | key | ts | value |      | key | ts | value |
    #   +-----+----+-------+      +-----+----+-------+
    #   |   1 |  1 |   1   |      |   2 |  3 |   3   |
    #   |   1 |  2 |   2   |      +-----+----+-------+
    #   +-----+----+-------+
    #
    #   T1 (read_timestamp=3): prev() -> key=2, value=3.
    #   T2 (read_timestamp=1): prev() -> key=1, value=1.
    # --------------------------------------------------------------------------
    def test_talbe_scan_with_different_read_ts_stable_prev(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        # Leader: write key=1 at ts=1 then update at ts=2.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['1'] = '1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        self.session.begin_transaction()
        c['1'] = '2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        c.close()

        # Checkpoint and pick it up on the follower.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(2))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Follower ingest: key=2 sorts after key=1.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf['2'] = '3'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(3))
        cf.close()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(3))
        cursor = self.session_follow.open_cursor(self.uri)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), '2')
        self.assertEqual(cursor.get_value(), '3')
        self.session_follow.commit_transaction()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(1))
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), '1')
        self.assertEqual(cursor.get_value(), '1')  # bug: returns '2'
        cursor.close()
        self.session_follow.commit_transaction()

    # --------------------------------------------------------------------------
    # Scenario 3: next(), ingest is alternate, read_timestamp rises.
    #
    #   Stable (checkpoint)       Ingest (follower)
    #   +-----+----+-------+      +-----+----+-------+
    #   | key | ts | value |      | key | ts | value |
    #   +-----+----+-------+      +-----+----+-------+
    #   |   1 |  1 |   1   |      |   2 |  2 |   2   |
    #   +-----+----+-------+      |   2 |  3 |   3   |
    #                             +-----+----+-------+
    #
    #   T1 (read_timestamp=2): next() -> key=1, value=1.
    #   T2 (read_timestamp=3): next() -> key=2, value=3.
    # --------------------------------------------------------------------------
    def test_talbe_scan_with_different_read_ts_ingest_next(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        # Leader: write key=1 at ts=1.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['1'] = '1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        # Checkpoint and pick it up on the follower.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Follower ingest: key=2 at ts=2, then updated at ts=3.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf['2'] = '2'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        self.session_follow.begin_transaction()
        cf['2'] = '3'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(3))
        cf.close()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(2))
        cursor = self.session_follow.open_cursor(self.uri)
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), '1')
        self.assertEqual(cursor.get_value(), '1')
        self.session_follow.commit_transaction()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(3))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), '2')
        self.assertEqual(cursor.get_value(), '3')  # bug: returns '2'
        cursor.close()
        self.session_follow.commit_transaction()

    # --------------------------------------------------------------------------
    # Scenario 4: prev(), ingest is alternate, read_timestamp rises.
    #
    #   Stable (checkpoint)       Ingest (follower)
    #   +-----+----+-------+      +-----+----+-------+
    #   | key | ts | value |      | key | ts | value |
    #   +-----+----+-------+      +-----+----+-------+
    #   |   2 |  1 |   1   |      |   1 |  2 |   2   |
    #   +-----+----+-------+      |   1 |  3 |   3   |
    #                             +-----+----+-------+
    #
    #   T1 (read_timestamp=2): prev() -> key=2, value=1.
    #   T2 (read_timestamp=3): prev() -> key=1, value=3.
    # --------------------------------------------------------------------------
    def test_talbe_scan_with_different_read_ts_ingest_prev(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session_follow.create(self.uri, 'key_format=S,value_format=S')

        # Leader: write key=2 at ts=1.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['2'] = '1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        # Checkpoint and pick it up on the follower.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Follower ingest: key=1 at ts=2, then updated at ts=3.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf['1'] = '2'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
        self.session_follow.begin_transaction()
        cf['1'] = '3'
        self.session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(3))
        cf.close()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(2))
        cursor = self.session_follow.open_cursor(self.uri)
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), '2')
        self.assertEqual(cursor.get_value(), '1')
        self.session_follow.commit_transaction()

        self.session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(3))
        self.assertEqual(cursor.prev(), 0)
        self.assertEqual(cursor.get_key(), '1')
        self.assertEqual(cursor.get_value(), '3')  # bug: returns '2'
        cursor.close()
        self.session_follow.commit_transaction()
