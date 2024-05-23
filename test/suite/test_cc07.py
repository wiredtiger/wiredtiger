#!/usr/bin/env python

import wttest

# test_cc07.py
class test_cc07(wttest.WiredTigerTestCase):
    num_tables = 1
    create_params = 'key_format=i,value_format=S'
    table_numkv = 100 * 1000
    uri_prefix = 'table:cc07'
    value_size = 1024

    def populate(self, uri, start_key, num_keys, value_size=1024):
        c = self.session.open_cursor(uri, None)
        for k in range(start_key, num_keys):
            self.session.begin_transaction()
            c[k] = 'k' * value_size
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(k + 1))
        c.close()

    def test_cc(self):

        # Create N tables.
        uris = []
        for i in range(self.num_tables):
            uri = self.uri_prefix + f'_{i}'
            uris.append(uri)
            self.session.create(uri, self.create_params)

        # Append some data.
        for uri in uris:
            self.populate(uri, 0, self.table_numkv)

        session2 = self.conn.open_session()
        long_txn_running = False

        for i in range(10):
            # Stop and start a long running transaction.
            if long_txn_running:
                session2.rollback_transaction()
            # Update the oldest ts to make content obsolete.
            self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(self.table_numkv * (i + 1) // 2))
            session2.begin_transaction()
            long_txn_running = True

            # Append some data.
            for uri in uris:
                self.populate(uri, self.table_numkv * (i + 1), self.table_numkv * (i + 2))

            # Checkpoint with cleanup.
            self.session.checkpoint("debug=(checkpoint_cleanup=true)")
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.table_numkv * (i + 2)))

        session2.rollback_transaction()

