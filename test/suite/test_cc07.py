#!/usr/bin/env python

import wttest
from test_cc01 import test_cc_base

# test_cc07.py
class test_cc07(test_cc_base):

    def populate(self, uri, start_key, num_keys, value_size=1024):
        c = self.session.open_cursor(uri, None)
        for k in range(start_key, num_keys):
            self.session.begin_transaction()
            c[k] = 'k' * value_size
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(k + 1))
        c.close()

    def test_cc(self):
        create_params = 'key_format=i,value_format=S'
        nrows = 10000
        uri = 'table:cc07'
        value_size = 1024

        self.session.create(uri, create_params)

        for i in range(10):
            # Append some data.
            self.populate(uri, nrows * (i), nrows * (i + 1))

            # Checkpoint with cleanup.
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(nrows * (i + 1)))
            self.session.checkpoint("debug=(checkpoint_cleanup=true)")
            self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(nrows * (i + 1)))

        self.session.checkpoint("debug=(checkpoint_cleanup=true)")
        self.wait_for_cc_to_run()

