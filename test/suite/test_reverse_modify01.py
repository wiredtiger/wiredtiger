#!/usr/bin/env python
import unittest, wiredtiger, wttest, time

def timestamp_str(t):
    return '%x' % t

# test_reverse_modify01.py
# Verify reverse modify traversal after eviction
class test_reverse_modify01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=2MB,statistics=(all),eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'

    def test_reverse_modifies_fails_visibility_check_without_timestamps(self):
        uri = "table:test_reverse_modify01_notimestamp"
        create_params = 'value_format=S,key_format=i'
        value1 = 'abcedfghijklmnopqrstuvwxyz' * 5
        value2 = 'b' * 100
        value3 = 'c' * 100
        value4 = 'd' * 100
        valuebig = 'e' * 1000
        self.session.create(uri, create_params)
        cursor = self.session.open_cursor(uri)

        session2 = self.setUpSessionOpen(self.conn)
        session2.create(uri, create_params)
        cursor2 = session2.open_cursor(uri)

        # Insert a full value.
        self.session.begin_transaction()
        cursor[1] = value1
        self.session.commit_transaction()

        # Insert a modify
        self.session.begin_transaction()
        cursor.set_key(1)
        cursor.modify([wiredtiger.Modify('A', 130, 1)])
        self.session.commit_transaction()

        # Validate that we do see the correct value.
        session2.begin_transaction()
        cursor2.set_key(1)
        cursor2.search()
        self.assertEquals(cursor2.get_value(),  value1 + 'A')
        session2.commit_transaction()

        # Begin transaction on session 2 so it sees and current snap_min snap_max
        session2.begin_transaction()

        # reset the cursor
        cursor2.reset()

        # Insert two more values
        self.session.begin_transaction()
        cursor.set_key(1)
        cursor[1] = value3
        self.session.commit_transaction()

        self.session.begin_transaction()
        cursor.set_key(1)
        cursor[1] = value4
        self.session.commit_transaction()

        self.session.checkpoint()

        # Insert a whole bunch of data into the table to force wiredtiger to evict data
        # from the previous table.
        self.session.begin_transaction()
        for i in range(2, 10000):
            cursor[i] = valuebig
        self.session.commit_transaction()

        # Try to find the value we saw earlier
        cursor2.set_key(1)
        cursor2.search()
        self.session.breakpoint()
        self.assertEquals(cursor2.get_value(), value1 + 'A')

if __name__ == '__main__':
    wttest.run()