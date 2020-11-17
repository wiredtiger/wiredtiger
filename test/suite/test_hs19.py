import time, wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

# test_hs19.py
# Ensure eviction doesn't clear the history store again after checkpoint has done so because of the same update without timestamp.
class test_hs19(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=5MB,eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'

    def test_hs19(self):
        uri = 'table:test_hs19'
        junk_uri = 'table:junk'
        self.session.create(uri, 'key_format=S,value_format=S')
        session2 = self.conn.open_session()
        session2.create(junk_uri, 'key_format=S,value_format=S')
        cursor2 = session2.open_cursor(junk_uri)
        cursor = self.session.open_cursor(uri)
        self.conn.set_timestamp(
            'oldest_timestamp=' + timestamp_str(1) + ',stable_timestamp=' + timestamp_str(1))

        value1 = 'a' * 500
        value2 = 'b' * 500
        value3 = 'c' * 50000

        # Insert an update without timestamp.
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction()

        # Do 2 modifies.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify('B', 100, 1)]
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(1))

        # This is the modify we will eventually try and reconstruct. It will get written as the on
        # disk value by eviction later on.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify('C', 101, 1)]
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Start a transaction to pin back the reconciliation last running value.
        session2.begin_transaction()
        cursor2[str(1)] = value3

        # Insert a modify ahead of our reconstructed modify, this one will be used unintentionally
        # to reconstruct the final value, corrupting the resulting value.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify('AAAAAAAAAA', 102, 10)]
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Insert a modify to get written as the on disk value by checkpoint.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify('D', 102, 1)]
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(4))

        # Checkpoint such that all modifies get written out to the history store and the latest
        # modify gets written to the on disk value.
        self.session.checkpoint('use_timestamp=true')

        # Add an additional modify so that when eviction sees this page it will rewrite it as it's
        # dirty.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify('E', 103, 1)]
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        # Evict the page, this utilises a modified reserve call that triggers eviction of the
        # underlying page.
        # First deposition the first cursor, so the page can be evicted.
        cursor.reset()
        evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        # Search for the key so we position our cursor on the page that we want to evict.
        evict_cursor.set_key(str(0))
        evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()

        # Construct and test the value as at timestamp 1
        expected = list(value1)
        expected[100] = 'B'
        expected = str().join(expected)

        # Retrieve the value at timestamp 1.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(1))
        cursor.set_key(str(0))
        cursor.search()
        cursor.get_value()

        # Assert that it matches our expected value which it won't.
        self.assertEqual(cursor[str(0)], expected)
        self.session.rollback_transaction()

        # Construct and test the value as at timestamp 2
        expected = list(expected)
        expected[101] = 'C'
        expected = str().join(expected)

        # Retrieve the value at timestamp 1.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(2))
        cursor.set_key(str(0))
        cursor.search()
        cursor.get_value()

        # Assert that it matches our expected value which it won't.
        self.assertEqual(cursor[str(0)], expected)
        self.session.rollback_transaction()

        # Construct and test the value as at timestamp 3
        expected = list(expected)
        for x in range(10):
            expected[102 + x] = 'A'
        expected = str().join(expected)

        # Retrieve the value at timestamp 1.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(3))
        cursor.set_key(str(0))
        cursor.search()
        cursor.get_value()

        # Assert that it matches our expected value which it won't.
        self.assertEqual(cursor[str(0)], expected)
        self.session.rollback_transaction()
