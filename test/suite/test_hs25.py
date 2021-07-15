import wttest

def timestamp_str(t):
    return '%x' % t

# test_hs25.py
# Ensure updates structure is correct when processing each key.
class test_hs25(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB'
    session_config = 'isolation=snapshot'
    uri = 'table:test_hs25'

    def test_insert_updates_hs(self):
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S')
        s = self.conn.open_session()

        # Update the first key.
        cursor1 = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor1[1] = 'a'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Update the second key.
        self.session.begin_transaction()
        cursor1[2] = 'a'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))
        self.session.begin_transaction()
        cursor1[2] = 'b'
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Prepared update on the first key.
        self.session.begin_transaction()
        cursor1[1] = 'b'
        cursor1[1] = 'c'
        self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(4))

        # Run eviction cursor.
        s.begin_transaction('ignore_prepare=true')
        evict_cursor = s.open_cursor(self.uri, None, 'debug=(release_evict)')
        self.assertEqual(evict_cursor[1], 'a')
        self.assertEqual(evict_cursor[2], 'b')
        s.rollback_transaction()
        self.session.rollback_transaction()
