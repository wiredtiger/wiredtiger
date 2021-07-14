import wttest, wiredtiger
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_hs25.py
# Ensure updates structure is correct when processing each key.
class test_hs25(wttest.WiredTigerTestCase):
    def large_updates(self, uri, value, nrows, commit_ts):
        # Update a large number of records, we'll hang if the history store table isn't working.
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            self.session.begin_transaction()
            cursor.set_key(str(i))
            cursor.set_value(str(i) + value)
            self.assertEqual(cursor.update(), 0)
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def prepare_updates(self, uri, value, nrows, prepare_ts):
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            self.session.begin_transaction()
            cursor.set_key(str(i))
            cursor.set_value(str(i) + value)
            self.assertEqual(cursor.update(), 0)
            cursor.set_key(str(i))
            cursor.set_value(value + str(i))
            self.assertEqual(cursor.update(), 0)
            self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(prepare_ts))
            # self.session.commit_transaction('commit_timestamp=' + timestamp_str(prepare_ts + 5)+
            #                                 ',durable_timestamp=' + timestamp_str(prepare_ts + 5))

            s = self.conn.open_session()
            s.begin_transaction('ignore_prepare=true')
            evict_cursor = s.open_cursor(self.uri, None, 'debug=(release_evict)')
            for i in range(1, self.n + 1):
                evict_cursor.set_key(str(i))
                self.assertEquals(evict_cursor.search(), 0)
                self.assertEquals(evict_cursor.get_value(), str(i) + value)
                evict_cursor.reset()

            s.rollback_transaction()
            self.session.rollback_transaction()

        cursor.close()

    def check(self, uri, value, nrows):
        cursor = self.session.open_cursor(uri)
        for i in range(1, nrows + 1):
            cursor.set_key(str(i))
            self.assertEquals(cursor.search(), 0)
            cursor.search()
            expected_value = str(i) + value
            actual_value = cursor.get_value()
            self.assertEqual(actual_value, expected_value)

        cursor.close()    
        
    # Force a small cache
    conn_config = 'cache_size=10MB' 
    session_config='isolation=snapshot'
    uri='table:test_hs25'
    n = 1000

    def test_insert_updates_hs(self):
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(1))

        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Insert values at different timestamps.
        value = 'a' * 1000
        self.large_updates(self.uri, value, self.n, 2)
        self.large_updates(self.uri, value, self.n, 3)
        self.prepare_updates(self.uri, value, self.n, 5)
        
