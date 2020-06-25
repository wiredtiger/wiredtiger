import wiredtiger, wttest
from wtdataset import simple_key, simple_value

def timestamp_str(t):
    return '%x' % t

class test_prepare_conflict(wttest.WiredTigerTestCase):
    def test_prepare(self):
        # Create a large table with lots of pages.
        uri = "table:test_prepare_conflict"
        config = 'allocation_size=512,leaf_page_max=512,key_format=S,value_format=S'
        self.session.create(uri, config)
        cursor = self.session.open_cursor(uri)
        for i in range(1, 80000):
            cursor[simple_key(cursor, i)] = simple_value(cursor, i)
        cursor.close()

        # Force to disk.
        self.reopen_conn()

        # Start a transaction.
        self.session.begin_transaction('isolation=snapshot')

        # Truncate the middle chunk.
        c1 = self.session.open_cursor(uri, None)
        c1.set_key(simple_key(c1, 10000))
        c2 = self.session.open_cursor(uri, None)
        c2.set_key(simple_key(c1, 70000))
        self.session.truncate(None, c1, c2, None)
        c1.close()
        c2.close()

        # Modify a record on a fast-truncate page.
        cursor = self.session.open_cursor(uri)
        cursor[simple_key(cursor, 40000)] = "replacement_value"
        cursor.close()

        # Prepare and commit the transaction.
        self.session.prepare_transaction('prepare_timestamp=' + timestamp_str(10))
        self.session.timestamp_transaction('commit_timestamp=' + timestamp_str(20))
        self.session.timestamp_transaction('durable_timestamp=' + timestamp_str(20))
        self.session.commit_transaction()

        # WT-6325 reports WT_PREPARE_CONFLICT while iterating the cursor.
        # Walk the table, the bug will cause a prepared conflict return.
        cursor = self.session.open_cursor(uri)
        while cursor.next() == 0:
            continue
        cursor.close()

if __name__ == '__main__':
    wttest.run()
