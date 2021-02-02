
from helper import copy_wiredtiger_home
import wiredtiger, wttest, unittest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

class tmp_test(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=10MB'
    session_config = 'isolation=snapshot'
    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer', dict(key_format='i')),
    ]
    value_format_values = [
        ('fixed', dict(value_format='8t')),
        ('variable', dict(value_format='i')),
    ]
    scenarios = make_scenarios(key_format_values, value_format_values)

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        if read_ts == 0:
            session.begin_transaction()
        else:
            session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value)
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)

    def test_rollback_to_stable(self):
        # Create a small table.
        uri = "table:tmp"
        create_params = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(uri, create_params)
        cursor =  self.session.open_cursor(uri)

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        tmp_value = 0x20
        tmp_value2 = 0x30
        tmp_value3 = 0x40
        tmp_value4 = 0x50
        tmp_value5 = 0x60

        # Do a bunch of transactions (Updates in memory)
        #Insert
        self.session.begin_transaction()
        for i in range(1, 20000):
            cursor[i] = tmp_value
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        #First Update
        self.session.begin_transaction()
        for i in range(1, 20000):
            cursor[i] = tmp_value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        #Set stable Timestamp
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(2))
        #Rollback to the stable timestamp
        self.conn.rollback_to_stable()
        # Check that only tmp_value is available
        self.check(tmp_value, uri, 19999, 2)

        #Second Update
        self.session.begin_transaction()
        for i in range(1, 20000):
            cursor[i] = tmp_value3
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(7))

        #Third Update
        self.session.begin_transaction()
        for i in range(1, 20000):
            cursor[i] = tmp_value4
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(9))

        #Set stable Timestamp
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(7))
        #Rollback to the stable timestamp
        self.conn.rollback_to_stable()
        #Check that only tmp_value is available
        self.check(tmp_value3, uri, 19999, 7)
        
        #Fourth Update
        self.session.begin_transaction()
        for i in range(1, 20000):
            cursor[i] = tmp_value5
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        self.session.close()

if __name__ == '__main__':
    wttest.run()
