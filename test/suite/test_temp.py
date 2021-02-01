
from helper import copy_wiredtiger_home
import wiredtiger, wttest, unittest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from test_rollback_to_stable01 import test_rollback_to_stable_base

def timestamp_str(t):
    return '%x' % t

class tmp_test(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=500MB,eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'
    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer', dict(key_format='i')),
    ]
    scenarios = make_scenarios(key_format_values)

    def test_rollback_to_stable(self):
        # Create a small table.
        uri = "table:tmp"
        create_params = 'key_format={},value_format=S'.format(self.key_format)
        self.session.create(uri, create_params)
        cursor =  self.session.open_cursor(uri)

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        tmp_value = 'aaaaa' * 100
        tmp_value2 = 'bbbbb' * 100
        tmp_value3 = 'ccccc' * 100
        tmp_value4 = 'ddddd' * 100
        tmp_value5 = 'eeeee' * 100

        self.session.begin_transaction()
        cursor[1] = tmp_value
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        self.session.begin_transaction()
        cursor[1] = tmp_value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(5))

        self.session.begin_transaction()
        cursor[1] = tmp_value3
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(7))

        self.session.begin_transaction()
        cursor[1] = tmp_value4
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(9))

        self.session.begin_transaction()
        cursor[1] = tmp_value5
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(10))

        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(7))
        self.conn.rollback_to_stable()
        self.check(tmp_value3, uri, 1, 9)

        self.session.close()
        

if __name__ == '__main__':
    wttest.run()
        

