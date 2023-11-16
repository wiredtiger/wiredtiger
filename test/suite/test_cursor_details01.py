
import time, wiredtiger, wttest
from wtdataset import SimpleDataSet

class test_cursor_details(wttest.WiredTigerTestCase):
    """
    Validate basic cursor::get_details support.
    """

    uri = 'table:cursor_details'
    checkpoint_name = 'WiredTigerCheckpoint'
    key_format = 'S'
    value_format = 'S'

    def test_cursor_details(self):

        # Create a checkpoint.
        self.session.create(self.uri,
                            f'key_format={self.key_format},value_format={self.value_format}')

        # Statistics cursors don't support get_details() make invoking results in
        # ENOTSUP.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                                     lambda: self.session.open_cursor('statistics:cursor_details'),
                                     '/unsupported object operation/')

        # There is no data in the database hence the stable timestamp will not have advanced,
        # verify it is zero.
        self._create_checkpoint_and_check_details(0)

        # Set oldest and stable timestamps to 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))

        # The stable timestamp has been forcibly advanced, and this should be
        # reflected in the checkpoint.
        self._create_checkpoint_and_check_details(10)

        # Insert at timestamp 20.
        ds = SimpleDataSet(
            self, self.uri, 0, key_format=self.key_format, value_format=self.value_format)
        ds.populate()
        self._insert_updates(ds, 10, 'aaaaa' * 10, 20)

        # Stable timestamp should be unchanged.
        self._create_checkpoint_and_check_details(10)

        # Advanced the stable timestamp again and check.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self._create_checkpoint_and_check_details(20)

    def _create_checkpoint_and_check_details(self, expected_stable_ts):
        '''
        Create a checkpoint with the current table and verify that the
        stable timestamp reported by get_details() is as expected.
        '''

        wall_time_pre_checkpoint = time.time()
        self.session.checkpoint()
        wall_time_post_checkpoint = time.time()

        cursor = self.session.open_cursor(self.uri, None, f"checkpoint={self.checkpoint_name}")
        details = cursor.get_details(None)

        # The checkpoint id happens to correspond to the wall time at some point in checkpoint
        # creation. Use this "insider" knowledge to sythesize a test for an otherwise opaque value.
        self.assertLessEqual(wall_time_pre_checkpoint, details.checkpoint.checkpoint_id)
        self.assertGreaterEqual(details.checkpoint.checkpoint_id, wall_time_post_checkpoint)

        self.assertEqual(details.checkpoint.stable_timestamp, expected_stable_ts)

        cursor.close()

    def _insert_updates(self, ds, nrows, value, ts):
        '''
        Insert data from specified datastore in table.
        '''
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, nrows + 1):
            cursor[ds.key(i)] = value
            if i % 101 == 0:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
                self.session.begin_transaction()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

if __name__ == '__main__':
    wttest.run()
