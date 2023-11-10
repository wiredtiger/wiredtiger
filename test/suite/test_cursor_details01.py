
import time, wttest

class test_cursor_details(wttest.WiredTigerTestCase):
    """
    Check that the cursor get_details API works with a checkpoint cursor.
    """

    def test_cursor_details(self):

        # Create a checkpoint.
        uri = 'table:cursor_details_checkpoint'
        internal_checkpoint_name = 'WiredTigerCheckpoint'
        self.session.create(uri, "key_format=S,value_format=S")
        self.session.checkpoint()

        # Query checkpoint cursor details. This is the first and currently
        # only cursor to support this API all other should return ENOTSUP.
        cursor = self.session.open_cursor(uri, None, f"checkpoint={internal_checkpoint_name}")
        ret, details = cursor.get_details(None)
        
        # Checkpoint cursor support this interface and there are no runtime errors.
        self.assertEqual(ret, 0)
        
        # The checkpoint is the wall time the snapshot was taken.
        self.assertGreaterEqual(details.checkpoint.checkpoint_id, time.time())

        # There is no data in the database hence the stable timestamp will not have advanced.
        self.assertEqual(details.checkpoint.stable_timestamp, 0)

        cursor.close()

if __name__ == '__main__':
    wttest.run()
