
import wiredtiger, wttest

class test_cursor_details(wttest.WiredTigerTestCase):

    def test_cursor_details(self):
        uri = 'table:cursor_details_checkpoint'
        internal_checkpoint_name = 'WiredTigerCheckpoint'
        self.session.create(uri, "key_format=S,value_format=S")
        # Need to insert data?
        self.session.checkpoint()
        cursor = self.session.open_cursor(uri, None, f"checkpoint={internal_checkpoint_name}")
        # Is this the wrong cursor type?
        details = wiredtiger.CursorDetails()
        self.assertEqual(details.checkpoint.stable_timestamp, 0)
        cursor.get_details(details, None)
        self.assertNotEqual(details.checkpoint.stable_timestamp, 0)
        cursor.close()

if __name__ == '__main__':
    wttest.run()
