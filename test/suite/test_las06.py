import wiredtiger, wttest
from wiredtiger import stat

def timestamp_str(t):
    return '%x' % t

# test_las06.py
# Verify that triggering lookaside usage does not cause a spike in memory usage
# to form an update chain from the lookaside contents.
#
# The required value should be fetched from lookaside and then passed straight
# back to the user without putting together an update chain.
#
# TODO: Tweak the checks after the main portion of the relevant history
# project work is complete.
class test_las06(wttest.WiredTigerTestCase):
    # Force a small cache.
    conn_config = 'cache_size=50MB,statistics=(fast)'
    session_config = 'isolation=snapshot'

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def get_non_page_image_memory_usage(self):
        return self.get_stat(stat.conn.cache_bytes_other)

    def get_lookaside_usage(self):
        return self.get_stat(stat.conn.cache_read_lookaside)

    def test_checkpoint_las_reads(self):
        # Create a small table.
        uri = "table:test_las06"
        create_params = 'key_format=i,value_format=S,'
        self.session.create(uri, create_params)

        value1 = b'a' * 500
        value2 = b'b' * 500

        # Fill up the cache with 50Mb of data.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 10000):
            cursor[i] = value1
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))

        # Load another 50Mb of data with a later timestamp.
        self.session.begin_transaction()
        for i in range(1, 10000):
            cursor[i] = value2
        self.session.commit_transaction('commit_timestamp=' + timestamp_str(3))

        # Now the latest version will get written to the data file.
        self.session.checkpoint()

        start_usage = self.get_non_page_image_memory_usage()

        # We haven't used lookaside yet.
        self.assertEqual(self.get_lookaside_usage(), 0)

        # Whenever we request something out of cache of timestamp 2, we should
        # be reading it straight from lookaside without initialising a full
        # update chain of every version of the data.
        self.session.begin_transaction('read_timestamp=' + timestamp_str(2))
        for i in range(1, 10000):
            self.assertEqual(cursor[i], value1)
        self.session.rollback_transaction()

        end_usage = self.get_non_page_image_memory_usage()

        # We expect to have used lookaside when reading historical data.
        #
        # Prior to this change, this type of workload will cause every lookaside
        # entry for a given page to get read in. Now that we're only reading the
        # lookaside entry that we need, we expect to see comparatively fewer
        # reads from lookaside.
        #
        # TODO: Set the upper bound to something smaller once the project work
        # is done. At the time of writing, I get ~1500 lookaside reads for this
        # workload.
        las_usage = self.get_lookaside_usage()
        self.assertNotEqual(las_usage, 0)
        self.assertLess(las_usage, 2000)

        # Non-page related memory usage shouldn't spike significantly.
        #
        # Prior to this change, this type of workload would use a lot of memory
        # to recreate update lists for each page.
        #
        # This check could be more aggressive but to avoid potential flakiness,
        # lets just ensure that it hasn't doubled.
        #
        # TODO: Uncomment this once the project work is done.
        # self.assertLessEqual(end_usage, (start_usage * 2))

if __name__ == '__main__':
    wttest.run()
