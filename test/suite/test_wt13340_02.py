#!/usr/bin/env python

from test_cc01 import test_cc_base
from suite_subprocess import suite_subprocess
import time
from wiredtiger import stat

# test_wt13340.py
#
class test_wt13340(test_cc_base, suite_subprocess):
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=32KB,'
    conn_config = 'cache_size=100MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),verbose=[checkpoint:2,compact:2]'
    uri = 'table:test_wt13340'

    table_numkv = 100000

    def delete_range(self, uri, start_key, num_keys, ts_start):
        c = self.session.open_cursor(uri, None)
        ts = ts_start
        for k in range(start_key, num_keys):
            c.set_key(k)
            self.session.begin_transaction()
            c.remove()
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(ts))
            ts += 1
        c.close()

    def populate(self, uri, start_key, num_keys, value=None, value_size=1024):
        c = self.session.open_cursor(uri, None)
        for k in range(start_key, num_keys):
            if not value:
                value = ('%07d' % k) + '_' + 'a' * (value_size - 2)
            self.session.begin_transaction()
            c[k] = value
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(k + 1))
        c.close()

    def get_size(self):
        c = self.session.open_cursor('statistics:' + self.uri, None, None)
        file_size = c[stat.dsrc.block_size][2]
        c.close()
        return(file_size)

    def test_compact16(self):
        # Pin oldest timestamp 1.
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Create and populate a table.
        self.session.create(self.uri, self.create_params)
        self.session.checkpoint()
        self.prout(f'File size: {self.get_size()}')
        self.prout('Populating...')
        self.populate(self.uri, 0, self.table_numkv)
        self.prout('Populating... Done!')

        # Make everything stable.
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(self.table_numkv)}')

        # Write to disk.
        self.session.checkpoint()

        # Delete everything.
        self.prout('Deleting...')
        self.delete_range(self.uri, 0, self.table_numkv, self.table_numkv + 1)
        self.prout('Deleting... Done!')

        # Make the deletions stable.
        ts = 2 * self.table_numkv
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')

        # Write to disk.
        self.session.checkpoint()

        # Make everything globally visible.
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(ts)}')
        self.prout(ts)
        self.prout(f'File size: {self.get_size()}')

        cc_success = 0
        while (cc_success < 10):
            self.session.checkpoint('debug=(checkpoint_cleanup=true)')
            c = self.session.open_cursor( 'statistics:')
            cc_success = c[stat.conn.checkpoint_cleanup_success][2]
            c.close()
            self.prout(f'cc_success={cc_success}')
            self.prout(f'File size: {self.get_size()}')
            time.sleep(1)

        self.session.checkpoint()
        self.session.checkpoint()

        self.prout(f'File size: {self.get_size()}')
        
        self.prout(f'compacting')
        self.session.compact(self.uri)
        
        self.prout(f'File size: {self.get_size()}')
        # It seems as though the minimum file size is 12KB with 4KB available for some reason.
        self.assertLessEqual(self.get_size(), 12 * 1024)

        # Ignore compact verbose messages used for debugging.
        # self.ignoreStdoutPatternIfExists('WT_VERB_COMPACT')
        # self.ignoreStderrPatternIfExists('WT_VERB_COMPACT')
        self.ignoreStdoutPatternIfExists('WT_VERB')
