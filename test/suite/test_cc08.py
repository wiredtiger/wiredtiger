#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# [TEST_TAGS]
# checkpoint:checkpoint_cleanup
# [END_TAGS]

import time

import wttest
from wiredtiger import stat

# test_cc08.py
# With a dedicated checkpoint cleanup thread, the cleanup statistics describe
# the most recent completed pass: they are replaced by each pass and are not
# cleared by a checkpoint.
class test_cc08(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),log=(enabled=false),' + \
        'checkpoint_cleanup=(use_thread=true,wait=1)'
    nrows = 10000
    uri = 'table:cc08'
    timeout = 60

    def get_stat(self, stat_id):
        cursor = self.session.open_cursor('statistics:')
        try:
            return cursor[stat_id][2]
        finally:
            cursor.close()

    def wait_for_pass(self, timeout=None):
        # Wait for a completed pass that processed at least one file.
        if timeout is None:
            timeout = self.timeout
        start = time.time()
        success = self.get_stat(stat.conn.cc_success)
        while (self.get_stat(stat.conn.cc_success) == success or
               self.get_stat(stat.conn.cc_handle_processed) == 0):
            if time.time() - start > timeout:
                self.fail('checkpoint cleanup did not complete a pass that ' +
                          'processed a file within {} seconds'.format(timeout))
            time.sleep(0.1)

    def wait_for_next_pass(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        start = time.time()
        success = self.get_stat(stat.conn.cc_success)
        while self.get_stat(stat.conn.cc_success) == success:
            if time.time() - start > timeout:
                self.fail('checkpoint cleanup did not complete a pass ' +
                          'within {} seconds'.format(timeout))
            time.sleep(0.1)

    def setup_file(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(self.nrows):
            cursor[i] = 'value'
        self.session.commit_transaction('commit_timestamp=10')
        cursor.close()
        self.conn.set_timestamp('oldest_timestamp=10,stable_timestamp=10')

        # The checkpoint makes the file eligible for cleanup.
        self.session.checkpoint()

    def test_passes_do_not_accumulate(self):
        self.setup_file()
        self.wait_for_pass()
        handles = self.get_stat(stat.conn.cc_handle_processed)

        # A second pass replaces the statistics; it does not add to them.
        self.wait_for_next_pass()
        self.assertEqual(self.get_stat(stat.conn.cc_handle_processed), handles,
            'checkpoint cleanup handles accumulate across cleanup passes')

    def test_checkpoint_does_not_clear(self):
        self.setup_file()
        self.wait_for_pass()
        handles = self.get_stat(stat.conn.cc_handle_processed)
        self.assertGreater(handles, 0)

        # A checkpoint does not clear the most recent pass statistics.
        self.session.checkpoint()
        self.assertEqual(self.get_stat(stat.conn.cc_handle_processed), handles,
            'checkpoint cleared the most recent checkpoint cleanup statistics')

if __name__ == '__main__':
    wttest.run()
