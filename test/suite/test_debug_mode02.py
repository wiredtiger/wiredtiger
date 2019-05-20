#!/usr/bin/env python
#
# Public Domain 2024-2029 MongoDB, Inc.
# Public Domain 2008-2024 WiredTiger, Inc.
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

import fnmatch, os, time, wiredtiger, wttest

# test_debug_mode02.py
#    Test the debug mode settings. Test checkpoint_retention use.
class test_debug_mode02(wttest.WiredTigerTestCase):
    uri = 'file:test_debug'

    entries = 100
    loop = 0
    retain = 5

    def conn_config(self):
        return 'log=(enabled=true,file_max=100K),debug_mode=(checkpoint_retention=%d)' % self.retain

    def advance_log_checkpoint(self):

        # Advance the log file to the next file and write a checkpoint.
        keys = range(1, self.entries)
        cur_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        c = self.session.open_cursor(self.uri, None)
        new_logs = cur_logs
        # Write data in small chunks until we switch log files.
        while cur_logs == new_logs:
            for k in keys:
                c[k + (self.loop * self.entries)] = 1
            self.loop += 1
            new_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        c.close()
        # Write a checkpoint into the new log file.
        self.session.checkpoint()

    def test_checkpoint_retain(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        # No log files should be archived while we have fewer than the
        # retention number of logs. Make sure each iteration the new
        # logs are a proper superset of the previous time.
        for i in range(1, self.retain):
            cur_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
            self.advance_log_checkpoint()
            time.sleep(1.0)
            new_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
            cur_set = set(cur_logs)
            new_set = set(new_logs)
            self.assertTrue(new_set.issuperset(cur_set))

        cur_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        self.advance_log_checkpoint()
        time.sleep(1.0)
        new_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        cur_set = set(cur_logs)
        new_set = set(new_logs)
        # This time archive should have happened.
        self.assertFalse(new_set.issuperset(cur_set))

    # Test that both zero and one archive as usual. And test reconfigure.
    def test_checkpoint_retain_off(self):
        self.conn.reconfigure("debug_mode=(checkpoint_retention=0)")
        self.session.create(self.uri, 'key_format=i,value_format=i')
        cur_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")

        self.advance_log_checkpoint()
        time.sleep(1.0)
        new_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        cur_set = set(cur_logs)
        new_set = set(new_logs)
        # Archive should have happened.
        self.assertFalse(new_set.issuperset(cur_set))

        self.conn.reconfigure("debug_mode=(checkpoint_retention=1)")
        cur_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        self.advance_log_checkpoint()
        time.sleep(1.0)
        new_logs = fnmatch.filter(os.listdir(self.home), "*gerLog*")
        cur_set = set(cur_logs)
        new_set = set(new_logs)
        # Archive should have happened.
        self.assertFalse(new_set.issuperset(cur_set))

if __name__ == '__main__':
    wttest.run()
