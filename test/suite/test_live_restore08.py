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

import os, glob, time, wiredtiger, wttest
from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from helper import copy_wiredtiger_home
from wtbackup import backup_base


# test_live_restore08.py
# Test bulk cursor usage with live restore.
@wttest.skip_for_hook("tiered", "using multiple WT homes")
class test_live_restore08(backup_base):
    format_values = [
        ('row_integer', dict(key_format='i', value_format='S')),
    ]

    read_sizes = [
        ('512B', dict(read_size='512B')),
    ]

    scenarios = make_scenarios(format_values, read_sizes)
    nrows = 100000

    def get_stat(self, statistic):
        stat_cursor = self.session.open_cursor("statistics:")
        val = stat_cursor[statistic][2]
        stat_cursor.close()
        return val

    def wait_for_live_restore_complete(self):
        state = 0
        timeout = 120
        iteration_count = 0
        # Build in a 2 minute timeout. Once we see the complete state exit the loop.
        while (iteration_count < timeout):
            state = self.get_stat(stat.conn.live_restore_state)
            # Stress the file create path in the meantime, this checks some assert conditions.
            # self.session.create(f'file:abc{iteration_count}', f'key_format={self.key_format},value_format={self.value_format}')
            self.pr(f'Looping until finish, live restore state is: {state}, \
                      Current iteration: is {iteration_count}')
            # State 2 means the live restore has completed.
            if (state == wiredtiger.WT_LIVE_RESTORE_COMPLETE):
                break
            time.sleep(1)
            iteration_count += 1
        self.assertEqual(state, wiredtiger.WT_LIVE_RESTORE_COMPLETE)

    def simulate_crash_restart(self):
        olddir = "DEST"
        newdir = "RESTART"

        os.mkdir(newdir)
        copy_wiredtiger_home(self, olddir, newdir)
        self.close_conn()
        self.open_conn("RESTART", config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=1,read_size=" + self.read_size + ")")

    def populate_backup(self):
        ds1 = SimpleDataSet(self, 'file:standard', self.nrows,
        key_format=self.key_format, value_format=self.value_format)
        ds1.populate()

        self.session.create('file:bulk', f'key_format={self.key_format},value_format={self.value_format}')

        self.session.checkpoint()

        # Close the default connection.
        os.mkdir("SOURCE")
        self.take_full_backup("SOURCE")
        self.close_conn()

        # Remove everything but SOURCE / stderr / stdout.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

    # Test bulk cursors on a partially restored database using live restore.
    def test_live_restore_crash_restart_with_bulk(self):
        # Live restore is not supported on Windows.
        if os.name == 'nt':
            return

        self.populate_backup()

        os.mkdir("DEST")

        # Open live restore connection
        self.open_conn("DEST", config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=1,read_size=" + self.read_size + ")")

        # Simulate a crash by copying the partially restored database to a new directory "RESTART".
        self.simulate_crash_restart()

        # Ensure bulk cursors can still be used on the restored file.
        cursor = self.session.open_cursor('file:bulk', None, "bulk")
        self.assertEqual(self.get_stat(stat.conn.cursor_bulk_count), 1)

        for i in range(1, 10):
            cursor[i] = "aaaa"

        cursor.close()

    # Test bulk cursors on a fully restored database using live restore.
    def test_live_restore_complete_with_bulk(self):
        # Live restore is not supported on Windows.
        if os.name == 'nt':
            return

        self.populate_backup()

        os.mkdir("DEST")

        # Open live restore connection
        self.open_conn("DEST", config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=1,read_size=" + self.read_size + ")")

        self.wait_for_live_restore_complete()

        # Ensure bulk cursors can still be used on the restored file.
        cursor = self.session.open_cursor('file:bulk', None, "bulk")
        self.assertEqual(self.get_stat(stat.conn.cursor_bulk_count), 1)

        for i in range(1, 10):
            cursor[i] = "aaaa"

        cursor.close()

