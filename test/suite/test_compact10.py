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

import os, random, shutil, time, wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# test_compact10.py
# Verify a backup of the database can be taken while background compaction is running.
# It is worth noting that this test has a random behavior and one run from the other may not execute
# the same code paths.
class test_compact10(wttest.WiredTigerTestCase):
    backup_dir = "BACKUP"
    conn_config = 'cache_size=100MB,statistics=(all)'
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=32KB'
    uri_prefix = 'table:test_compact10'

    n_tables = 3
    table_numkv = 100 * 1000
    value_size = 1024 # The value should be small enough so that we don't create overflow pages.

    timing_stress_cfg_values = [
        ('none', dict(timing_stress_cfg='')),
        ('checkpoint_slow', dict(timing_stress_cfg='checkpoint_slow')),
        ('compact_slow', dict(timing_stress_cfg='compact_slow')),
    ]
    scenarios = make_scenarios(timing_stress_cfg_values)

    def delete_range(self, uri, num_keys):
        c = self.session.open_cursor(uri, None)
        for i in range(num_keys):
            c.set_key(i)
            c.remove()
        c.close()

    def get_bg_compaction_running(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        compact_running = stat_cursor[stat.conn.background_compact_running][2]
        stat_cursor.close()
        return compact_running

    def get_pages_rewritten(self, uri):
        stat_cursor = self.session.open_cursor('statistics:' + uri, None, None)
        pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
        stat_cursor.close()
        return pages_rewritten

    def populate(self, uri, num_keys, value_size):
        c = self.session.open_cursor(uri, None)
        for k in range(num_keys):
            c[k] = ('%07d' % k) + '_' + 'abcd' * ((value_size // 4) - 2)
        c.close()

    def take_full_backup(self, fromdir, todir):
        # Do a full backup by opening up a backup cursor and copying the files.
        cursor = self.session.open_cursor('backup:', None, None)
        os.mkdir(todir)
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            bkup_file = cursor.get_key()
            copy_file = os.path.join(fromdir, bkup_file)
            sz = os.path.getsize(copy_file)
            shutil.copy(copy_file, todir)
        assert ret == wiredtiger.WT_NOTFOUND
        cursor.close()

    def turn_on_bg_compact(self, config):
        self.session.compact(None, config)
        compact_running = self.get_bg_compaction_running()
        while not compact_running:
            time.sleep(1)
            compact_running = self.get_bg_compaction_running()
        self.assertEqual(compact_running, 1)

    def test_compact10(self):

        # FIXME-WT-11399
        if self.runningHook('tiered'):
            self.skipTest("this test does not yet work with tiered storage")

        # Create and populate tables.
        uris = []
        for i in range(self.n_tables):
            uri = self.uri_prefix + f'_{i}'
            uris.append(uri)
            self.session.create(uri, self.create_params)
            self.populate(uri, self.table_numkv, self.value_size)

        # Write to disk.
        self.session.checkpoint()

        # Delete 50% of the file.
        for uri in uris:
            self.delete_range(uri, 50 * self.table_numkv // 100)

        # Write to disk.
        self.session.checkpoint()

        # Reconfigure the connection with:
        # - Aggressive compaction
        reconf_config = self.conn_config + ',debug_mode=(background_compact)'
        # - Stressing options to slow down compaction and checkpoints so compaction takes more time.
        reconf_config += f',timing_stress_for_test=({self.timing_stress_cfg})'
        # - Verbose for debugging purposes.
        # reconf_config += ',verbose=(compact,compact_progress)'
        self.conn.reconfigure(reconf_config)
        
        # Enable background compaction.
        compact_config = f'background=true,free_space_target=1MB'
        self.turn_on_bg_compact(compact_config)

        # Wait for a random table to be compacted before crashing.
        uri_idx = random.randint(0, self.n_tables - 1)
        uri = uris[uri_idx]
        # Given 50% of each collection has been deleted, compaction must have work to do.
        while not self.get_pages_rewritten(uri):
            time.sleep(0.5)

        # Sleep some random time before proceeding.
        time.sleep(random.uniform(0, 0.5))

        # Take a full backup.
        self.take_full_backup('.', self.backup_dir)
        self.reopen_conn(self.backup_dir, self.conn_config)

        # Verify each table from the backup.
        for uri in uris:
            self.verifyUntilSuccess(self.session, uri, "strict")

        # Background compaction may be have been inspecting a table when disabled which is
        # considered as an interruption, ignore that message.
        self.ignoreStdoutPatternIfExists('background compact interrupted by application')

if __name__ == '__main__':
    wttest.run()
