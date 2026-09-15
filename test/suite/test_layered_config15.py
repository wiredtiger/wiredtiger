#!/usr/bin/env python3
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

import os, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# Test that blocks written with a larger block header stay readable by a node that knows nothing
# about the added fields, and that a node refuses blocks it is told it cannot interpret.
@disagg_test_class
class test_layered_config15(wttest.WiredTigerTestCase, suite_subprocess):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="follower"),'

    create_session_config = 'key_format=S,value_format=S,type=layered'

    num_items = 2000
    num_modify = 100
    uri = f"table:{test_name}"

    disagg_storages = gen_disagg_storages(disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def value(self, i, modified=False):
        prefix = 'value_mod' if modified else 'value'
        return f'{prefix}{i:04}' + 'abcd' * 100

    def populate(self, count, timestamp, modified=False, start=0):
        self.session.begin_transaction()
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(start, count):
            cursor[f'key{i:04}'] = self.value(i, modified)
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(timestamp))
        self.session.checkpoint()

    def check_all(self, num_modified):
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.num_items):
            self.assertEqual(cursor[f'key{i:04}'], self.value(i, i < num_modified))
        cursor.close()

    def debug_config(self, mode):
        return self.conn_config + f',debug_mode=(disagg_block_header_upgrade={mode})'

    def test_larger_block_header_is_readable(self):
        """
        A node writing a larger block header records the size in the header itself. A node that
        knows only the smaller header must skip the fields it does not recognise and find the data
        where the writer left it.
        """
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.session.create(self.uri, self.create_session_config)
        self.populate(self.num_items, 1)

        # Restart as a node that writes the larger header, and rewrite part of the table so the
        # larger header reaches storage.
        self.restart_without_local_files(config=self.debug_config('compatible'))
        self.check_all(0)
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.populate(self.num_modify, 2, modified=True)

        # Restart as a node that knows nothing about the added fields. It must read every row,
        # including those in the blocks carrying the larger header.
        self.restart_without_local_files(config=self.debug_config('none'))
        self.check_all(self.num_modify)

        # It must also be able to keep writing on top of them.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.populate(self.num_modify * 2, 3, modified=True, start=self.num_modify)
        self.check_all(self.num_modify * 2)

        # And a node that writes the larger header again reads what the older node wrote.
        self.restart_without_local_files(config=self.debug_config('compatible'))
        self.check_all(self.num_modify * 2)

    def subprocess_incompatible_block_header_refused(self):
        """Subprocess body: reading a block that demands a newer reader fails."""
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.session.create(self.uri, self.create_session_config)
        self.populate(self.num_items, 1)

        # Write blocks that declare they need a reader newer than any build here.
        self.restart_without_local_files(config=self.debug_config('incompatible'))
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.populate(self.num_modify, 2, modified=True)

        # Read them back without the debug mode: the blocks demand a newer reader.
        self.restart_without_local_files(config=self.debug_config('none'))
        self.check_all(self.num_modify)

    def test_incompatible_block_header_refused(self):
        """
        A block whose compatible version exceeds the reader's version cannot be interpreted, and
        the read path refuses it down the same route it refuses corruption, which takes the
        connection down. The refusal is fatal during checkpoint pickup, so it runs in a subprocess.

        This asserts the consequence rather than the reported reason: the refusal surfaces on a
        session that tolerates corruption, which reports the reason at verbose level instead of as
        an error. The version comparison itself is covered by the
        "disagg block header version compatibility" unit test, and
        test_larger_block_header_is_readable runs the same sequence with a compatible version and
        expects it to succeed, so a failure here is specific to the version being rejected.
        """
        # Set timestamps so the fixture can close the parent connection cleanly.
        self.conn.set_timestamp(
            'stable_timestamp=' + self.timestamp_str(1) +
            ',oldest_timestamp=' + self.timestamp_str(1))

        name = 'incompatible_block_header_refused'
        [returncode, home] = self.run_subprocess_function(f'SUBPROCESS_{name}',
            f'{self.test_name}.{self.test_name}.subprocess_{name}', silent=True)
        self.assertNotEqual(returncode, 0)
        self.check_file_contains(os.path.join(home, 'stderr.txt'), 'unable to read root page')
