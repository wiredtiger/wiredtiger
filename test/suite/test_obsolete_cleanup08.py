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

import wttest
from test_obsolete_cleanup01 import test_obsolete_cleanup_base
from wiredtiger import stat
from wtscenario import make_scenarios

# test_obsolete_cleanup08.py
# Verify obsolete cleanup cleans up logged tables when configured in aggressive mode.
@wttest.skip_for_hook("tiered", "Obsolete cleanup does not support tiered tables")
class test_obsolete_cleanup08(test_obsolete_cleanup_base):
    conn_config = 'statistics=(all),statistics_log=(json,wait=1,on_close=true),log=(enabled=true)'

    obsolete_cleanup_methods = [
        ('obsolete_cleanup_method_none', dict(obsolete_cleanup_aggressive=False, obsolete_cleanup_config='checkpoint_cleanup=[method=none]')),
        ('obsolete_cleanup_method_reclaim_space', dict(obsolete_cleanup_aggressive=True, obsolete_cleanup_config='checkpoint_cleanup=[method=reclaim_space]')),
    ]

    scenarios = make_scenarios(obsolete_cleanup_methods)

    def test_obsolete_cleanup08(self):
        # Increase the likelihood of having internal pages since they are targeted by checkpoint
        # cleanup.
        create_params = 'key_format=i,value_format=S,allocation_size=512,internal_page_max=512,leaf_page_max=512'
        nrows = 1000
        uri = 'table:cc08'
        value = 'k' * 128

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, value)

        # Write everything to disk.
        self.session.checkpoint()

        # Restart and specify the obsolete cleanup's strategy.
        self.reopen_conn(config=f'{self.conn_config},{self.obsolete_cleanup_config}')

        # Open the table as we need the dhandle to be open for obsolete cleanup to process the
        # table.
        _ = self.session.open_cursor(uri, None, None)

        # Force obsolete cleanup and wait for it to make progress.
        self.check_obsolete_cleanup_stats()

        # Check stats.
        selected_pages = self.get_stat(stat.conn.checkpoint_cleanup_pages_read_reclaim_space)
        if self.obsolete_cleanup_aggressive:
            self.assertGreater(selected_pages, 0)
        else:
            self.assertEqual(selected_pages, 0)
