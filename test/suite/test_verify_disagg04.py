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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_verify_disagg04.py
#    Verify that read_corrupt lets WT_SESSION::verify continue past disaggregated read
#    errors instead of aborting the connection. A failed disaggregated read used to panic
#    on the API verify path (which sets neither read-corrupt session flag); it must now
#    return the error so verify can record it and keep traversing.
@disagg_test_class
class test_verify_disagg04(wttest.WiredTigerTestCase):
    test_name = __qualname__
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    nitems = 10000

    conn_config = 'statistics=(all),disaggregated=(role="leader")'
    conn_config_follower = 'statistics=(all),disaggregated=(role="follower")'

    # Small pages so the stable tree has internal pages with several leaf children to traverse.
    table_cfg = 'key_format=S,value_format=S,block_manager=disagg,' \
                'leaf_page_max=4KB,internal_page_max=4KB'
    uri = f'layered:{test_name}'

    # The failpoint forces every ordinary page read to be treated as corrupt.
    read_failpoint = 'timing_stress_for_test=[failpoint_page_log_handle_read]'

    def test_verify_disagg_read_corrupt(self):
        # Populate a layered table on the leader and checkpoint so the stable table is on the
        # page service.
        self.session.create(self.uri, self.table_cfg)
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            cursor[str(i)] = str(i)
        cursor.close()
        self.session.checkpoint()

        # Bring up a follower and advance it to the checkpoint; it reads the stable pages fresh
        # from the page service.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
                                           self.conn_config_follower)
        session_follow = conn_follow.open_session('')
        self.disagg_advance_checkpoint(conn_follow)

        # Without fault injection the follower verifies cleanly.
        self.verifyUntilSuccess(session_follow)

        # Inject read failures: every ordinary (non-root-probe) page read returns a corruption
        # error. With read_corrupt, verify must record the error and continue rather than panic.
        conn_follow.reconfigure(self.read_failpoint)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: session_follow.verify(self.uri, 'read_corrupt'), '/WT_ERROR/')
        self.ignoreStderrPatternIfExists('corrupt dump')
        self.ignoreStderrPatternIfExists('fatal read error')
        self.ignoreStderrPatternIfExists('read checksum error')

        # The failpoint actually fired during the traversal.
        self.assertStatGreaterSoon(
            wiredtiger.stat.conn.disagg_block_plh_read_failed, 0, session=session_follow)

        # The connection survived the read errors: with the failpoint cleared, verify succeeds.
        conn_follow.reconfigure('timing_stress_for_test=[]')
        self.verifyUntilSuccess(session_follow)

        session_follow.close()
        conn_follow.close()
