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
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


# test_recovery02.py
# Test that txn recovery is bypassed in disagg

@disagg_test_class
class test_recovery02(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),verbose=(recovery),' \
                     + 'disaggregated=(lose_all_my_data=true),'

    uri = "layered:test_recovery02"

    role_scenarios = [
        ('leader', dict(role='leader')),
        ('follower', dict(role='follower')),
    ]
    disagg_storages = gen_disagg_storages('test_recovery02', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, role_scenarios)

    def conn_config(self):
        return self.conn_base_config + f'disaggregated=(role="{self.role}")'

    def test_recovery_bypassed_in_disagg(self):
        # The connection is already open at this point with verbose=(recovery)
        # Check that we see the "skipping recovery" message in stdout
        self.captureout.checkAdditionalPattern(
            self, 'skipping recovery in disaggregated mode')

        # Check that we still see this message when reopening the connection.
        # Ignore "[WT_VERB_METADATA][WARNING]: Removing local file due to disagg mode"
        self.reopen_conn()
        self.captureout.checkAdditionalPattern(
            self, 'skipping recovery in disaggregated mode',
            ignore_pat='WT_VERB_METADATA')
