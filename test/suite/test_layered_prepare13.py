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

import test_prepare53
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


# Exercise the shared bounds checks with committed data on a follower's stable table.
@disagg_test_class
class test_layered_prepare13(test_prepare53.test_prepare53):
    uri = 'layered:prepare_bounds'
    conn_config = test_prepare53.test_prepare53.conn_config + \
        ',precise_checkpoint=true,disaggregated=(role="leader")'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True),
        test_prepare53.test_prepare53.scenarios)

    def reader_connection(self):
        connection = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,statistics=(all),disaggregated=(role="follower")')
        self.disagg_advance_checkpoint_and_wait(connection)
        return connection
