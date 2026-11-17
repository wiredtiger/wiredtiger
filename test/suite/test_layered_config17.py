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

import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# The extended abandon-checkpoint arguments and the checkpoint LSN option are for internal use.
# These are the parts reachable without a page log that implements the extended method.

class test_layered_config17(wttest.WiredTigerTestCase, DisaggConfigMixin):

    disagg_storages = gen_disagg_storages(disagg_only = True)

    scenarios = make_scenarios(disagg_storages)

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_abandon_checkpoint_lsn(self):
        # The arguments structure is empty until it is given an LSN, and keeps the value.
        args = wiredtiger.PageLogAbandonCheckpointArgs()
        self.assertEqual(args.last_checkpoint_lsn, 0)
        args.last_checkpoint_lsn = 1234
        self.assertEqual(args.last_checkpoint_lsn, 1234)

        # The LSN of the installed checkpoint is accepted as a configuration option.
        self.conn.reconfigure('disaggregated=(checkpoint_lsn=1234)')

        # It is validated as an integer.
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.conn.reconfigure('disaggregated=(checkpoint_lsn=notanumber)'))
        self.ignoreStderrPatternIfExists('Invalid value for key')

if __name__ == '__main__':
    wttest.run()
