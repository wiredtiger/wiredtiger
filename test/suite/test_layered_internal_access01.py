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

from contextlib import closing
import errno
import wiredtiger, wttest
from helper_disagg import disagg_test_class

@disagg_test_class
class test_layered_internal_access01(wttest.WiredTigerTestCase):
    conn_config = 'disaggregated=(role=leader,lose_all_my_data=true)'
    uri = 'layered:test_layered_internal_access01'

    def test_constituent_access(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        with closing(self.session.open_cursor(self.uri)) as cursor:
            with self.transaction(commit_timestamp=1):
                cursor['key'] = 'value'
            self.assertEqual(cursor['key'], 'value')
        self.conn.set_timestamp('stable_timestamp=1')
        self.session.checkpoint()

        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as debug:
            for suffix in ('.wt_ingest', '.wt_stable'):
                uri = 'file:test_layered_internal_access01' + suffix
                if wiredtiger.diagnostic_build():
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: self.session.open_cursor(uri), '/direct access to an internal table/')
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: self.session.verify(uri), '/direct access to an internal table/')
                else:
                    with closing(self.session.open_cursor(uri)):
                        pass
                with closing(debug.open_cursor(uri)):
                    pass
            for uri in ('file:WiredTigerShared.wt_stable', 'file:WiredTigerSharedHS.wt_stable'):
                with closing(debug.open_cursor(uri)):
                    for operation in (self.session.verify, self.session.salvage):
                        with self.assertRaises(wiredtiger.WiredTigerError) as error:
                            operation(uri)
                        self.assertIn(wiredtiger.wiredtiger_strerror(errno.EBUSY), str(error.exception))
