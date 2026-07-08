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

# Test remove returns not found when deleting an non-existent key

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_cursor11(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = 'statistics=(all),precise_checkpoint=true,' \
                  'disaggregated=(role="follower")'

    uri = f'layered:{test_name}'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_delete_non_existent_key(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, 'overwrite=false')
        self.session.begin_transaction()
        cursor.set_key(1)
        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        cursor.close()

    # An unpositioned blind remove on a follower skips the stable lookup and so cannot tell "exists
    # only in stable" from "doesn't exist at all"; it assumes the key exists rather than fail.
    def test_delete_non_existent_key_blind(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')

        cursor = self.session.open_cursor(self.uri, None, 'blind_remove=true')
        self.session.begin_transaction()
        cursor.set_key(1)
        self.assertEqual(cursor.remove(), 0)
        self.session.rollback_transaction()
        cursor.close()

    # A plain cursor that removes a positioned key, then removes the same (now stale) position
    # again without re-searching, loses its position on the resulting not-found: WiredTiger's
    # __cursor_state_restore only restores a saved *external* key copy, and this cursor was
    # positioned internally (on-page), so nothing gets restored.
    def test_positioned_double_remove_plain_loses_position(self):
        uri = 'table:' + self.test_name + '_plain'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)

        self.session.begin_transaction()
        cursor['k1'] = 'v1'
        self.session.commit_transaction()

        self.session.begin_transaction()
        cursor.set_key('k1')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor.get_key(), 'k1')

        self.assertEqual(cursor.remove(), wiredtiger.WT_NOTFOUND)
        self.assertRaisesWithMessage(
            wiredtiger.WiredTigerError, lambda: cursor.get_key(), "/requires key be set/")
        self.session.rollback_transaction()
        cursor.close()

    # A layered cursor does *not* lose position the same way a plain cursor does (this is
    # pre-existing behavior, not something the blind-remove branch adds): the second remove lands on
    # the "else if (current_cursor == c_ingest)" branch in __clayered_remove_from_ingest, since the
    # first remove's update() left VALUE_INT set on the ingest cursor. With blind_remove configured,
    # that branch treats the already-deleted key as a no-op and reports success rather than
    # not-found (matching the skip-stable path's handling of the same situation).
    def test_positioned_double_remove_blind_keeps_position(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri, None, 'blind_remove=true')

        self.session.begin_transaction()
        cursor['k2'] = 'v2'
        self.session.commit_transaction()

        self.session.begin_transaction()
        cursor.set_key('k2')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor.get_key(), 'k2')

        self.assertEqual(cursor.remove(), 0)
        self.assertEqual(cursor.get_key(), 'k2')
        self.session.rollback_transaction()
        cursor.close()
