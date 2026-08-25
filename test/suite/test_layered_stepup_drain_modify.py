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

# test_layered_stepup_drain_modify.py
#   A follower modify that moves a value into, out of, or within the tombstone namespace
#   (values beginning with the bytes \x14\x14) must survive the step-up drain of the ingest
#   table and read back unchanged from the stable table.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup_drain_modify(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    uri = f'layered:{test_name}'

    values = [
        ('into_namespace', dict(base=b'ab\xff\xff\x00',
            mods=[(b'\x14\x14', 0, 2)], expected=b'\x14\x14\xff\xff\x00')),
        ('out_of_namespace', dict(base=b'\x14\x14abc',
            mods=[(b'zz', 0, 2)], expected=b'zzabc')),
        ('within_namespace', dict(base=b'\x14\x14abc',
            mods=[(b'X', 2, 1)], expected=b'\x14\x14Xbc')),
        ('to_exact_tombstone', dict(base=b'\x14\x14X',
            mods=[(b'', 2, 1)], expected=b'\x14\x14')),
        ('outside_namespace', dict(base=b'plain',
            mods=[(b'\xaa', 5, 0)], expected=b'plain\xaa')),
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, values)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setUp(self):
        super().setUp()
        # Reading a tombstone-namespace value off the stable table logs a warning; expected here.
        self.ignoreStdoutPattern('stable table value in the tombstone namespace')

    def test_stepup_drain_after_modify(self):
        self.session.create(self.uri, 'key_format=S,value_format=u')

        follow_conn = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        follow = follow_conn.open_session('')
        follow.create(self.uri, 'key_format=S,value_format=u')

        # Write the base value on the follower.
        c = follow.open_cursor(self.uri)
        follow.begin_transaction()
        c['k'] = self.base
        follow.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Modify it.
        follow.begin_transaction()
        c.set_key('k')
        self.assertEqual(c.modify([wiredtiger.Modify(*m) for m in self.mods]), 0)
        follow.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        # The modified value must read back on the follower before the drain.
        self.assertEqual(c['k'], self.expected)
        c.close()

        # Step the follower up; the drain moves every ingest version to the stable table.
        self.conn.close('debug=(skip_checkpoint=true)')
        follow_conn.reconfigure('disaggregated=(role="leader")')
        follow_conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        follow.checkpoint()

        # The value must read back unchanged after the drain.
        c = follow.open_cursor(self.uri)
        self.assertEqual(c['k'], self.expected)
        c.close()
        follow_conn.close()
