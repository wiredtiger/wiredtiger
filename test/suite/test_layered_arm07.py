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

# test_layered_arm07.py
#    A table created while armed, so with no stable constituent in the demotion checkpoint, keeps a
#    write at or below that checkpoint's timestamp when the node takes the lead again.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_arm07_create(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    disagg_storages = gen_disagg_storages(disagg_only=True)
    preserve = [
        ('discard', dict(preserve_config='')),
        ('preserve', dict(preserve_config='preserve_prepared=true,')),
    ]
    scenarios = make_scenarios(disagg_storages, preserve)

    @property
    def conn_base_config(self):
        return 'statistics=(all),precise_checkpoint=true,' + self.preserve_config

    @property
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    uri = 'layered:test_layered_arm07_create'

    def test_armed_create_below_checkpoint(self):
        self.set_global_ts(1, 1)
        self.arm()
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k': 'v'}, 15)
        self.checkpoint_at(20)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.assertEqual(self.read_kvs_at(self.uri, 25), {'k': 'v'})

        self.promote()
        self.assertEqual(self.read_kvs_at(self.uri, 25), {'k': 'v'})
        self.checkpoint_at(25)

        conn_c = self.open_node('node_c', config=self.conn_base_config)
        conn_c.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                             ',stable_timestamp=' + self.timestamp_str(25))
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 25, session_c), {'k': 'v'})
        session_c.close()
        conn_c.close()

if __name__ == '__main__':
    wttest.run()
