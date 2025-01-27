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

import os
import time
import wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered21.py
# Test a secondary can perform reads and writes to the ingest component
# of a layered table, without the stable component.
class test_layered21(wttest.WiredTigerTestCase, DisaggConfigMixin):
    uri = 'layered:test_layered21'

    conn_base_config = 'transaction_sync=(enabled,method=fsync),' \
                     + 'disaggregated=(stable_prefix=.,page_log=palm),'
    disagg_storages = gen_disagg_storages('test_layered21', disagg_only = True)

    nitems = 1

    scenarios = make_scenarios(disagg_storages)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignoreStdoutPattern('WT_VERB_RTS')

    # Load the page log extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', 'palm')

    # Custom test case setup
    def early_setup(self):
        os.mkdir('follower')
        # Create the home directory for the PALM k/v store, and share it with the follower.
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_secondary_ops_without_stable(self):
        session_config = 'key_format=S,value_format=S'
        self.session.create(self.uri, 'disaggregated=(max_consecutive_delta=1)')

        # TODO figure out self.extensionsConfig()
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' + self.conn_base_config + "disaggregated=(role=\"follower\")")
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, session_config)

        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
            cursor["Hi " + str(i)] = "There"
            cursor["OK " + str(i)] = "Go"
            if i % 25000 == 0:
                time.sleep(1)
        cursor.reset()

        # Ensure that all data makes it to the follower before we check.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # Reopen the follower connection.
        session_follow.close()
        conn_follow.close()
        self.pr("reopen the follower")
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',' + self.conn_base_config + "disaggregated=(role=\"follower\")")
        session_follow = conn_follow.open_session('')

        session_follow.create(self.uri + 'a')
        cursor_follow = session_follow.open_cursor(self.uri + 'a', None, None)
        cursor_follow["** Hello 0"] = "World"
        cursor_follow.close()
        cursor_follow = session_follow.open_cursor(self.uri + 'a', None, None)
        self.assertEqual(cursor_follow.next(), 0)
        self.assertEqual(cursor_follow.get_key(), b'** Hello 0')
        self.assertEqual(cursor_follow.get_value(), b'World')

        cursor_follow.close()
        session_follow.close()
        conn_follow.close()
