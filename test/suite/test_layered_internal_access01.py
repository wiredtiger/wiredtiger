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
from helper_disagg import disagg_test_class
from suite_subprocess import suite_subprocess

# test_layered_internal_access01.py
#   Direct access to the ingest and stable constituents of a layered table is
#   refused unless the session is configured with debug=(allow_internal_access).
@disagg_test_class
class test_layered_internal_access01(wttest.WiredTigerTestCase, suite_subprocess):

    test_name = __qualname__
    uri_base = test_name
    conn_config = 'disaggregated=(role="leader"),' \
                + 'disaggregated=(lose_all_my_data=true)'

    uri = "layered:" + uri_base
    ingest_uri = "file:" + uri_base + ".wt_ingest"
    stable_uri = "file:" + uri_base + ".wt_stable"

    msg = '/direct access to an internal table/'

    def expect_refused(self, expr):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, expr, self.msg)

    def test_refused_without_debug_config(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # The layered table itself works through a regular session.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['key'] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        # Data, checkpoint and statistics cursors on the constituents are refused.
        self.expect_refused(lambda: self.session.open_cursor(self.ingest_uri))
        self.expect_refused(lambda: self.session.open_cursor(self.stable_uri))
        self.expect_refused(lambda: self.session.open_cursor(
            self.stable_uri, None, 'checkpoint=WiredTigerCheckpoint'))
        self.expect_refused(lambda: self.session.open_cursor(
            'statistics:' + self.stable_uri))

        # Shared internal tables are refused with a message that does not point at a
        # layered table, since none exists for them.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor('file:WiredTigerShared.wt_stable'),
            '/direct access to an internal table is not allowed/')

        # Schema operations on the constituents are refused.
        self.expect_refused(lambda: self.session.create(
            'file:xyzzy.wt_ingest', 'key_format=S,value_format=S'))
        self.expect_refused(lambda: self.session.drop(self.ingest_uri))
        self.expect_refused(lambda: self.session.truncate(self.ingest_uri, None, None, None))
        self.expect_refused(lambda: self.session.alter(self.ingest_uri, 'access_pattern_hint=none'))

        # Verify and salvage are guarded too.
        self.expect_refused(lambda: self.session.verify(self.stable_uri))
        self.expect_refused(lambda: self.session.salvage(self.stable_uri))

        # Constituent suffixes are reserved across schema and cursor APIs, including table URIs.
        logical_uri = 'table:' + self.uri_base + '_logical.wt_stable'
        self.expect_refused(lambda: self.session.create(
            logical_uri, 'key_format=S,value_format=S'))
        self.expect_refused(lambda: self.session.open_cursor(logical_uri))
        self.expect_refused(lambda: self.session.alter(logical_uri, 'access_pattern_hint=none'))
        self.expect_refused(lambda: self.session.drop(logical_uri))
        self.expect_refused(lambda: self.session.truncate(logical_uri, None, None, None))

        debug_session = self.conn.open_session('debug=(allow_internal_access=true)')
        debug_session.create(logical_uri, 'key_format=S,value_format=S')
        c = debug_session.open_cursor(logical_uri)
        c.close()
        debug_session.alter(logical_uri, 'access_pattern_hint=none')
        debug_session.truncate(logical_uri, None, None, None)
        debug_session.drop(logical_uri)
        debug_session.close()

    def test_allowed_with_debug_config(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        debug_session = self.conn.open_session('debug=(allow_internal_access=true)')
        c = debug_session.open_cursor(self.ingest_uri)
        c.close()
        c = debug_session.open_cursor(self.stable_uri)
        c.close()
        c = debug_session.open_cursor('statistics:' + self.stable_uri)
        c.close()

        missing_uri = 'file:missing.wt_stable'
        for operation in (debug_session.verify, debug_session.salvage):
            try:
                operation(missing_uri)
            except wiredtiger.WiredTigerError as e:
                self.assertNotIn('direct access to an internal table', str(e))
        debug_session.close()

    def test_reconfigure(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        session = self.conn.open_session('')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: session.open_cursor(self.ingest_uri), self.msg)

        session.reconfigure('debug=(allow_internal_access=true)')
        c = session.open_cursor(self.ingest_uri)
        session.reconfigure('debug=(allow_internal_access=false)')
        self.expect_refused(lambda: session.open_cursor(None, c))
        c.close()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: session.open_cursor(self.ingest_uri), self.msg)
        session.close()

    def test_cli_internal_access(self):
        self.ignoreStdoutPattern('Removing local file due to disagg mode')
        self.session.create(self.uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['key'] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        config = self.extensionsConfig() + ',disaggregated=(role="leader")'
        self.close_conn()
        self.runWt(['-C', config, 'dump', self.stable_uri],
            outfilename='dump.out', errfilename='dump.err', closeconn=False, failure=True)
        self.check_file_contains('dump.err', 'direct access to an internal table')
        self.runWt(['-i', '-C', config, 'dump', self.stable_uri],
            outfilename='dump.out', errfilename='dump.err', closeconn=False)
        self.check_file_contains('dump.out', 'value')
