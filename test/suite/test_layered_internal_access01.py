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
import re
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

    def check_cursor_access(self, uri, config=None):
        if wiredtiger.diagnostic_build():
            self.expect_refused(lambda: self.session.open_cursor(uri, None, config))
        else:
            with closing(self.session.open_cursor(uri, None, config)):
                pass

    def test_refused_without_debug_config(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # The layered table itself works through a regular session.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c['key'] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        c.close()

        self.session.checkpoint()
        self.check_cursor_access(self.ingest_uri)
        self.check_cursor_access(self.stable_uri)
        self.check_cursor_access(self.stable_uri, 'checkpoint=WiredTigerCheckpoint')
        self.check_cursor_access('statistics:' + self.stable_uri)
        with closing(self.session.open_cursor('metadata:')) as cursor:
            checkpoint = re.search(r'WiredTigerCheckpoint\.\d+', cursor[self.stable_uri]).group(0)
        self.check_cursor_access(self.stable_uri + '/' + checkpoint)
        self.check_cursor_access('file:WiredTigerShared.wt_stable')

        # Schema operations on the constituents are refused.
        self.expect_refused(lambda: self.session.create(
            'file:xyzzy.wt_ingest', 'key_format=S,value_format=S'))
        self.expect_refused(lambda: self.session.drop(self.ingest_uri))
        self.expect_refused(lambda: self.session.truncate(self.ingest_uri, None, None, None))
        self.expect_refused(lambda: self.session.alter(self.ingest_uri, 'access_pattern_hint=none'))
        self.expect_refused(lambda: self.session.compact(self.stable_uri))

        # Verify and salvage are guarded too.
        self.expect_refused(lambda: self.session.verify(self.stable_uri))
        self.expect_refused(lambda: self.session.salvage(self.stable_uri))
        self.expect_refused(lambda: self.session.verify(self.ingest_uri))
        self.expect_refused(lambda: self.session.salvage(self.ingest_uri))

        logical_uri = 'table:' + self.uri_base + '_logical.wt_stable'
        self.expect_refused(lambda: self.session.create(logical_uri, 'key_format=S,value_format=S'))
        if wiredtiger.diagnostic_build():
            self.expect_refused(lambda: self.session.open_cursor(logical_uri))
        self.expect_refused(lambda: self.session.alter(logical_uri, 'access_pattern_hint=none'))
        self.expect_refused(lambda: self.session.truncate(logical_uri, None, None, None))
        self.expect_refused(lambda: self.session.drop(logical_uri))

    def test_constituent_name_prefixes(self):
        for prefix in ('table:', 'layered:', 'colgroup:', 'index:', 'custom:'):
            for suffix in ('.wt_ingest', '.wt_stable'):
                uri = prefix + 'reserved' + suffix
                operations = [
                    lambda: self.session.create(uri, 'key_format=S,value_format=S'),
                    lambda: self.session.drop(uri),
                    lambda: self.session.alter(uri, 'access_pattern_hint=none'),
                    lambda: self.session.truncate(uri, None, None, None),
                    lambda: self.session.compact(uri),
                    lambda: self.session.verify(uri),
                    lambda: self.session.salvage(uri),
                ]
                for operation in operations:
                    self.expect_refused(operation)
                if wiredtiger.diagnostic_build():
                    self.expect_refused(lambda: self.session.open_cursor(uri))
                    self.expect_refused(lambda: self.session.open_cursor('statistics:' + uri))

    def test_allowed_with_debug_config(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        debug_session = self.conn.open_session('debug=(allow_internal_access=true)')
        c = debug_session.open_cursor(self.ingest_uri)
        c.close()
        c = debug_session.open_cursor(self.stable_uri)
        c.close()
        c = debug_session.open_cursor('statistics:' + self.stable_uri)
        c.close()

        file_uri = 'file:debug_access.wt_stable'
        debug_session.create(file_uri, 'key_format=S,value_format=S')
        with closing(debug_session.open_cursor(file_uri)):
            pass
        debug_session.alter(file_uri, 'access_pattern_hint=none')
        debug_session.truncate(file_uri, None, None, None)
        debug_session.drop(file_uri)

        logical_uri = 'table:debug_access.wt_stable'
        debug_session.create(logical_uri, 'key_format=S,value_format=S')
        with closing(debug_session.open_cursor(logical_uri)):
            pass
        debug_session.alter(logical_uri, 'access_pattern_hint=none')
        debug_session.truncate(logical_uri, None, None, None)
        debug_session.drop(logical_uri)

        missing_uri = 'file:missing.wt_stable'
        for operation in (debug_session.verify, debug_session.salvage):
            try:
                operation(missing_uri)
            except wiredtiger.WiredTigerError as e:
                self.assertNotIn('direct access to an internal table', str(e))
        debug_session.close()

    def test_constituent_source_aliases(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session.create('table:column_alias',
            'key_format=S,value_format=S,columns=(key,value),colgroups=(group)')
        self.session.create('table:index_alias',
            'key_format=S,value_format=S,columns=(key,value)')
        cases = [
            ('table:table_alias', 'key_format=S,value_format=S'),
            ('colgroup:column_alias:group', 'columns=(value)'),
            ('index:index_alias:index', 'columns=(value)'),
        ]
        for source in (self.ingest_uri, self.stable_uri):
            for uri, config in cases:
                self.expect_refused(
                    lambda: self.session.create(uri, config + ',source="' + source + '"'))

        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            source = 'file:alias_bypass.wt_stable'
            session.create(source, 'key_format=S,value_format=S')
            session.create('table:allowed_alias',
                'key_format=S,value_format=S,source="' + source + '"')
            with closing(session.open_cursor('table:allowed_alias')):
                pass
            session.drop('table:allowed_alias')

    def test_reconfigure(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        session = self.conn.open_session('')
        self.check_cursor_access(self.ingest_uri)

        session.reconfigure('debug=(allow_internal_access=true)')
        c = session.open_cursor(self.ingest_uri)
        session.reconfigure('debug=(allow_internal_access=false)')
        if wiredtiger.diagnostic_build():
            self.expect_refused(lambda: session.open_cursor(None, c))
        c.close()
        if wiredtiger.diagnostic_build():
            self.expect_refused(lambda: session.open_cursor(self.ingest_uri))
        else:
            with closing(session.open_cursor(self.ingest_uri)):
                pass
        session.close()

    def test_cursor_truncate_not_rechecked(self):
        uri = 'file:range_access.wt_stable'
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            session.create(uri, 'key_format=S,value_format=S')
            with closing(session.open_cursor(uri)) as start, closing(session.open_cursor(uri)) as stop:
                session.begin_transaction()
                start['a'] = 'value'
                start['z'] = 'value'
                session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))

                session.reconfigure('debug=(allow_internal_access=false)')
                start.set_key('a')
                stop.set_key('z')
                session.begin_transaction()
                self.assertEqual(session.truncate(None, start, stop, None), 0)
                session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))
                start.set_key('a')
                self.assertEqual(start.search(), wiredtiger.WT_NOTFOUND)

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
            outfilename='dump.out', errfilename='dump.err', closeconn=False)
        self.check_file_contains('dump.out', 'value')
        self.runWt(['-C', config, 'dump', self.ingest_uri],
            outfilename='ingest.out', errfilename='ingest.err', closeconn=False)
        self.check_file_contains('ingest.out', 'Data\n')
        self.runWt(['-i', '-C', config, 'dump', self.stable_uri],
            outfilename='dump.out', errfilename='dump.err', closeconn=False)
        self.check_file_contains('dump.out', 'value')
        self.runWt(['-C', config, 'read', 'file:WiredTiger.wt', self.uri],
            outfilename='read.out', errfilename='read.err', closeconn=False)
        self.check_file_contains('read.out', 'value_format=S')
        self.runWt(['-C', config, 'stat', self.stable_uri],
            outfilename='stat.out', errfilename='stat.err', closeconn=False)
        self.check_file_contains('stat.out', 'cursor:')
        self.runWt(['-p', '-C', config, 'verify', self.stable_uri],
            outfilename='verify.out', errfilename='verify.err', closeconn=False, failure=True)
        self.check_file_contains('verify.err', 'direct access to an internal table')
