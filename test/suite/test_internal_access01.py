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
import importlib.util, os, sys
from unittest import mock
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# Direct file access is guarded without restricting public metadata cursors.
@disagg_test_class
class test_internal_access01(wttest.WiredTigerTestCase, suite_subprocess):
    targets = [
        ('local_metadata', dict(internal_uri='file:WiredTiger.wt', shared=False)),
        ('local_hs', dict(internal_uri='file:WiredTigerHS.wt', shared=False)),
        ('shared_metadata', dict(internal_uri='file:WiredTigerShared.wt_stable', shared=True)),
        ('shared_hs', dict(internal_uri='file:WiredTigerSharedHS.wt_stable', shared=True)),
    ]
    scenarios = make_scenarios(gen_disagg_storages(), targets)
    msg = '/direct access to an internal table/'

    def conn_config(self):
        config = 'statistics=(all)'
        if self.is_disagg:
            config += ',disaggregated=(role=leader,lose_all_my_data=true)'
        return config

    def check_target(self):
        if self.shared and not self.is_disagg:
            self.skipTest('shared files exist only in disaggregated storage')

    def check_schema_access(self):
        operations = [
            lambda: self.session.create(self.internal_uri, 'key_format=S,value_format=S'),
            lambda: self.session.drop(self.internal_uri),
            lambda: self.session.truncate(self.internal_uri, None, None, None),
            lambda: self.session.alter(self.internal_uri, 'access_pattern_hint=none'),
            lambda: self.session.compact(self.internal_uri),
        ]
        for operation in operations:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError, operation, self.msg)

    def test_schema_access(self):
        self.check_schema_access()

    def test_global_names_all_prefixes(self):
        name = self.internal_uri.split(':', 1)[1]
        uris = [prefix + name for prefix in
            ('file:', 'table:', 'layered:', 'colgroup:', 'index:', 'custom:')]
        uris += ['colgroup:parent:' + name, 'index:parent:' + name]
        for uri in uris:
            operations = [
                lambda: self.session.create(uri, 'key_format=S,value_format=S'),
                lambda: self.session.drop(uri),
                lambda: self.session.alter(uri, 'access_pattern_hint=none'),
                lambda: self.session.truncate(uri, None, None, None),
                lambda: self.session.compact(uri),
            ]
            for operation in operations:
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError, operation, self.msg)
            if wiredtiger.diagnostic_build():
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.open_cursor(uri), self.msg)
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.open_cursor('statistics:' + uri), self.msg)

    def test_internal_source_aliases(self):
        self.session.create('table:column_alias',
            'key_format=S,value_format=S,columns=(key,value),colgroups=(group)')
        self.session.create('table:index_alias',
            'key_format=S,value_format=S,columns=(key,value)')
        cases = [
            ('table:table_alias', 'key_format=S,value_format=S'),
            ('colgroup:column_alias:group', 'columns=(value)'),
            ('index:index_alias:index', 'columns=(value)'),
        ]
        for uri, config in cases:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.create(uri, config + ',source="' + self.internal_uri + '"'),
                self.msg)

        self.session.create('file:ordinary_source.wt', 'key_format=S,value_format=S')
        self.assertEqual(self.session.create('table:ordinary_alias',
            'key_format=S,value_format=S,source="file:ordinary_source.wt"'), 0)
        with closing(self.session.open_cursor('table:ordinary_alias')):
            pass

    def check_schema_name_validation(self):
        uri = 'file:bad,name.wt'
        operations = [
            lambda: self.session.create(uri, 'key_format=S,value_format=S'),
            lambda: self.session.drop(uri),
            lambda: self.session.truncate(uri, None, None, None),
            lambda: self.session.alter(uri, 'access_pattern_hint=none'),
            lambda: self.session.compact(uri),
        ]
        for operation in operations:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError, operation,
                '/WiredTiger objects should not include grouping characters/')

    def test_schema_name_validation(self):
        self.check_schema_name_validation()

    def test_cursor_access(self):
        if not wiredtiger.diagnostic_build():
            self.check_target()
        self.session.checkpoint()
        cursors = [
            (self.internal_uri, None),
            ('statistics:' + self.internal_uri, None),
            (self.internal_uri, 'checkpoint=WiredTigerCheckpoint'),
        ]
        for uri, config in cursors:
            if wiredtiger.diagnostic_build():
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.open_cursor(uri, None, config), self.msg)
            else:
                with closing(self.session.open_cursor(uri, None, config)):
                    pass

    def test_inspect_internal_files(self):
        self.check_target()
        self.ignoreStdoutPattern('Removing local file due to disagg mode')
        self.session.checkpoint()
        config = self.extensionsConfig()
        if self.is_disagg:
            config += ',disaggregated=(role=leader)'
        self.close_conn()
        self.runWt(['-C', config, 'dump', self.internal_uri],
            outfilename='dump.out', errfilename='dump.err', closeconn=False)
        self.check_file_contains('dump.out', 'WiredTiger Dump')
        self.check_file_contains('dump.out', 'Data\n')
        self.runWt(['-C', config, 'stat', self.internal_uri],
            outfilename='stat.out', errfilename='stat.err', closeconn=False)
        self.check_file_contains('stat.out', 'cursor:')

    def test_compare_internal_files(self):
        self.check_target()
        tools_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
        with mock.patch.object(sys, 'path', [tools_dir] + sys.path):
            spec = importlib.util.spec_from_file_location(
                'wt_cmp_uri_test', os.path.join(tools_dir, 'wt_cmp_uri.py'))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        cursor = module.wt_open(self.conn, None, self.internal_uri)
        session = cursor.session
        try:
            self.assertIn(cursor.next(), (0, wiredtiger.WT_NOTFOUND))
        finally:
            cursor.close()
            session.close()

    def test_bypass_and_reconfigure(self):
        self.check_target()
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            with closing(session.open_cursor(self.internal_uri)) as cursor:
                session.reconfigure('debug=(allow_internal_access=false)')
                if wiredtiger.diagnostic_build():
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: session.open_cursor(None, cursor), self.msg)
                elif cursor.next() == 0:
                    with closing(session.open_cursor(None, cursor)):
                        pass
            if wiredtiger.diagnostic_build():
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: session.open_cursor(self.internal_uri), self.msg)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: session.alter(self.internal_uri, 'access_pattern_hint=none'), self.msg)
            session.reconfigure('debug=(allow_internal_access=true)')
            with closing(session.open_cursor(self.internal_uri)):
                pass
            session.checkpoint()
            with closing(session.open_cursor(self.internal_uri, None,
                'checkpoint=WiredTigerCheckpoint')):
                pass

    def test_cursor_projection_and_checkpoint(self):
        uri = 'table:public_checkpoint'
        self.session.create(uri, 'key_format=S,value_format=SS,columns=(key,v1,v2)')
        index_uri = 'index:public_checkpoint:by_v1'
        self.session.create(index_uri, 'columns=(v1)')
        with closing(self.session.open_cursor(uri)) as cursor:
            self.session.begin_transaction()
            cursor['key'] = ('left', 'right')
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(1))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()
        with closing(self.session.open_cursor(uri)) as cursor:
            self.session.begin_transaction()
            cursor['key'] = ('new_left', 'new_right')
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))

        cases = [
            (uri + '(v2,v1)', None, ('new_right', 'new_left')),
            (uri + '(v2,v1)', 'checkpoint=WiredTigerCheckpoint', ('right', 'left')),
            ('file:public_checkpoint.wt', 'checkpoint=WiredTigerCheckpoint', ('left', 'right')),
        ]
        for cursor_uri, config, expected in cases:
            with closing(self.session.open_cursor(cursor_uri, None, config)) as cursor:
                self.assertEqual(tuple(cursor['key']), expected)

        with closing(self.session.open_cursor(index_uri + '(v2,v1)')) as cursor:
            self.assertEqual(tuple(cursor['new_left']), ('new_right', 'new_left'))
        with closing(self.session.open_cursor(index_uri + '(v2,v1)', None,
            'checkpoint=WiredTigerCheckpoint')) as cursor:
            self.assertEqual(tuple(cursor['left']), ('right', 'left'))

        if wiredtiger.diagnostic_build():
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor('file:bad,name.wt'),
                '/WiredTiger objects should not include grouping characters/')

    def test_checkpoint_view_cursor_access(self):
        if not wiredtiger.diagnostic_build():
            self.skipTest('cursor admission checks require a diagnostic build')
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.internal_uri + '/missing'), self.msg)

    def test_maintenance_access(self):
        self.check_target()
        # Keep the file busy so admission is tested without actually salvaging it.
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            with closing(session.open_cursor(self.internal_uri)):
                for operation in (self.session.verify, self.session.salvage):
                    with self.assertRaises(wiredtiger.WiredTigerError) as error:
                        operation(self.internal_uri)
                    self.assertIn('Device or resource busy', str(error.exception))

    def test_metadata_aliases(self):
        self.session.create('table:public', 'key_format=S,value_format=S')
        for uri in ('metadata:', 'metadata:create'):
            with closing(self.session.open_cursor(uri)) as cursor:
                self.assertIn('key_format=S', cursor['table:public'])

    def test_name_lookalikes(self):
        operations = [self.session.verify, self.session.salvage]
        if wiredtiger.diagnostic_build():
            operations.append(self.session.open_cursor)
        for operation in operations:
            if self.shared and self.is_disagg:
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: operation(self.internal_uri + '_other'), self.msg)
            else:
                with self.assertRaises(wiredtiger.WiredTigerError) as error:
                    operation(self.internal_uri + '_other')
                self.assertIn('No such file or directory', str(error.exception))
                self.ignoreStderrPatternIfExists('No such file|not found|unknown')

    def test_in_memory_schema_access(self):
        if self.is_disagg or self.shared:
            self.skipTest('in-memory databases use only local files')
        self.reopen_conn(config='in_memory=true,statistics=(all)')
        self.check_schema_access()

    def test_in_memory_schema_name_validation(self):
        if self.is_disagg or self.shared:
            self.skipTest('in-memory databases use only local files')
        self.reopen_conn(config='in_memory=true,statistics=(all)')
        self.check_schema_name_validation()
