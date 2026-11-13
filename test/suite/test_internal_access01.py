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
import errno, importlib.util, sys
from pathlib import Path
from unittest import mock
import wiredtiger, wttest
from suite_subprocess import suite_subprocess

@wttest.skip_for_hook('disagg', 'The utility does not support disaggregated schema commands')
class test_internal_access01(wttest.WiredTigerTestCase, suite_subprocess):
    uri = 'table:WiredTigerApp'
    config = 'key_format=S,value_format=S'
    debug_config = 'debug=(allow_internal_access=true)'

    def test_session_access(self):
        if wiredtiger.diagnostic_build():
            operations = [
                lambda: self.session.create(self.uri, self.config),
                lambda: self.session.open_cursor(self.uri),
                lambda: self.session.drop(self.uri),
                lambda: self.session.alter(self.uri, 'access_pattern_hint=none'),
                lambda: self.session.compact(self.uri),
                lambda: self.session.truncate(self.uri, None, None, None),
                lambda: self.session.publish(self.uri),
            ]
            for operation in operations:
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError, operation,
                    '/name space may not be used/')
        else:
            self.session.create(self.uri, self.config)

        alias_config = self.config + ',source="file:WiredTigerApp.wt"'
        with closing(self.conn.open_session(self.debug_config)) as debug:
            debug.create(self.uri, self.config)
            with closing(debug.open_cursor(self.uri)) as cursor:
                cursor['key'] = 'value'
            if wiredtiger.diagnostic_build():
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.create('table:alias', alias_config),
                    '/name space may not be used/')
            else:
                self.session.create('table:alias', alias_config)
            debug.create('table:alias', alias_config)
        with closing(self.session.open_cursor('table:alias')) as cursor:
            self.assertEqual(cursor['key'], 'value')
        with closing(self.session.open_cursor('metadata:')) as cursor:
            self.assertIn('key_format=S', cursor[self.uri])

    def test_metadata_access(self):
        with closing(self.conn.open_session(self.debug_config)) as debug:
            for uri in ('file:WiredTiger.wt', 'file:WiredTigerHS.wt'):
                if wiredtiger.diagnostic_build():
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: self.session.open_cursor(uri), '/direct access to an internal table/')
                else:
                    with closing(self.session.open_cursor(uri)):
                        pass
                with closing(debug.open_cursor(uri)):
                    for operation in (self.session.verify, self.session.salvage):
                        with self.assertRaises(wiredtiger.WiredTigerError) as error:
                            operation(uri)
                        self.assertIn(wiredtiger.wiredtiger_strerror(errno.EBUSY), str(error.exception))

    def test_debug_tools(self):
        path = Path(__file__).resolve().parents[2] / 'tools' / 'wt_cmp_uri.py'
        with mock.patch.object(sys, 'path', [str(path.parent)] + sys.path):
            spec = importlib.util.spec_from_file_location('wt_cmp_uri_test', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        cursor = module.wt_open(self.conn, None, 'file:WiredTiger.wt')
        with closing(cursor.session), closing(cursor):
            self.assertIn(cursor.next(), (0, wiredtiger.WT_NOTFOUND))

        self.runWt(['create', '-c', self.config, self.uri])
        self.runWt(['write', self.uri, 'key', 'value'])
        with open('explore.in', 'w') as output:
            output.write('d key\\00\nq\n')
        self.runWt(['dump', '-e', self.uri], infilename='explore.in', outfilename='explore.out')
        self.check_file_contains('explore.out', 'Removed key')
        with closing(self.conn.open_session(self.debug_config)) as debug:
            with closing(debug.open_cursor(self.uri)) as cursor:
                self.assertEqual(list(cursor), [])
        self.runWt(['drop', self.uri])
        self.runWt(['-i', 'list'], errfilename='option.err', failure=True)
        self.check_file_contains('option.err', 'illegal option -- i')
