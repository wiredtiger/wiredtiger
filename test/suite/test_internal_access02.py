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
import wiredtiger, wttest

@wttest.skip_for_hook('disagg', 'Layered tables do not support named column groups or indexes')
class test_internal_access02(wttest.WiredTigerTestCase):
    config = 'key_format=S,value_format=S'
    msg = '/name space may not be used/'

    def test_namespace_uri_shapes(self):
        self.session.create('table:column_parent',
            self.config + ',columns=(key,value),colgroups=(WiredTigerGroup)')
        self.session.create('table:index_parent', self.config + ',columns=(key,value)')
        cases = [
            ('file:WiredTigerFile.wt', self.config),
            ('table:WiredTigerApp', self.config),
            ('colgroup:column_parent:WiredTigerGroup', 'columns=(value)'),
            ('index:index_parent:WiredTigerIndex', 'columns=(value)'),
        ]
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as debug:
            for uri, config in cases:
                if wiredtiger.diagnostic_build():
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: self.session.create(uri, config), self.msg)
                else:
                    self.assertEqual(self.session.create(uri, config), 0)
                self.assertEqual(debug.create(uri, config), 0)
                if wiredtiger.diagnostic_build():
                    self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                        lambda: self.session.open_cursor(uri), self.msg)
                else:
                    with closing(self.session.open_cursor(uri)) as cursor:
                        self.assertEqual(list(cursor), [])
                with closing(debug.open_cursor(uri)) as cursor:
                    self.assertEqual(list(cursor), [])

    def test_source_namespace_and_alias_reuse(self):
        source = 'file:WiredTigerBacking.wt'
        alias = 'table:ordinary_alias'
        alias_config = self.config + ',source="' + source + '"'
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as debug:
            debug.create(source, self.config)
            with closing(debug.open_cursor(source)) as cursor:
                cursor['key'] = 'value'
            if wiredtiger.diagnostic_build():
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.create(alias, alias_config), self.msg)
            else:
                self.assertEqual(self.session.create(alias, alias_config), 0)
            debug.create(alias, alias_config)
        with closing(self.session.open_cursor(alias)) as cursor:
            self.assertEqual(dict(cursor), {'key': 'value'})

    def test_publish_namespace(self):
        uri = 'table:WiredTigerApp'
        if wiredtiger.diagnostic_build():
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.publish(uri), self.msg)
        else:
            self.assertEqual(self.session.publish(uri), 0)
