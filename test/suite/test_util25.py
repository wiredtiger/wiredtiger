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
from suite_subprocess import suite_subprocess

@wttest.skip_for_hook('disagg', 'Disaggregated storage does not support CLI schema commands')
class test_util25(wttest.WiredTigerTestCase, suite_subprocess):
    uri = 'table:WiredTiger.wt'
    config = 'key_format=S,value_format=S'

    def test_mutation_commands(self):
        # Separate tables leave the database's own metadata and HS files unchanged.
        if wiredtiger.diagnostic_build():
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.create(self.uri, self.config), '/direct access to an internal table/')
        self.runWt(['create', '-c', self.config, self.uri])
        self.runWt(['write', self.uri, 'key', 'value'])
        self.runWt(['read', self.uri, 'key'], outfilename='read.out')
        self.check_file_contains('read.out', 'value')
        self.runWt(['alter', self.uri, 'access_pattern_hint=none'])
        self.runWt(['compact', self.uri])
        self.runWt(['verify', self.uri])
        if wiredtiger.diagnostic_build():
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(self.uri), '/direct access to an internal table/')
        self.runWt(['truncate', self.uri])
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            with closing(session.open_cursor(self.uri)) as cursor:
                self.assertEqual(list(cursor), [])
        self.runWt(['drop', self.uri])
        with closing(self.session.open_cursor('metadata:')) as cursor:
            self.assertNotIn(self.uri, dict(cursor))

    def test_explore_access(self):
        self.runWt(['create', '-c', self.config, self.uri])
        self.runWt(['write', self.uri, 'key', 'value'])
        with open('explore.in', 'w') as output:
            output.write('d key\\00\nq\n')
        self.runWt(['dump', '-e', self.uri], infilename='explore.in', outfilename='explore.out')
        self.check_file_contains('explore.out', 'Removed key')
        with closing(self.conn.open_session('debug=(allow_internal_access=true)')) as session:
            with closing(session.open_cursor(self.uri)) as cursor:
                self.assertEqual(list(cursor), [])

    def test_internal_access_option_removed(self):
        self.runWt(['-i', 'list'], errfilename='option.err', failure=True)
        self.check_file_contains('option.err', 'illegal option -- i')
        self.runWt(['-?'], errfilename='help.err')
        self.check_file_not_contains('help.err', '    -i ')
