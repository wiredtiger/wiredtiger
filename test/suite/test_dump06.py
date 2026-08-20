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

import wttest
from suite_subprocess import suite_subprocess

# Test that a zero-length 'u' formatted value is dumped correctly with
# the pretty-print (-p) option, rather than showing the previous
# record's value.
class test_dump06(wttest.WiredTigerTestCase, suite_subprocess):
    uri = 'table:test_dump06'
    table_format = 'key_format=q,value_format=u'

    pretty_file = 'dump_pretty.out'
    hex_file = 'dump_hex.out'
    json_file = 'dump_json.out'

    def populate_table(self):
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor[2] = b'A'
        cursor[3] = b''
        cursor[5] = b'B'
        cursor[6] = b'C'
        cursor.close()

    def dump_value_lines(self, filename):
        lines = open(filename).readlines()
        start = lines.index('Data\n') + 1
        pairs = lines[start:]
        return [pairs[i + 1].rstrip('\n') for i in range(0, len(pairs), 2)]

    def test_dump_pretty_empty_value(self):
        self.session.create(self.uri, self.table_format)
        self.populate_table()

        self.runWt(['dump', '-p', self.uri], outfilename=self.pretty_file)
        self.runWt(['dump', '-x', self.uri], outfilename=self.hex_file)
        self.runWt(['dump', '-j', self.uri], outfilename=self.json_file)

        pretty = self.dump_value_lines(self.pretty_file)
        hexvals = self.dump_value_lines(self.hex_file)

        self.assertEqual(pretty, ['A', '', 'B', 'C'])
        self.assertEqual(hexvals[1], '')
        self.check_file_contains(self.json_file, '"key0" : 3,\n"value0" : ""\n')

    def test_dump_pretty_empty_value_first_record(self):
        self.session.create(self.uri, self.table_format)
        cursor = self.session.open_cursor(self.uri, None, None)
        cursor[1] = b''
        cursor[2] = b'A'
        cursor.close()

        self.runWt(['dump', '-p', self.uri], outfilename=self.pretty_file)
        pretty = self.dump_value_lines(self.pretty_file)
        self.assertEqual(pretty, ['', 'A'])

    def test_dump_pretty_composite_format_with_empty_u(self):
        uri_qu = 'table:test_dump06_qu'
        uri_uq = 'table:test_dump06_uq'

        self.session.create(uri_qu, 'key_format=q,value_format=qu')
        cursor = self.session.open_cursor(uri_qu, None, None)
        cursor[1] = (10, b'X')
        cursor[2] = (20, b'')
        cursor[3] = (30, b'Y')
        cursor.close()

        self.session.create(uri_uq, 'key_format=q,value_format=uq')
        cursor = self.session.open_cursor(uri_uq, None, None)
        cursor[1] = (b'X', 100)
        cursor[2] = (b'', 200)
        cursor[3] = (b'Y', 300)
        cursor.close()

        self.runWt(['dump', '-p', uri_qu], outfilename=self.pretty_file)
        qu = self.dump_value_lines(self.pretty_file)
        self.assertEqual(qu, ['10,X', '20,', '30,Y'])

        self.runWt(['dump', '-p', uri_uq], outfilename=self.pretty_file)
        uq = self.dump_value_lines(self.pretty_file)
        self.assertEqual(uq, ['X100', '200', 'Y300'])

if __name__ == '__main__':
    wttest.run()
