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

# test_dump04.py
# Test dump utility with -j and -k options
class test_dump04(wttest.WiredTigerTestCase, suite_subprocess):
    output = "dump.out"
    uri = 'table:test_dump'
    create_params = 'key_format=u,value_format=u'
    dict = {
        'key' : 'value',
        'key0' : 'value0',
        '1' : '1',
    }

    def wrap_in_json(self, s1, s2):
        return f"\"{s1}\" : \"{s2}\""

    def key_to_json(self, s):
        return self.wrap_in_json("key0", s)

    def value_to_json(self, s):
        return self.wrap_in_json("value0", s)

    def string_to_unicode(self, s):
        ret = ""
        for c in s:
            ret += '\\u{:04x}'.format(ord(c))
        return ret

    def test_dump(self):
        self.session.create(self.uri, self.create_params)
        cursor = self.session.open_cursor(self.uri)
        for (k, v) in self.dict.items():
            cursor[k] = v
        cursor.close()

        # Perform checkpoint, to clean the dirty pages and place values on disk.
        self.session.checkpoint()

        cases = [
            (True, ''),
            (False, 'key'),
            (True, '1'),
            (True, 'table')
        ]

        for (json, key) in cases:
            args = ['dump']
            if json:
                args.append('-j')
            if key:
                args += ['-k', key]
            args.append(self.uri)
            self.runWt(args, outfilename=self.output)

            for (k, v) in self.dict.items():
                if json:
                    ckey = self.key_to_json(self.string_to_unicode(k))
                    cvalue = self.value_to_json(self.string_to_unicode(v))
                else:
                    ckey = f"{k}\n"
                    cvalue = f"{v}\n"
                if not key or k == key:
                    self.check_file_contains(self.output, ckey)
                    self.check_file_contains(self.output, cvalue)
                else:
                    self.check_file_not_contains(self.output, ckey)
                    self.check_file_not_contains(self.output, cvalue)

