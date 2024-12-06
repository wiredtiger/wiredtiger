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
# test dump utility with -j and -k options
class test_dump04(wttest.WiredTigerTestCase, suite_subprocess):
    output = "dump.out"

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
        uri = 'table:test_dump'
        create_params = 'key_format=u,value_format=u'
        self.session.create(uri, create_params)

        cursor = self.session.open_cursor(uri)
        for i in range(1, 5):
            cursor[str(i)] = str(i)
        cursor['key'] = 'value'
        cursor.close()

        # Perform checkpoint, to clean the dirty pages and place values on disk.
        self.session.checkpoint()

        # Call dump with -j option
        self.runWt(['dump', '-j', uri], outfilename=self.output)
        self.check_file_contains(self.output, self.key_to_json(self.string_to_unicode("key")))
        self.check_file_contains(self.output, self.value_to_json(self.string_to_unicode("value")))
        for i in range(1, 5):
            self.check_file_contains(self.output, self.key_to_json(self.string_to_unicode(str(i))))
            self.check_file_contains(self.output, self.value_to_json(self.string_to_unicode(str(i))))

        # Call dump with -k option
        self.runWt(['dump', '-k', "key", uri], outfilename=self.output)
        self.check_file_contains(self.output, "key\n")
        self.check_file_contains(self.output, "value\n")
        for i in range(1, 5):
            self.check_file_not_contains(self.output, f"{i}\n")
            self.check_file_not_contains(self.output, f"{i}\n")

        # Call dump with -j and -k options
        self.runWt(['dump', '-j', '-k', "key", uri], outfilename=self.output)
        self.check_file_contains(self.output, self.key_to_json(self.string_to_unicode("key")))
        self.check_file_contains(self.output, self.value_to_json(self.string_to_unicode("value")))
        for i in range(1, 5):
            self.check_file_not_contains(self.output, self.key_to_json(self.string_to_unicode(str(i))))
            self.check_file_not_contains(self.output, self.key_to_json(self.string_to_unicode(str(i))))

        # Call dump with -j and -k options missing key
        self.runWt(['dump', '-j', '-k', "table", uri], outfilename=self.output)
        self.check_file_not_contains(self.output, self.key_to_json(self.string_to_unicode("key")))
        self.check_file_not_contains(self.output, self.value_to_json(self.string_to_unicode("value")))
        for i in range(1, 5):
            self.check_file_not_contains(self.output, self.key_to_json(self.string_to_unicode(str(i))))
            self.check_file_not_contains(self.output, self.key_to_json(self.string_to_unicode(str(i))))
