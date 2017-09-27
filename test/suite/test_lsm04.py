#!/usr/bin/env python
#
# Public Domain 2014-2017 MongoDB, Inc.
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
from wtscenario import make_scenarios

# test_lsm_key_format
#    Test LSM tree key configurations.
class test_lsm_key_format(wttest.WiredTigerTestCase):
    formats = [
        ('Q1', dict(key_format='key_format=Q', ok=False)),
        ('Q2', dict(key_format='key_format=q', ok=False)),
        ('r', dict(key_format='key_format=r', ok=False)),
        ('S', dict(key_format='key_format=S', ok=True)),
        ('u', dict(key_format='key_format=u', ok=True))
    ]

    scenarios = make_scenarios(formats)

    def test_lsm_key_format(self):
        if (self.ok):
            self.session.create(
                "table:A", self.key_format + ",value_format=S,type=lsm")
        else:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.create(
                "table:A", self.key_format + ",value_format=S,type=lsm"),
                '/key_format/')

if __name__ == '__main__':
    wttest.run()
