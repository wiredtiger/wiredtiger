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

import errno, re, wiredtiger, wttest
from wtscenario import make_scenarios

# Leftover tiered URI prefixes are rejected by schema drop, alter, truncate, verify, and salvage.
class test_tiered01(wttest.WiredTigerTestCase):
    uri_types = [
        ('object', dict(prefix='object:', err_prefix='unsupported object operation')),
        ('tier', dict(prefix='tier:', err_prefix='unknown object type')),
        ('tiered', dict(prefix='tiered:', err_prefix='unsupported object operation')),
    ]
    scenarios = make_scenarios(uri_types)

    def _assert_unsupported(self, expr, uri):
        msg = f'{self.err_prefix}: {uri}'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, expr, '/' + re.escape(msg) + '/')
        err, _sub, last_msg = self.session.get_last_error()
        self.assertEqual(err, errno.ENOTSUP)
        self.assertEqual(last_msg, msg)

    def test_leftover_uri_ops(self):
        uri = self.prefix + 'test_tiered01'
        self._assert_unsupported(lambda: self.session.drop(uri), uri)
        self._assert_unsupported(lambda: self.session.alter(uri, 'access_pattern_hint=random'), uri)
        self._assert_unsupported(lambda: self.session.verify(uri), uri)
        self._assert_unsupported(lambda: self.session.salvage(uri), uri)
        # session.truncate still treats tiered: as a btree URI and opens a file cursor.
        if self.prefix == 'tiered:':
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.truncate(uri, None, None, None),
                '/No such file or directory/')
            err, _sub, _msg = self.session.get_last_error()
            self.assertEqual(err, errno.ENOENT)
        else:
            self._assert_unsupported(lambda: self.session.truncate(uri, None, None, None), uri)
