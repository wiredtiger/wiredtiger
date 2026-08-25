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

class _tiered_uri_deprecate:
    def _uri(self):
        return self.prefix + 'test_tiered_deprecate'

    def _assert_unsupported(self, expr):
        uri = self._uri()
        msg = f'{self.err_prefix}: {uri}'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, expr, '/' + re.escape(msg) + '/')
        err, _sub, last_msg = self.session.get_last_error()
        self.assertEqual(err, errno.ENOTSUP)
        self.assertEqual(last_msg, msg)

# Test that deprecated tiered storage URIs are refused.
class test_tiered_deprecate(_tiered_uri_deprecate, wttest.WiredTigerTestCase):
    uri_types = [
        ('object', dict(prefix='object:', err_prefix='unsupported object operation')),
        ('tier', dict(prefix='tier:', err_prefix='unknown object type')),
        ('tiered', dict(prefix='tiered:', err_prefix='unsupported object operation')),
    ]
    scenarios = make_scenarios(uri_types)

    def test_drop(self):
        self._assert_unsupported(lambda: self.session.drop(self._uri()))

    def test_alter(self):
        self._assert_unsupported(
            lambda: self.session.alter(self._uri(), 'access_pattern_hint=random'))

    def test_verify(self):
        self._assert_unsupported(lambda: self.session.verify(self._uri()))

    def test_salvage(self):
        self._assert_unsupported(lambda: self.session.salvage(self._uri()))

class test_tiered_deprecate_truncate(_tiered_uri_deprecate, wttest.WiredTigerTestCase):
    uri_types = [
        ('object', dict(prefix='object:', err_prefix='unsupported object operation')),
        ('tier', dict(prefix='tier:', err_prefix='unknown object type')),
    ]
    scenarios = make_scenarios(uri_types)

    def test_truncate(self):
        self._assert_unsupported(lambda: self.session.truncate(self._uri(), None, None, None))
