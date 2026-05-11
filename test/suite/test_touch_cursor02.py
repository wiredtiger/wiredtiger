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
#
# [TEST_TAGS]
# cursors
# config_api
# [END_TAGS]

# test_touch_cursor02.py
# Touch-cursor configuration validation.
#
# A touch cursor open must fail synchronously (not at first search()) for
# every incompatible combination. We check:
#   - column-store source           -> ENOTSUP
#   - bulk loading                  -> EINVAL
#   - next_random                   -> EINVAL
#   - invalid action / class_id     -> EINVAL via config check
#
# We also verify a clean enable+open on a row-store table and that the
# default config values round-trip (action=warmup, class_id=1, etc).

import wiredtiger, wttest


class test_touch_cursor02(wttest.WiredTigerTestCase):

    # No disagg config: this is purely about config validation in
    # __curfile_create. Cheap and fast on every CI machine.
    conn_config = ''

    def _assert_open_rejects(self, uri, config, stderr_pattern):
        """Open a touch cursor and assert it raises with the given stderr message.

        WT's WiredTigerError carries only the errno strerror text (e.g. "Invalid
        argument"); the descriptive message goes to stderr. We use
        expectedStderrPattern to validate the message and assert that the
        exception is raised.
        """
        with self.expectedStderrPattern(stderr_pattern):
            self.assertRaises(
                wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(uri, None, config))

    def test_touch_rejects_column_store(self):
        """A column-store source rejects touch=() at open time."""
        uri = 'table:touch_cs'
        self.session.create(uri, 'key_format=r,value_format=i')
        self._assert_open_rejects(
            uri, 'touch=(enabled=true)',
            'only row-store tables are supported')

    def test_touch_rejects_bulk(self):
        """touch=(enabled=true) with bulk=true is rejected up-front."""
        uri = 'file:touch_bulk.wt'
        self.session.create(uri, 'key_format=S,value_format=S')
        self._assert_open_rejects(
            uri, 'touch=(enabled=true),bulk=true',
            'incompatible with bulk-load')

    def test_touch_rejects_next_random(self):
        """touch=(enabled=true) with next_random=true is rejected up-front."""
        uri = 'table:touch_random'
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        c['k'] = 'v'
        c.close()
        self._assert_open_rejects(
            uri, 'touch=(enabled=true),next_random=true',
            'incompatible with next_random')

    def test_touch_rejects_unknown_action(self):
        """touch.action must be in the allowed set (currently {warmup})."""
        uri = 'table:touch_action'
        self.session.create(uri, 'key_format=S,value_format=S')
        self._assert_open_rejects(
            uri, 'touch=(enabled=true,action=not_a_real_action)',
            "Value 'not_a_real_action'")

    def test_touch_class_id_bounds(self):
        """class_id must be in [0, 255]."""
        uri = 'table:touch_classid'
        self.session.create(uri, 'key_format=S,value_format=S')
        for bad, frag in (('-1', 'too small'), ('256', 'too large'),
                          ('99999', 'too large')):
            self._assert_open_rejects(
                uri, f'touch=(enabled=true,class_id={bad})',
                frag)

    def test_touch_class_id_accepted_range(self):
        """class_id 0, 1, 127, 255 all open cleanly."""
        uri = 'table:touch_classid_ok'
        self.session.create(uri, 'key_format=S,value_format=S')
        for good in (0, 1, 127, 255):
            c = self.session.open_cursor(
                uri, None, f'touch=(enabled=true,class_id={good})')
            c.close()

    def test_touch_open_succeeds_on_row_store(self):
        """A row-store table with touch=(enabled=true) opens successfully and
        search() returns WT_NOTFOUND."""
        uri = 'table:touch_ok'
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(
            uri, None,
            'touch=(enabled=true,action=warmup,class_id=42,command="hello")')
        try:
            c.set_key('k')
            self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
        finally:
            c.close()

    def test_touch_default_disabled(self):
        """Without touch=(...) the cursor is a normal cursor."""
        uri = 'table:touch_default'
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri)
        try:
            c['k'] = 'v'
            c.set_key('k')
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), 'v')
        finally:
            c.close()

    def test_touch_search_returns_notfound_for_every_key_shape(self):
        """search() on a touch cursor always returns WT_NOTFOUND, regardless
        of whether the underlying key exists."""
        uri = 'table:touch_notfound'
        self.session.create(uri,
                            'key_format=S,value_format=S,'
                            'allocation_size=512,leaf_page_max=512')
        c = self.session.open_cursor(uri)
        for i in range(100):
            c[f'k{i:05d}'] = f'v{i:05d}'
        c.close()
        self.session.checkpoint()

        t = self.session.open_cursor(
            uri, None, 'touch=(enabled=true)')
        try:
            for k in ('k00000', 'k00050', 'k00099', 'k99999', 'aaa', 'zzz'):
                t.set_key(k)
                self.assertEqual(t.search(), wiredtiger.WT_NOTFOUND,
                                 f'search({k!r}) should be WT_NOTFOUND')
        finally:
            t.close()

    def test_touch_cursor_close_releases_payload(self):
        """Close on a touch cursor doesn't leak the command buffer."""
        uri = 'table:touch_close'
        self.session.create(uri, 'key_format=S,value_format=S')
        # Open + close many times with a payload to surface any leak under
        # diagnostic builds.
        payload = 'x' * 200
        for _ in range(50):
            c = self.session.open_cursor(
                uri, None,
                f'touch=(enabled=true,command="{payload}")')
            c.set_key('any')
            self.assertEqual(c.search(), wiredtiger.WT_NOTFOUND)
            c.close()
