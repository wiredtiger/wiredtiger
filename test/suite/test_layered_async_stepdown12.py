#!/usr/bin/env python3
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

# test_layered_async_stepdown12.py
#    Verify that mirrored values preserve the constituent-specific tombstone encoding while the
#    layered cursor returns the original application values.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_async_stepdown12(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    encodings = [
        ('escaped', dict(encoding='true')),
        ('unescaped', dict(encoding='false')),
    ]
    write_modes = [
        ('mirrored', dict(write_mirroring=True)),
        ('ingest_only', dict(write_mirroring=False)),
    ]
    scenarios = make_scenarios(disagg_storages, encodings, write_modes)

    uri = f'layered:{test_name}'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + \
            f'disaggregated=(stepdown_write_mirroring={str(self.write_mirroring).lower()},' \
            f'legacy_tombstone_encoding_break_glass={self.encoding},role="leader")'

    def setUp(self):
        super().setUp()
        self.ignoreStdoutPattern(
            'stable table value in the tombstone namespace')

    def test_mirrored_values_round_trip(self):
        marker = b'\x14\x14'
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=u')
        self.write_at(self.uri, {'base': b'base'}, 10)

        self.set_step_down_ts(20)
        self.write_at(
            self.uri, {
                'insert': marker,
                'updated': b'plain',
                'modified': b'ab',
                'removed': b'remove-me',
            }, 30)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('updated')
        cursor.set_value(marker + b'ab')
        self.assertEqual(cursor.update(), 0)
        self.session.commit_transaction('commit_timestamp=' +
                                        self.timestamp_str(31))

        self.session.begin_transaction()
        cursor.set_key('modified')
        self.assertEqual(cursor.modify([wiredtiger.Modify(marker, 0, 2)]), 0)
        self.session.commit_transaction('commit_timestamp=' +
                                        self.timestamp_str(32))
        cursor.close()
        self.remove_at(self.uri, ['removed'], 33)

        expected = {
            'base': b'base',
            'insert': marker,
            'updated': marker + b'ab',
            'modified': marker
        }
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)

        stable_expected = {
            key:
            value + b'\x14'
            if self.encoding == 'true' and value.startswith(marker) else value
            for key, value in expected.items()
        }
        ingest_expected = {
            key: value + b'\x14' if value.startswith(marker) else value
            for key, value in expected.items() if key != 'base'
        }
        ingest_expected['removed'] = marker
        if not self.stable_has_step_down_writes():
            stable_expected = {'base': b'base'}
        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 40), stable_expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40),
                         ingest_expected)
