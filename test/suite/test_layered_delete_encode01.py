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

import re, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class

# test_layered_delete_encode01.py
#   A layered cursor disambiguates an application value that happens to start with the ingest
#   tombstone (two leading 0x14 bytes) by appending one extra 0x14 byte before writing. The
#   encode/decode pair (__clayered_deleted_encode / __clayered_deleted_decode in cur_layered.c)
#   is applied in the cursor layer for every write, regardless of whether the write lands in the
#   ingest or the stable table. On a leader, a layered write goes straight to the stable table,
#   so the encoded value is reconciled and persisted with the trailing byte attached.
#
#   This test proves that. It writes a value starting with the tombstone bytes through the layered
#   cursor, checkpoints, then reads the stable constituent back through the two paths the engine
#   itself uses (see __clayered_open_stable in cur_layered.c):
#     - the leader path: a plain cursor on the stable URI (the live stable btree);
#     - the follower path: a cursor on "<stable_uri>/<checkpoint_name>", which reads the persisted
#       checkpoint from disaggregated storage.
#   Both bypass the layered decode and so expose the raw bytes, including the trailing 0x14.
@disagg_test_class
class test_layered_delete_encode01(DisaggConfigMixin, wttest.WiredTigerTestCase):

    test_name = __qualname__
    uri_base = test_name
    uri = 'layered:' + uri_base
    stable_uri = 'file:' + uri_base + '.wt_stable'

    conn_config = 'disaggregated=(role="leader"),disaggregated=(lose_all_my_data=true)'

    # The ingest tombstone is two 0x14 (DC4) bytes; see __wt_tombstone in cursor_inline.h.
    tombstone = b'\x14\x14'
    encode_byte = b'\x14'

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Return the name of the latest checkpoint on the stable file, mirroring what the follower
    # stable-open path derives via __wt_meta_checkpoint_last_name.
    def last_stable_checkpoint_name(self):
        meta = self.session.open_cursor('metadata:')
        meta.set_key(self.stable_uri)
        self.assertEqual(meta.search(), 0)
        value = meta.get_value()
        meta.close()
        names = re.findall(r'(WiredTigerCheckpoint\.\d+)=', value)
        self.assertTrue(names, f'no checkpoint found in stable metadata: {value}')
        return names[-1]

    def test_stable_persists_encoded_value(self):
        # A raw value format lets us store arbitrary bytes, including the tombstone prefix.
        self.session.create(self.uri, 'key_format=S,value_format=u')

        # A value that starts with the tombstone and is longer than it: this is the case that
        # gets encoded (a value equal to the tombstone, or shorter, is left untouched).
        key = 'k1'
        original = self.tombstone + b'payload'

        cursor = self.session.open_cursor(self.uri)
        cursor[key] = original
        cursor.close()

        # Persist the stable table.
        self.session.checkpoint()

        # The layered cursor decodes on read: the application sees its original value.
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(key)
        self.assertEqual(cursor.search(), 0)
        layered_value = cursor.get_value()
        cursor.close()
        self.assertEqual(layered_value, original,
            'layered cursor should return the original, decoded value')

        # Leader read path: a plain cursor on the stable btree, exactly as __clayered_open_stable
        # does for a leader. A file cursor applies no layered decode, so it exposes the stored byte.
        stable = self.session.open_cursor(self.stable_uri)
        stable.set_key(key)
        self.assertEqual(stable.search(), 0)
        live_value = stable.get_value()
        stable.close()

        # Follower read path: open "<stable_uri>/<checkpoint_name>", which reads the persisted
        # checkpoint from disaggregated storage rather than the in-memory btree.
        ckpt_name = self.last_stable_checkpoint_name()
        ckpt = self.session.open_cursor(f'{self.stable_uri}/{ckpt_name}')
        ckpt.set_key(key)
        self.assertEqual(ckpt.search(), 0)
        disk_value = ckpt.get_value()
        ckpt.close()

        self.pr(f'original         : {original.hex()}')
        self.pr(f'stable (live)    : {live_value.hex()}')
        self.pr(f'stable (on disk) : {disk_value.hex()}')

        # The encoded value (original + one trailing 0x14) is what the stable table holds, both in
        # the live btree and in the persisted checkpoint read back from disk.
        encoded = original + self.encode_byte
        self.assertEqual(live_value, encoded,
            'live stable btree should hold the encoded value')
        self.assertEqual(disk_value, encoded,
            'persisted stable checkpoint should hold the encoded value')
