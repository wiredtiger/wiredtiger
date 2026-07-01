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
#
# WT-17933: exercise the no-encoding read path against legacy on-disk data. The leader writes with
# debug_mode=(legacy_tombstone_encoding=true), which encodes tombstone-namespace values and stamps
# stable pages with the legacy (pre-no-encoding) version. The follower runs new code and must decode
# those values correctly when it reads the leader's checkpoint.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# Values in the tombstone namespace: each begins with the two tombstone bytes, so legacy code encodes
# them on disk; the new read path must restore each one exactly.
PAYLOADS = [
    b'\x14\x14a',
    b'\x14\x14\x14',
    b'\x14\x14abc',
    b'\x14\x14ends-with-enc\x14',
    b'\x14\x14' + b'z' * 300,
    b'ordinary-value',
]

@disagg_test_class
class test_layered_tombstone_legacy(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    uri = f'layered:{test_name}'
    conn_follow = None
    session_follow = None

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_follower(self):
        self.conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')

    def create_tables(self):
        cfg = 'key_format=i,value_format=u'
        self.session.create(self.uri, cfg)
        self.session_follow.create(self.uri, cfg)

    def legacy(self, on):
        self.conn.reconfigure(
            f'debug_mode=(legacy_tombstone_encoding={"true" if on else "false"})')

    def checkpoint_and_pickup(self, ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def evict_stable(self, key):
        # Force the stable constituent page out of cache, so the next access re-reads it from disk
        # and carries the on-disk page version (mirrors reading a legacy page after an upgrade).
        ec = self.session.open_cursor(
            'file:' + self.test_name + '.wt_stable', None, 'debug=(release_evict=true)')
        ec.set_key(key)
        ec.search()
        ec.reset()
        ec.close()

    def stable_raw(self, key):
        # Read the stable constituent directly, which bypasses the layered-cursor decode and exposes
        # the bytes actually stored on disk.
        c = self.session.open_cursor('file:' + self.test_name + '.wt_stable')
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        v = c.get_value()
        c.close()
        return v

    def follower_check(self, key, expected, read_ts=None):
        cfg = None if read_ts is None else f'read_timestamp={self.timestamp_str(read_ts)}'
        self.session_follow.begin_transaction(cfg)
        c = self.session_follow.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        self.assertEqual(c.get_value(), expected)
        c.close()
        self.session_follow.rollback_transaction()

    # Legacy-encoded values sitting on a v1 stable page must read back raw on a new-code follower.
    def test_legacy_data_page_decode(self):
        self.setup_follower()
        self.create_tables()

        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        for i, v in enumerate(PAYLOADS, start=1):
            c[i] = v
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')

        self.checkpoint_and_pickup(10)

        # Prove the debug knob actually encoded on disk: the stored constituent bytes carry the extra
        # tombstone byte for namespace values, and are untouched for an ordinary value. (Without this
        # the round-trip below would pass even if nothing had been encoded.)
        for i, v in enumerate(PAYLOADS, start=1):
            expected_raw = v + b'\x14' if (len(v) > 2 and v[:2] == b'\x14\x14') else v
            self.assertEqual(self.stable_raw(i), expected_raw)

        self.legacy(False)

        # The follower runs new code: it must decode the v1 values back to the originals. If the page
        # were mislabelled v2, decode would not fire and the encoded bytes would leak, failing here.
        for i, v in enumerate(PAYLOADS, start=1):
            self.follower_check(i, v)

    # An older legacy-encoded version resolved through the history store must also decode. The newer
    # version is written raw (new code) onto a v2 page; reading at the old timestamp pulls the v1 HS
    # value, whose decode must follow the HS page version, not the data page the cursor sits on.
    def test_legacy_history_store_decode(self):
        self.setup_follower()
        self.create_tables()

        key = 1
        old = b'\x14\x14old-value'
        new = b'\x14\x14new-value-distinct'

        # Old version, written and persisted as legacy (v1).
        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = old
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.checkpoint_and_pickup(10)

        # Drop the page so the next write re-reads the legacy (v1) image from disk.
        self.evict_stable(key)

        # New version, written raw (v2). The old version moves to the history store.
        self.legacy(False)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = new
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        self.checkpoint_and_pickup(20)

        self.follower_check(key, old, read_ts=15)
        self.follower_check(key, new, read_ts=25)

    # A modify whose base is a legacy-encoded value must apply to the decoded base, not the encoded
    # bytes. The leader reads the v1 base, decodes it, applies the modify, and re-encodes the result.
    def test_legacy_modify_base(self):
        self.setup_follower()
        self.create_tables()

        key = 1
        base = b'\x14\x14ABCDEFGH'

        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = base
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.checkpoint_and_pickup(10)
        self.evict_stable(key)

        # New code: replace two bytes at offset 4 ("CD" -> "xy") of the decoded base.
        self.legacy(False)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.modify([wiredtiger.Modify(b'xy', 4, 2)]), 0)
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        self.checkpoint_and_pickup(20)

        # Applied to the decoded base; if it had been applied to the encoded base a trailing
        # tombstone byte would survive into the result.
        self.follower_check(key, b'\x14\x14ABxyEFGH')

    # A legacy page with several namespace keys, where new code updates only one. The other keys are
    # carried through reconciliation untouched and must still read back correctly after the page is
    # converted to the no-encoding format.
    def test_legacy_multi_key_reconcile(self):
        self.setup_follower()
        self.create_tables()

        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        for i in range(1, 6):
            c[i] = b'\x14\x14key-' + bytes([0x30 + i])
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.checkpoint_and_pickup(10)
        self.evict_stable(1)

        # New code updates only key 1; keys 2..5 remain encoded (untouched) on the legacy page.
        self.legacy(False)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[1] = b'\x14\x14updated-one'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        self.checkpoint_and_pickup(20)

        self.follower_check(1, b'\x14\x14updated-one')
        for i in range(2, 6):
            self.follower_check(i, b'\x14\x14key-' + bytes([0x30 + i]))

    # A raw value written by new code that merely begins with the tombstone bytes must never be
    # decoded, even when it is pushed to the history store while its page is still in the legacy
    # format (during that page's converting reconcile). It is chain-sourced, not disk-sourced.
    def test_legacy_raw_chain_value_to_hs(self):
        self.setup_follower()
        self.create_tables()
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        # Establish a legacy (v1) page holding key 1.
        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[1] = b'\x14\x14anchor'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.checkpoint_and_pickup(10)
        self.evict_stable(1)

        # New code writes key 2 (same page) raw, then supersedes it. Both versions are in the update
        # chain at the next checkpoint, so the superseded ts=20 value is pushed to the history store
        # in the same reconcile that converts the page from v1.
        self.legacy(False)
        raw_chain = b'\x14\x14raw-chain-value'
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[2] = raw_chain
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')

        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[2] = b'\x14\x14superseded'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(30)}')
        self.checkpoint_and_pickup(30)

        # The ts=20 value was raw; reading it back from the history store must not strip a byte.
        self.follower_check(2, raw_chain, read_ts=25)

    # Delete a key with legacy-encoded history, then reinsert it with new code. The reinsert lands on
    # a page that converts to the no-encoding format on reconcile, so the new value is stored raw and
    # reads back raw; reading at an old timestamp still resolves the legacy history-store version.
    def test_legacy_delete_reinsert(self):
        self.setup_follower()
        self.create_tables()
        self.conn.set_timestamp(f'oldest_timestamp={self.timestamp_str(1)}')

        key = 1
        v1 = b'\x14\x14version-one'
        v3 = b'\x14\x14version-three'

        self.legacy(True)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = v1
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        self.checkpoint_and_pickup(10)
        self.evict_stable(key)

        # Second legacy version; the first moves to the (v1) history store.
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = b'\x14\x14version-two'
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')
        self.checkpoint_and_pickup(20)
        self.evict_stable(key)

        self.legacy(False)
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.remove(), 0)
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(30)}')
        self.checkpoint_and_pickup(30)

        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[key] = v3
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(40)}')
        self.checkpoint_and_pickup(40)

        self.follower_check(key, v1, read_ts=15)
        self.follower_check(key, v3, read_ts=45)
