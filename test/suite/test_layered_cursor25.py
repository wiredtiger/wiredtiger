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
# WT-17933: layered values are no longer tombstone-encoded on write; a value that
# begins with the reserved tombstone bytes is stored raw on the new page version,
# and read back via the page-version-gated decode path. The two-byte tombstone
# value itself is now a restricted value.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

TOMBSTONE = b'\x14\x14'

@disagg_test_class
class test_layered_cursor25(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',create,statistics=(all),'
    uri = f'layered:{test_name}'
    conn_follow = None
    session_follow = None

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Values that exercise the encoded namespace: each begins with the two tombstone
    # bytes, so the legacy code would have appended a trailing byte before persisting.
    # A correct page-version gate must return every one of these unchanged.
    payloads = [
        b'\x14\x14',           # only valid as a stored value via the +1 byte below
        b'\x14\x14a',
        b'\x14\x14\x14',
        b'\x14\x14abc',
        b'\x14\x14abc\x14',    # also ends with the encoding byte
        b'\x14\x14' + b'z' * 200,
        b'plain-value',        # control: outside the encoded namespace
    ]

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

    def write(self, session, items):
        c = session.open_cursor(self.uri)
        for k, v in items:
            c[k] = v
        c.close()

    def check(self, session, items):
        # Point search path.
        c = session.open_cursor(self.uri)
        for k, v in items:
            c.set_key(k)
            self.assertEqual(c.search(), 0)
            # The value must come back byte-for-byte, with no stripped or extra byte.
            self.assertEqual(c.get_value(), v)
        c.close()

        # Forward iteration path (a separate decode site from search).
        expected = dict(items)
        c = session.open_cursor(self.uri)
        while c.next() == 0:
            self.assertEqual(c.get_value(), expected[c.get_key()])
        c.close()

    def advance_follower(self, ts):
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts)}')
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

    def test_no_encoding_round_trip(self):
        self.setup_follower()
        self.create_tables()

        # Keys 1..N map to the payloads; skip the bare two-byte tombstone here (tested below).
        items = list(enumerate((p for p in self.payloads if p != TOMBSTONE), start=1))

        # In-memory (ingest is followerless leader path, but values sit in cache as raw).
        self.session.begin_transaction()
        self.write(self.session, items)
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(10)}')
        self.check(self.session, items)

        # After checkpoint the stable page is rewritten with the no-encoding version, so
        # the gate must NOT decode these raw values.
        self.advance_follower(10)
        self.check(self.session, items)
        self.check(self.session_follow, items)

    def test_timestamped_history_round_trip(self):
        # A read at an old timestamp may resolve via the history store. The value's source page
        # version (HS page, not the data page the cursor sits on) drives the decode decision, so a
        # tombstone-namespace value must still round-trip raw. (The legacy-encoded HS case needs an
        # old-version data fixture and is not covered here.)
        self.setup_follower()
        self.create_tables()

        old = b'\x14\x14old'
        new = b'\x14\x14new-and-longer'

        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[1] = old
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')

        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        c[1] = new
        c.close()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(20)}')

        self.advance_follower(20)

        for session in [self.session, self.session_follow]:
            session.begin_transaction(f'read_timestamp={self.timestamp_str(15)}')
            c = session.open_cursor(self.uri)
            c.set_key(1)
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), old)
            c.close()
            session.rollback_transaction()

            session.begin_transaction(f'read_timestamp={self.timestamp_str(25)}')
            c = session.open_cursor(self.uri)
            c.set_key(1)
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), new)
            c.close()
            session.rollback_transaction()

    def test_tombstone_value_is_restricted(self):
        self.setup_follower()
        self.create_tables()

        c = self.session.open_cursor(self.uri)
        c.set_key(1)
        c.set_value(TOMBSTONE)
        msg = '/Invalid argument/'
        self.session.begin_transaction()
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, lambda: c.insert(), msg)
        self.session.rollback_transaction()
        c.close()
