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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_tombstone_value.py
#    A layered table marks deletions in the ingest table with a two-byte tombstone marker, so a value
#    that begins with that marker is encoded (an extra byte appended) in the ingest table to keep it
#    distinct from a real tombstone. The stable table uses real removes, so it must store such values
#    raw. Exercise the encode/decode paths (the coverage FIXME-WT-17838 leaves open) and confirm the
#    stable table never carries the ingest encoding.
@disagg_test_class
class test_layered_tombstone_value(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),'
    uri = 'layered:test_layered_tombstone_value'
    ingest_uri = 'file:test_layered_tombstone_value.wt_ingest'
    stable_uri = 'file:test_layered_tombstone_value.wt_stable'
    create_config = 'key_format=i,value_format=u'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    # A value in the encoded namespace: it begins with the two-byte tombstone marker but is longer,
    # so it is distinct from an actual tombstone. The marker is 0x14 0x14 (see __wt_tombstone).
    encoded_value = b'\x14\x14 a layered value'
    # The same value with the appended marker byte, i.e. how the ingest table stores it.
    ingest_encoded_value = b'\x14\x14 a layered value\x14'
    plain_value = b'an ordinary value'

    def open_follower(self):
        conn = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="follower")')
        return conn, conn.open_session('')

    def layered_value(self, session, key):
        c = session.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        v = c.get_value()
        c.close()
        return v

    # Collect every stored value from a constituent file. The keys are read positionally so the test
    # does not depend on the constituent's key format.
    def constituent_values(self, session, constituent_uri):
        c = session.open_cursor(constituent_uri)
        values = []
        while c.next() == 0:
            values.append(c.get_value())
        c.close()
        return values

    def test_leader_writes_raw_to_stable(self):
        # A leader writes straight to the stable table, so no encoding should be applied.
        self.session.create(self.uri, self.create_config)
        c = self.session.open_cursor(self.uri)
        c[1] = self.encoded_value
        c[2] = self.plain_value
        c.close()

        # Both values round-trip through the layered cursor.
        self.assertEqual(self.layered_value(self.session, 1), self.encoded_value)
        self.assertEqual(self.layered_value(self.session, 2), self.plain_value)

        # The stable constituent stored them raw, without an appended marker byte.
        stable_values = self.constituent_values(self.session, self.stable_uri)
        self.assertIn(self.encoded_value, stable_values)
        self.assertIn(self.plain_value, stable_values)
        self.assertNotIn(self.ingest_encoded_value, stable_values)

    def test_follower_encodes_in_ingest(self):
        # A follower writes to the ingest table, which encodes values in the tombstone namespace.
        self.session.create(self.uri, self.create_config)
        conn_follow, session_follow = self.open_follower()
        session_follow.create(self.uri, self.create_config)

        c = session_follow.open_cursor(self.uri)
        c[1] = self.encoded_value
        c[2] = self.plain_value
        c.close()

        # Both values round-trip through the layered cursor (the ingest value is decoded on read).
        self.assertEqual(self.layered_value(session_follow, 1), self.encoded_value)
        self.assertEqual(self.layered_value(session_follow, 2), self.plain_value)

        # The ingest constituent stored the tombstone-namespace value with the appended marker byte;
        # the ordinary value is stored unchanged.
        ingest_values = self.constituent_values(session_follow, self.ingest_uri)
        self.assertIn(self.ingest_encoded_value, ingest_values)
        self.assertIn(self.plain_value, ingest_values)
        self.assertNotIn(self.encoded_value, ingest_values)

    def test_drain_decodes_into_stable(self):
        # When a follower steps up, its ingest table drains into the stable table; the drained value
        # must be stored raw, not with the ingest encoding.
        self.ignoreStdoutPattern('Picking up the same checkpoint again')

        self.session.create(self.uri, self.create_config)
        conn_follow, session_follow = self.open_follower()
        session_follow.create(self.uri, self.create_config)

        # Establish a leader checkpoint the follower can pick up on step up.
        self.session.checkpoint()

        # Follower writes land in its ingest table. The drain only moves updates with a durable
        # timestamp, so commit at one.
        session_follow.begin_transaction()
        c = session_follow.open_cursor(self.uri)
        c[1] = self.encoded_value
        c[2] = self.plain_value
        c.close()
        session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Step the follower up to leader: this drains its ingest table into stable.
        self.disagg_switch_follower_and_leader(conn_follow, self.conn)

        # The values still round-trip, now served from the stable table.
        self.assertEqual(self.layered_value(session_follow, 1), self.encoded_value)
        self.assertEqual(self.layered_value(session_follow, 2), self.plain_value)

        # The drain stripped the ingest encoding: the stable constituent holds the raw values.
        stable_values = self.constituent_values(session_follow, self.stable_uri)
        self.assertIn(self.encoded_value, stable_values)
        self.assertIn(self.plain_value, stable_values)
        self.assertNotIn(self.ingest_encoded_value, stable_values)
