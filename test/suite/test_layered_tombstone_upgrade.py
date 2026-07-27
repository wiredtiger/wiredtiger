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

# test_layered_tombstone_upgrade.py
#   The stable tombstone encoding mode is stamped into the checkpoint metadata. A node must not pick
#   up a checkpoint whose stable data was written in a different mode than the node is configured
#   for: mixing the escaped and unescaped formats silently corrupts reads. Picking up a matching
#   checkpoint succeeds; picking up a mismatched one is rejected so the operator wipes and retries.

import re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

class tombstone_upgrade_base(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    collide = b'\x14\x14ab'  # colliding value: stored differently by each mode
    control = b'plain'       # control value: stored identically by both modes

    def conn_config(self):
        return self.extensionsConfig() + ',create,' + self.conn_base_config + \
            f'disaggregated=(stable_tombstone_encoding={self.leader},role="leader")'

    def setUp(self):
        super().setUp()
        self.ignoreStdoutPattern('stable table value in the tombstone namespace')

    # The opposite of the leader's configured mode.
    def flipped(self):
        return 'false' if self.leader == 'true' else 'true'

    # Leader writes a colliding value and a control value, then checkpoints, stamping its mode into
    # the completed-checkpoint metadata.
    def leader_checkpoint(self, collide=None):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.session.create(self.uri, 'key_format=i,value_format=u')
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[1] = self.collide if collide is None else collide
        c[2] = self.control
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

    # Read both keys back through a layered cursor and confirm they decode to their original bytes,
    # proving the pickup landed the colliding value and left the control key intact.
    def assert_reads(self, conn, collide):
        s = conn.open_session()
        rc = s.open_cursor(self.uri)
        s.begin_transaction('read_timestamp=' + self.timestamp_str(20))
        rc.set_key(1)
        self.assertEqual(rc.search(), 0)
        self.assertEqual(rc.get_value(), collide)
        rc.set_key(2)
        self.assertEqual(rc.search(), 0)
        self.assertEqual(rc.get_value(), self.control)
        s.rollback_transaction()
        rc.close()
        s.close()

@disagg_test_class
class test_layered_tombstone_upgrade_mismatch(tombstone_upgrade_base):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    # Only the mismatch check needs the full leader/follower cross product; a matched pair picks up
    # cleanly, a mismatched one is rejected.
    modes = [
        ('on_on',   dict(leader='true',  follower='true',  reject=False)),
        ('off_off', dict(leader='false', follower='false', reject=False)),
        ('on_off',  dict(leader='true',  follower='false', reject=True)),
        ('off_on',  dict(leader='false', follower='true',  reject=True)),
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, modes)

    def test_mode_mismatch_rejected(self):
        self.leader_checkpoint()

        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config +
            f'disaggregated=(stable_tombstone_encoding={self.follower},role="follower")')

        if self.reject:
            with self.expectedStderrPattern(
                'must be wiped before changing the stable tombstone encoding'):
                with self.assertRaises(wiredtiger.WiredTigerError):
                    self.disagg_advance_checkpoint(conn_follow)
            # The rejected pickup must leave the connection usable, not wedged.
            s = conn_follow.open_session()
            s.close()
        else:
            self.disagg_advance_checkpoint(conn_follow)
            self.assert_reads(conn_follow, self.collide)
        conn_follow.close()

@disagg_test_class
class test_layered_tombstone_upgrade(tombstone_upgrade_base):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    # These checks read only the local node's own mode, so a single-node leader true/false set
    # covers every distinct behavior without the follower cross product.
    modes = [
        ('on',  dict(leader='true')),
        ('off', dict(leader='false')),
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, modes)

    def test_reconfigure_flip_rejected(self):
        # The mode is fixed when the connection opens. Reconfiguring it to the same value is a
        # harmless no-op, but flipping it on a running node is rejected: it would mix escaped and
        # unescaped values in one data set.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        self.session.create(self.uri, 'key_format=i,value_format=u')
        self.conn.reconfigure(f'disaggregated=(stable_tombstone_encoding={self.leader})')
        with self.expectedStderrPattern('cannot be changed by reconfigure'):
            with self.assertRaises(wiredtiger.WiredTigerError):
                self.conn.reconfigure(f'disaggregated=(stable_tombstone_encoding={self.flipped()})')

    def test_reconfigure_same_value_noop(self):
        # Reconfiguring the encoding to the value it already has must be a clean no-op: it neither
        # raises nor disturbs the running node.
        self.leader_checkpoint()
        self.conn.reconfigure(f'disaggregated=(stable_tombstone_encoding={self.leader})')
        # The connection is still usable and both values are unchanged.
        self.assert_reads(self.conn, self.collide)

    def test_leader_cold_startup_mismatch_rejected(self):
        # A node restarted with no local files reloads the shared checkpoint at startup. Coming up as
        # a leader in the flipped mode reloads that checkpoint while opening (a leader picks it up
        # itself rather than through a reconfigure) and rejects the mode mismatch, so wiredtiger_open
        # fails outright and leaves no connection behind.
        self.leader_checkpoint()
        with self.expectedStderrPattern(
            'must be wiped before changing the stable tombstone encoding'):
            with self.assertRaises(wiredtiger.WiredTigerError):
                self.restart_without_local_files(config=self.conn_base_config +
                    f'disaggregated=(stable_tombstone_encoding={self.flipped()},role="leader")')
        # The rejected open left no connection; reopen a follower in the matching mode and pick the
        # checkpoint back up so teardown can verify and close cleanly.
        self.open_conn(config=self.conn_base_config +
            f'disaggregated=(stable_tombstone_encoding={self.leader},role="follower")')
        self.disagg_advance_checkpoint(self.conn)

    def test_follower_cold_startup_mismatch_rejected(self):
        # The same startup pickup path, entered as a follower, is rejected on a mode mismatch too.
        self.leader_checkpoint()
        with self.expectedStderrPattern(
            'must be wiped before changing the stable tombstone encoding'):
            with self.assertRaises(wiredtiger.WiredTigerError):
                self.restart_without_local_files(config=self.conn_base_config +
                    f'disaggregated=(stable_tombstone_encoding={self.flipped()},role="follower")')

    def test_unstamped_checkpoint_reads_as_escaped(self):
        # A checkpoint written before the encoding stamp existed carries no stable_tombstone_encoding
        # field. The absent stamp is read as the legacy escaped format, so an escaped-mode reader
        # (the default) must pick it up without complaint. A value outside the tombstone namespace is
        # stored identically in both modes, so the pickup succeeds regardless of the leader's mode.
        self.leader_checkpoint(collide=b'hello')
        meta = self.disagg_get_complete_checkpoint_meta()
        self.assertIn('stable_tombstone_encoding=', meta)
        unstamped = re.sub(r',?stable_tombstone_encoding=(true|false)', '', meta)
        self.assertNotIn('stable_tombstone_encoding=', unstamped)

        # The reader omits the encoding config entirely, so it defaults to escaped.
        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config +
            'disaggregated=(role="follower")')
        conn_follow.reconfigure(f'disaggregated=(checkpoint_meta="{unstamped}")')
        self.assert_reads(conn_follow, b'hello')
        conn_follow.close()
