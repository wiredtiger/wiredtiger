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

# test_layered_modify02.py
#
# Test modify operations on layered tables in follower mode, focusing on
# the cross-table reconstruct path in __wt_modify_reconstruct_from_upd_list
# where no WT_UPDATE_STANDARD exists in the ingest update chain and the base
# value must be fetched from the stable table. Three scenarios are covered:
#
# 1. First modify for a key in follower mode (base only in stable).
# 2. Chained modifies where the base moves into the ingest table.
# 3. Modifies applied after an ingest drain, which puts the base back in stable.
#
# In HAVE_DIAGNOSTIC builds the added assertion fires if a WT_UPDATE_STANDARD
# with an aborted txnid is found in the ingest chain, distinguishing that bug
# from the expected cross-table fallback.

import random
import wiredtiger, wttest

from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from modify_utils import create_mods, create_value
from wtscenario import make_scenarios


@disagg_test_class
class test_layered_modify02(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = "disaggregated=(page_log=palite),"
    disagg_storages = gen_disagg_storages("test_layered_modify02", disagg_only=True)
    uri = "layered:test_layered_modify02"

    # Keep the key count small so the test runs quickly while still covering
    # multiple pages of the ingest btree.
    nkeys = 50

    valuefmt = [
        ("item", dict(valuefmt="u")),
        ("string", dict(valuefmt="S")),
    ]
    scenarios = make_scenarios(disagg_storages, valuefmt)

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    # ------------------------------------------------------------------ helpers

    def _write_base_values(self, r):
        """Write nkeys records as the leader, checkpoint, and return the values."""
        self.session.create(self.uri, "key_format=i,value_format=" + self.valuefmt)
        c = self.session.open_cursor(self.uri)
        values = []
        for k in range(self.nkeys):
            v = create_value(r, 500, 25, self.valuefmt)
            c[k] = v
            values.append(v)
        c.close()
        self.session.checkpoint()
        return values

    def _become_follower(self):
        """Reopen as follower, picking up the latest leader checkpoint."""
        # Step down before closing to prevent a shutdown checkpoint on the current
        # (leader) connection. A shutdown checkpoint taken after a step-up + drain
        # cycle can fail and roll back its transaction, triggering the
        # "metadata updates should never be rolled back" assertion in txn.c.
        # This mirrors the pattern used by reopen_disagg_conn() in helper_disagg.py.
        self.conn.reconfigure('disaggregated=(role="follower")')
        meta = self.disagg_get_complete_checkpoint_meta()
        self.reopen_conn(
            config=self.conn_base_config
            + f'disaggregated=(role="follower",checkpoint_meta="{meta}")',
        )

    def _apply_modify(self, cursor, k, current_value, r, ts):
        """Apply a single random modify to key k and return the resulting value."""
        (_, mods, new_value) = create_mods(
            r, 500, 25, 3, 64, self.valuefmt, current_value
        )
        self.assertIsNotNone(mods)
        self.session.begin_transaction()
        cursor.set_key(k)
        self.assertEqual(cursor.modify(mods), 0)
        self.session.commit_transaction(f"commit_timestamp={self.timestamp_str(ts)}")
        return new_value

    def _verify_all(self, cursor, values):
        """Assert that every key in [0, nkeys) reads back the expected value."""
        for k in range(self.nkeys):
            self.assertEqual(cursor[k], values[k])

    # ------------------------------------------------------------------ tests

    def test_modify_base_in_stable(self):
        """
        Apply the first modify to each key in follower mode.

        When the layered cursor searches for the key, the base value is only in
        the stable constituent (not in the ingest btree). The follower cursor
        path in __clayered_modify_follower detects this (current_cursor != ingest),
        constructs the full post-modify value, and writes a WT_UPDATE_STANDARD to
        the ingest btree via ingest->update(). No call to ingest->modify() occurs
        here, so __wt_modify_reconstruct_from_upd_list is not reached with
        upd == NULL. The test verifies the resulting value is correct.
        """
        r = random.Random(1)
        values = self._write_base_values(r)
        self._become_follower()

        c = self.session.open_cursor(self.uri)
        for k in range(self.nkeys):
            values[k] = self._apply_modify(c, k, values[k], r, k + 1)

        self._verify_all(c, values)
        c.close()

    def test_chained_modifies_on_follower(self):
        """
        Apply three rounds of modifies to each key in follower mode.

        Round 1: base is in stable -> __clayered_modify_follower writes
                 WT_UPDATE_STANDARD to ingest (ingest->update()).
        Round 2: base is now in ingest -> __clayered_modify_follower calls
                 ingest->modify() directly, which invokes
                 __wt_modify_reconstruct_from_upd_list. The WT_UPDATE_STANDARD
                 written in round 1 is found in the ingest update chain and used
                 as the base for reconstruction. The diagnostic assertion added
                 in that function confirms that no aborted WT_UPDATE_STANDARD
                 was silently skipped.
        Round 3: same as round 2, now reconstructing from a chain of two
                 WT_UPDATE_MODIFY entries backed by the ingest WT_UPDATE_STANDARD.

        The test verifies that all values are correct after all three rounds.
        """
        r = random.Random(2)
        values = self._write_base_values(r)
        self._become_follower()

        c = self.session.open_cursor(self.uri)
        ts = 1
        for _round in range(3):
            for k in range(self.nkeys):
                values[k] = self._apply_modify(c, k, values[k], r, ts)
                ts += 1

        self._verify_all(c, values)
        c.close()

    def test_modify_after_ingest_drain(self):
        """
        Apply modifies in follower mode, drain the ingest constituent by stepping
        up as leader, then step back down and apply further modifies.

        Steps:
        1. Leader writes base values and checkpoints.
        2. Node reopens as follower; two rounds of modifies are applied, leaving
           WT_UPDATE_STANDARD + WT_UPDATE_MODIFY chains in the ingest btree.
        3. Node steps up as leader, triggering the ingest drain: all ingest entries
           are moved to the stable constituent and the ingest btree is cleared.
           A checkpoint publishes this stable state.
        4. Node reopens as follower and picks up the post-drain checkpoint. The
           ingest btree is now empty; the drained (modified) values are in stable.
        5. A third round of modifies is applied. The search again finds each key
           only in stable, so __clayered_modify_follower takes the fallback path
           (ingest->update), exercising the cross-constituent reconstruct a second
           time without triggering the diagnostic assertion.

        The test verifies that the final value after all three rounds of modifies
        is correct.
        """
        # The drain + checkpoint cycle may not advance the page_log LSN, causing
        # WiredTiger to emit "Picking up the same checkpoint again" when the follower
        # reconnects. Extend the existing WT_VERB_RTS ignore pattern to cover it.
        self.ignoreStdoutPattern(r"WT_VERB_RTS|Picking up the same checkpoint again")

        r = random.Random(3)
        values = self._write_base_values(r)
        self._become_follower()

        # Rounds 1 and 2: build the ingest update chain.
        c = self.session.open_cursor(self.uri)
        ts = 1
        for _round in range(2):
            for k in range(self.nkeys):
                values[k] = self._apply_modify(c, k, values[k], r, ts)
                ts += 1
        c.close()

        # Step up as leader to trigger ingest drain, then checkpoint.
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.conn.set_timestamp(f"stable_timestamp={self.timestamp_str(ts)}")
        self.session.checkpoint()

        # Reopen as follower, picking up the post-drain checkpoint.
        self._become_follower()

        # Round 3: base values are back in stable after the drain.
        c = self.session.open_cursor(self.uri)
        for k in range(self.nkeys):
            values[k] = self._apply_modify(c, k, values[k], r, ts + k + 1)

        self._verify_all(c, values)
        c.close()
