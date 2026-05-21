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

# test_layered106.py
#   Regression tests for WT-17453 (assertion at src/support/modify.c:486
#   ("cbt->slot != UINT32_MAX") and SIGSEGV in __wt_row_leaf_value seen
#   during test.format with CONFIG.disagg disagg.multi=1).
#
#   Bug shape (before the fix):
#     An ingest-btree row K has on-disk value V1 and an in-memory chain
#         [ STANDARD(V2) -> MODIFY(D1) -> NULL ]
#     layered on top, with the MODIFY's reconstruction base being the
#     on-disk V1. Ingest GC eligibility advances past V1 while the
#     in-memory entries are still not globally visible. Eviction
#     reconciliation drops K from the rebuilt in-memory disk image (in
#     rec_visibility.c upd_select->upd is cleared via the
#     WT_REC_HAS_ON_DISK + !found_last_upd_to_keep + !first_pruned_update
#     path, then rec_row.c's GARBAGE_COLLECT path writes the tombstone
#     and skips the cell). The saved chain survives via supd_restore but
#     lands on the insert list of the rebuilt page (K no longer has a
#     row, cbt->ins != NULL, cbt->slot == UINT32_MAX). A reader whose
#     read_timestamp can see the MODIFY but not the newer STANDARD walks
#     modify->next == NULL with no on-page fallback and aborts the
#     process at the assertion.
#
#   Two symptoms, one bug:
#     - test_modify_survives_ingest_gc_of_base_value (cursor.next on a
#       fresh cursor): __cursor_row_next is entered with newpage=true,
#       which sets cbt->slot = UINT32_MAX (bt_curnext.c:300). The MODIFY
#       in the smallest insert list has no base in the chain and no
#       on-page cell to fall back to, so modify.c:486 trips the
#       diagnostic assertion "cbt->slot != UINT32_MAX".
#
#     - test_modify_reconstruction_via_search_hits_null_pointer
#       (cursor.search on the same key): __wt_row_search lands on a page
#       with page->entries == 0, so the binary-search base stays 0 and
#       row_srch.c:725-730 sets cbt->slot = 0 and steers the search into
#       the smallest insert list. The exact-match read goes through
#       __cursor_valid_insert -> __wt_txn_read_upd_list ->
#       __wt_modify_reconstruct_from_upd_list. The line-486 assertion
#       now sees cbt->slot == 0, which is != UINT32_MAX, so the
#       assertion passes. Execution continues to modify.c:488 and
#       __wt_value_return_buf evaluates &page->pg_row[cbt->slot] as
#       &(NULL)[0] == NULL on a zero-row page, then __wt_row_leaf_value
#       dereferences it and SIGSEGVs.
#
#   With WT-17453 applied, __rec_upd_select_inmem keeps upd_select->upd
#   non-NULL when the bottom non-aborted update of the saved chain is a
#   MODIFY. That preserves the on-page V1 cell in the rebuilt page, so
#   the orphan-MODIFY state never arises and both read paths return the
#   reconstructed value cleanly.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase):
    base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = base_config + 'disaggregated=(role="leader")'
    conn_config_follower = base_config + 'disaggregated=(role="follower")'

    uri = 'layered:test_layered106'
    ingest_uri = 'file:test_layered106.wt_ingest'
    create_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages('test_layered106', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_follow = None
    session_follow = None

    def create_follower(self):
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create,' + self.conn_config_follower)
        self.session_follow = self.conn_follow.open_session()

    def force_evict(self, conn, uri, key):
        """Force-evict the leaf page that holds `key`."""
        session_evict = conn.open_session('debug=(release_evict_page)')
        evict_cursor = session_evict.open_cursor(uri)
        evict_cursor.set_key(key)
        evict_cursor.search()
        evict_cursor.reset()
        evict_cursor.close()
        session_evict.close()

    def setup_orphan_modify_precondition(self):
        """
        Shared timeline through force eviction on the follower's ingest
        btree. Leaves the follower ready for a read at read_timestamp=25
        that sees MODIFY(D1) but not STANDARD(V2).

        Returns the value expected when D1 is reconstructed against V1.
        """
        self.create_follower()

        self.session.create(self.uri, self.create_config)
        self.session_follow.create(self.uri, self.create_config)

        # __wt_btcur_modify auto-promotes a MODIFY to a STANDARD when the
        # post-modify value fits in 64 bytes or fewer (see
        # __cursor_chain_needs_full_upd in bt_cursor.c). To put an actual
        # MODIFY on the chain we need values larger than 64 bytes.
        v1 = 'value1' + ('.' * 100)
        v2 = 'value3' + ('.' * 100)

        # Step 1: insert K=1 at ts=10 on both sides.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[1] = v1
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(10)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf[1] = v1
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(10)}')
        cf.close()

        # Step 2: bake V1 onto the follower's ingest disk image.
        self.force_evict(self.conn_follow, self.uri, 1)

        # Step 2b: lock stable_timestamp at 11 before the higher-timestamp
        # writes. stable_timestamp can only advance and is bounded by the
        # commit timestamps of already-committed transactions, so this
        # has to happen before the ts=20 / ts=30 writes.
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')
        self.conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')

        # Step 3: modify K at ts=20 on both sides. v1[6] == '.', so the
        # post-modify value is 'value1' + 'X' + ('.' * 99). Because v1 is
        # > 64 bytes, this MODIFY is preserved as a MODIFY update on the
        # ingest chain (not collapsed to a STANDARD).
        mods = [wiredtiger.Modify('X', 6, 1)]

        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c.set_key(1)
        self.assertEqual(c.modify(mods), 0)
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf.set_key(1)
        self.assertEqual(cf.modify(mods), 0)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        cf.close()

        # Step 4: full update at ts=30 on both sides. Ingest chain becomes
        # [STANDARD(V2) -> MODIFY(D1) -> NULL].
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[1] = v2
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf[1] = v2
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        cf.close()

        # Step 5: checkpoint while stable is still 11. With last_ckpt == 1
        # and no other session using the layered dhandle, the follower's
        # prune update sets prune_timestamp = checkpoint_timestamp = 11.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Step 6: oldest_timestamp = 11. V1@10 <= 11 -> visible_all;
        # D1@20 and V2@30 > 11 -> not visible_all.
        self.conn_follow.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(11)}')

        # Step 7: force eviction. Reconciliation walks the ingest chain
        # for K. Without the fix it clears upd_select->upd (the on-page
        # value is the only globally visible base, the in-memory entries
        # are not yet visible_all), then rec_row.c's GARBAGE_COLLECT path
        # drops K from the rebuilt in-memory disk image, leaving the
        # chain stranded on the insert list of the new page. With the
        # fix the on-page value is preserved because a MODIFY in the
        # chain depends on it.
        self.force_evict(self.conn_follow, self.uri, 1)

        return 'value1' + 'X' + ('.' * 99)

    def teardown_follower_and_checkpoint_leader(self):
        # Drop the follower before tearDown to avoid a verifyLayered
        # contention on a follower that still has the ingest btree open.
        self.session_follow.close()
        self.conn_follow.close()
        self.session_follow = None
        self.conn_follow = None

        # The stable timestamp was pinned at 11 to control prune_timestamp.
        # That leaves the leader with uncheckpointed dirty data (the ts=20
        # and ts=30 writes), which verifyLayered() in tearDown can't handle.
        # Advance stable and take a final checkpoint to flush everything.
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(100)}')
        self.session.checkpoint()

    def test_modify_survives_ingest_gc_of_base_value(self):
        """
        Read at read_timestamp=25 via cursor.next(). Before the fix this
        aborts at modify.c:486 with 'cbt->slot != (4294967295U)'. After
        the fix the MODIFY is reconstructed against V1.
        """
        expected = self.setup_orphan_modify_precondition()

        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(25)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)
        # cursor.next walks the insert list; this is the call that aborts
        # the process with the bug present.
        self.assertEqual(cf.next(), 0)
        self.assertEqual(cf.get_key(), 1)
        self.assertEqual(cf.get_value(), expected)
        # No more entries — the ingest btree only has K=1.
        self.assertEqual(cf.next(), wiredtiger.WT_NOTFOUND)
        cf.close()
        self.session_follow.rollback_transaction()

        self.teardown_follower_and_checkpoint_leader()

    def test_modify_reconstruction_via_search_hits_null_pointer(self):
        """
        Same precondition as test_modify_survives_ingest_gc_of_base_value,
        but exercises cursor.search() instead of next(). row_srch.c sets
        cbt->slot = 0 on the empty page, which bypasses the modify.c:486
        assertion; without the fix __wt_value_return_buf dereferences
        page->pg_row[0] (NULL) and SIGSEGVs.
        """
        expected = self.setup_orphan_modify_precondition()

        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(25)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)
        cf.set_key(1)
        # This is the call that crashes without the fix.
        self.assertEqual(cf.search(), 0)
        self.assertEqual(cf.get_key(), 1)
        self.assertEqual(cf.get_value(), expected)
        cf.close()
        self.session_follow.rollback_transaction()

        self.teardown_follower_and_checkpoint_leader()

    def setup_multikey_orphan_modify_precondition(self, num_keys, target_key):
        """
        Multi-key variant of setup_orphan_modify_precondition.

        Inserts K=1..num_keys at ts=10 so all keys share a single leaf
        page on the follower's ingest btree, then puts the
        [STANDARD -> MODIFY -> NULL] chain on only target_key. After
        force-eviction, without the WT-17453 fix the reconciler hits
        the WT_REC_HAS_ON_DISK && !found_last_upd_to_keep &&
        !first_pruned_update branch for target_key only, clears
        upd_select->upd, and the GARBAGE_COLLECT path in rec_row.c
        drops target_key's row from the rebuilt pg_row. Neighbor rows
        K != target_key have no in-memory chain, so the same branch
        does not fire for them and their cells survive in pg_row.

        With target_key absent from pg_row but its chain restored on
        the INSERT list via supd_restore, cursor.search(target_key)
        on the rebuilt page lands cbt->slot on an adjacent neighbor in
        pg_row (binary search returns the nearest slot, not
        UINT32_MAX). __wt_modify_reconstruct_from_upd_list reads
        &page->pg_row[cbt->slot] for the on-page base -- yielding the
        neighbor's value with target_key's MODIFY delta applied. This
        is the SERVER-121340 cross-key signature ("intact valid
        neighboring document at the wrong rid").

        Returns the expected value when target_key's MODIFY is
        correctly reconstructed against target_key's own on-disk value.
        """
        self.create_follower()

        # leaf_page_max=64KB keeps all num_keys on one page; the
        # SERVER-121340 signature requires the target key and its
        # neighbors to share the same pg_row.
        multikey_create_config = self.create_config + ',leaf_page_max=64KB'
        self.session.create(self.uri, multikey_create_config)
        self.session_follow.create(self.uri, multikey_create_config)

        def initial_value(k):
            # Owner tag at offset 0..4 ('V_K' + 2-digit key) encodes
            # the key identity so cross-key contamination is visible
            # from get_value(). Length > 64 bytes prevents
            # __cursor_chain_needs_full_upd from auto-promoting the
            # MODIFY to a STANDARD (bt_cursor.c). Byte at offset 6
            # is '.' for every key, so a single Modify('X', 6, 1)
            # produces a deterministic post-image regardless of which
            # neighbor's value the bug applies it to.
            return f'V_K{k:02d}_' + ('.' * 100)

        def neighbor_v11(k):
            # Intermediate value at ts=11 (prunable, will trigger the
            # prune branch in __rec_upd_select_inmem to set
            # found_last_upd_to_keep=true).
            return f'V_K{k:02d}A' + ('.' * 100)

        def neighbor_v20(k):
            # Latest value at ts=20 (non-prunable, will be selected
            # as upd_select->upd and written to the new disk image).
            # The owner tag at offset 0..4 ('V_K' + 2-digit key)
            # exposes cross-key contamination -- a contaminated read
            # of target_key would return V_K04U... or V_K06U...,
            # NOT V_K05...
            return f'V_K{k:02d}U' + ('.' * 100)

        # Step 1a: target_key on BOTH leader and follower at ts=10.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[target_key] = initial_value(target_key)
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(10)}')
        c.close()

        # Step 1b: ALL keys on the follower at ts=10 (target_key
        # second-write is idempotent for the buggy branch -- the
        # chain bottom is what matters). Neighbors are NOT written
        # on the leader -- they have no stable counterpart.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        for k in range(1, num_keys + 1):
            cf[k] = initial_value(k)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(10)}')
        cf.close()

        # Step 2: bake all V_K??@10 onto the follower's ingest disk
        # image.
        self.force_evict(self.conn_follow, self.uri, 1)

        # Step 2b: Two-entry chain on each neighbor.
        # NOTE: stable_timestamp must NOT be set yet -- WT requires
        # commit_ts > stable_ts, so committing at ts=11 here has to
        # happen before stable advances to 11 below.
        #
        # The disagg follower pins visible_all at the layered
        # last_checkpoint_timestamp (txn_inline.h:1008-1016): no upd
        # is ever visible_all past the picked-up checkpoint ts.
        # And prune_ts == checkpoint_ts (conn_layered_ingest.c:1040).
        # So no chain entry can simultaneously be visible_all AND
        # non-prunable on a follower -- the (prune_ts, pinned_ts]
        # gap is closed by design.
        #
        # To keep neighbor cells in pg_row without the buggy branch
        # firing, we need first_pruned_update != NULL in the chain
        # AND upd_select->upd already set when the prune branch
        # triggers. rec_visibility.c:1200 sets
        # found_last_upd_to_keep = (upd_select->upd != NULL) at the
        # moment of the prune break -- so the recipe is:
        #
        #   chain head: STANDARD@20 (non-prunable; sets upd_select->upd)
        #   chain tail: STANDARD@11 (prunable, triggers break; flips
        #                            found_last_upd_to_keep to true
        #                            because upd_select->upd is now
        #                            non-NULL)
        #
        # We build that chain by writing each neighbor TWICE without
        # an intervening force_evict, so both entries stay in the
        # in-memory chain. target_key is left at chain=[STANDARD@10]
        # on disk; its modify/full at ts=20/30 below produces a chain
        # whose oldest entry is non-prunable AND has no second
        # prunable entry below it -- so target hits the buggy branch
        # and its cell gets dropped.
        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        for k in range(1, num_keys + 1):
            if k != target_key:
                cf[k] = neighbor_v11(k)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(11)}')
        cf.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        for k in range(1, num_keys + 1):
            if k != target_key:
                cf[k] = neighbor_v20(k)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        cf.close()

        # Step 2c: pin stable_timestamp at 11 (after the neighbor
        # chain is built; subsequent commits must be > 11).
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')
        self.conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')

        # Step 3: modify target_key at ts=20 on both sides.
        mods = [wiredtiger.Modify('X', 6, 1)]

        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c.set_key(target_key)
        self.assertEqual(c.modify(mods), 0)
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf.set_key(target_key)
        self.assertEqual(cf.modify(mods), 0)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        cf.close()

        # Step 4: full update target_key at ts=30. Follower's ingest
        # chain for target_key is now [STANDARD(v2) -> MODIFY(D) -> NULL].
        v2 = f'V_K{target_key:02d}_NEW' + ('.' * 100)

        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[target_key] = v2
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf[target_key] = v2
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        cf.close()

        # Step 5: leader checkpoint at stable=11 (captures target@10
        # only) and follower pickup. This sets the follower's
        # last_checkpoint_timestamp (and therefore both prune_ts and
        # pinned_ts for visible_all) to 11.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Step 5b: advance oldest_timestamp to 11 on the follower
        # (matches the existing single-key test).
        self.conn_follow.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(11)}')

        # Step 6: force-evict the ingest page. Reconcile decisions:
        #   - target_key: chain [STANDARD@30, MODIFY@20]; on-disk @10.
        #     Walk: STANDARD@30 (not prunable; not visible_all),
        #     MODIFY@20 (not prunable; not visible_all). Loop ends
        #     with found_last_upd_to_keep=false, first_pruned_update=NULL.
        #     Buggy branch fires (rec_visibility.c:1257), clears
        #     upd_select->upd. rec_row.c: upd==NULL,
        #     eligible_for_gc(@10) with prune_ts=11 -> drop cell;
        #     supd_restore lands the orphan MODIFY chain on the
        #     INSERT list of the new page.
        #   - neighbors: chain [STANDARD@20, STANDARD@11]; on-disk @10.
        #     Walk: STANDARD@20 (not prunable; not visible_all)
        #     -> upd_select->upd = STANDARD@20.
        #     STANDARD@11: prune check 11 <= prune_ts=11 TRUE,
        #     first_pruned_update = STANDARD@11,
        #     found_last_upd_to_keep = (upd_select->upd != NULL) = TRUE,
        #     break.
        #     Buggy branch DOES NOT fire (found_last_upd_to_keep=true).
        #     rec_row.c: upd = STANDARD@20, write to new disk image.
        #     Cell preserved with the neighbor_v20 value.
        self.force_evict(self.conn_follow, self.uri, target_key)

        # Expected at read_ts=25 if the read correctly reconstructs
        # MODIFY@20 against target_key's OWN initial value:
        # initial_value(target_key) with byte 6 = 'X'.
        initial = initial_value(target_key)
        return initial[:6] + 'X' + initial[7:]

    def test_modify_multikey_no_cross_key_contamination(self):
        """
        Populated-page variant of the orphan-MODIFY bug -- the silent
        cross-key contamination signature from SERVER-121340.

        The other two tests reduce the page to entries == 0 and trip
        either the slot-bounds assertion (cursor.next) or a NULL
        pg_row deref (cursor.search). This test keeps neighbors on
        the page so cbt->slot is well within bounds and the slot
        assertion passes -- making the defect SILENT: the
        reconstruction reads the wrong key's value as the on-page
        base, applies target_key's MODIFY delta, and returns a value
        whose owner tag belongs to a neighbor.

        Before WT-17453: cf.get_value() returns 'V_K0[4|6]_X' + dots
        (a neighbor's tag with target_key's delta), failing the
        owner-tag assertion.
        With WT-17453: target_key's on-page cell is preserved,
        reconstruction reads target_key's own value, owner tag and
        full value match.
        """
        target_key = 5
        num_keys = 10
        expected = self.setup_multikey_orphan_modify_precondition(
            num_keys=num_keys, target_key=target_key)

        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(25)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)

        # Probe a few neighbors first to confirm the page is in the
        # multi-key state the test requires. If neighbors return
        # WT_NOTFOUND, the eviction reconcile collapsed the page to
        # entries==0 and the test is going to fail in the
        # crash/null-deref variant instead of demonstrating the
        # silent variant -- fail loudly with a diagnostic so the
        # setup can be corrected, rather than letting the assertion
        # on target_key crash the process.
        neighbors_found = []
        for k in (target_key - 1, target_key + 1,
                  target_key - 2, target_key + 2):
            if k < 1 or k > num_keys:
                continue
            cf.set_key(k)
            ret = cf.search()
            if ret == 0:
                neighbors_found.append((k, cf.get_value()[:8]))
            cf.reset()
        self.assertTrue(
            len(neighbors_found) >= 1,
            f'Multi-key precondition not established: no neighbor of '
            f'target_key={target_key} survives on the rebuilt ingest '
            f'page (page appears pruned to entries==0). Cannot '
            f'demonstrate the silent cross-key variant from this '
            f'state. Setup needs adjustment.')

        # Read target_key. With the bug present, cbt->slot is set by
        # row_srch to a slot in pg_row that belongs to a NEIGHBOR
        # (target_key's own row was dropped during reconcile),
        # __wt_modify_reconstruct_from_upd_list falls through to
        # __wt_value_return_buf which reads &page->pg_row[cbt->slot]
        # (a neighbor's on-page value), and applies target_key's
        # MODIFY delta on top -- returning a value whose owner tag
        # belongs to the neighbor (the SERVER-121340 _id-vs-rid
        # signature). No crash, no assertion: the page has rows so
        # cbt->slot is in bounds.
        cf.set_key(target_key)
        self.assertEqual(cf.search(), 0)
        self.assertEqual(cf.get_key(), target_key)
        actual = cf.get_value()

        # Owner-tag check: the SERVER-121340 _id-vs-rid signature.
        # If this assertion fires, the value belongs to a different
        # key; the diagnostic includes the owner tag we got vs. the
        # one we expected.
        owner_tag = f'V_K{target_key:02d}'
        self.assertTrue(
            actual.startswith(owner_tag),
            f'Cross-key contamination on target_key={target_key}: '
            f'returned value has owner tag {actual[:5]!r}, expected '
            f'{owner_tag!r}. First 16 bytes returned: {actual[:16]!r}. '
            f'Expected (first 16): {expected[:16]!r}. Surviving '
            f'neighbors probed: {neighbors_found}.')

        # Full-value check: catches modify-delta-applied-to-wrong-base
        # even in the (extremely unlikely) case the neighbor's owner
        # tag coincidentally matches target_key's.
        self.assertEqual(actual, expected)

        cf.close()
        self.session_follow.rollback_transaction()

        self.teardown_follower_and_checkpoint_leader()

    def setup_unstable_onpage_modify_precondition(self):
        """
        Variant where the on-disk cell is NOT GC-eligible (its start
        timestamp is strictly greater than prune_timestamp). With only
        the first attempt at the WT-17453 fix in place (keep
        upd_select->upd whenever the chain bottom is a MODIFY,
        regardless of whether the on-disk cell is GC-eligible),
        reconciliation writes the MODIFY-reconstructed value to the
        new disk image at the MODIFY's time window (ts=20) -- silently
        bumping the on-disk start_ts from 15 to 20 and erasing the
        ts=15..19 visibility window of V0. The refined guard checks
        onpage_gc_eligible: when the on-disk cell is not yet
        GC-eligible, clear upd_select->upd as the original code did
        so rec_row.c keeps the on-disk cell unchanged (the GC drop
        does not fire since the cell is not eligible).

        Timeline:
          ts=15: V0 inserted on follower (and leader).
          ts=15: follower force-evicts -> on-disk V0@15.
          stable_timestamp -> 11.
          ts=20: MODIFY(D1) on follower (and leader).
          ts=30: STANDARD(V2) on follower (and leader).
          leader checkpoint at stable=11 (does NOT capture V0@15
            because 15 > 11; the checkpoint exists to set the
            follower's last_checkpoint_timestamp = 11 and therefore
            prune_timestamp = 11).
          oldest_timestamp -> 11.
          follower force-evicts -> reconciliation walks the chain
            with on-disk V0@15 (start_ts=15 > prune_ts=11, NOT
            GC-eligible).
        """
        self.create_follower()
        self.session.create(self.uri, self.create_config)
        self.session_follow.create(self.uri, self.create_config)

        v0 = 'value0' + ('.' * 100)
        v2 = 'value3' + ('.' * 100)

        # Step 1: insert K=1 at ts=15 on both sides. 15 will be GREATER
        # than the prune_timestamp we configure below, so V0's on-disk
        # cell will not be GC-eligible during reconciliation.
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[1] = v0
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(15)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf[1] = v0
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(15)}')
        cf.close()

        # Step 2: bake V0@15 onto the follower's ingest disk image.
        self.force_evict(self.conn_follow, self.uri, 1)

        # Step 2b: pin stable_timestamp at 11. This is below the ts=15
        # commit (stable_ts can be set to any value that doesn't move
        # backwards) and must be in place before the ts=20 / ts=30
        # writes so the leader checkpoint below picks up
        # checkpoint_timestamp = 11.
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')
        self.conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(11)}')

        # Step 3: modify K at ts=20 on both sides. v0[6] == '.', so the
        # post-modify value is 'value0' + 'X' + ('.' * 99). v0 is > 64
        # bytes so the MODIFY is preserved (not collapsed).
        mods = [wiredtiger.Modify('X', 6, 1)]

        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c.set_key(1)
        self.assertEqual(c.modify(mods), 0)
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf.set_key(1)
        self.assertEqual(cf.modify(mods), 0)
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')
        cf.close()

        # Step 4: full update at ts=30 on both sides. Ingest chain
        # becomes [STANDARD(V2) -> MODIFY(D1) -> NULL].
        c = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        c[1] = v2
        self.session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        c.close()

        cf = self.session_follow.open_cursor(self.uri)
        self.session_follow.begin_transaction()
        cf[1] = v2
        self.session_follow.commit_transaction(
            f'commit_timestamp={self.timestamp_str(30)}')
        cf.close()

        # Step 5: leader checkpoint at stable=11. Captures nothing for
        # K=1 (V0@15 is above stable=11), but sets the follower's
        # last_checkpoint_timestamp = 11 on pickup.
        self.session.checkpoint()
        self.disagg_advance_checkpoint(self.conn_follow)

        # Step 6: oldest = 11.
        self.conn_follow.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(11)}')

        # Step 7: force eviction. Reconciliation walks the ingest chain
        # for K=1 with first_pruned_update=NULL and
        # found_last_upd_to_keep=false (nothing in the chain is
        # prunable or visible_all). vpack->tw.start_ts = 15,
        # prune_ts = 11, so the on-disk cell is NOT GC-eligible
        # (WT_REC_CAN_PRUNE_UPD returns false for the cell).
        self.force_evict(self.conn_follow, self.uri, 1)

        # Expected value at read_timestamp=20 (or any ts in [20, 29])
        # when MODIFY@20 is correctly reconstructed against V0@15.
        return v0, 'value0' + 'X' + ('.' * 99), v2

    def test_modify_with_unstable_onpage_value(self):
        """
        Variant where the on-disk cell is NOT GC-eligible during
        reconciliation. Without WT-17453's refinement
        (onpage_gc_eligible guard) but with the earlier "MODIFY chain
        bottom" guard, reconciliation writes the MODIFY-reconstructed
        value to the new disk image at the MODIFY's time window (ts=20)
        and supd_restore truncates the saved chain to drop the on-page
        upd and everything below it (only STANDARD@30 survives). The
        original V0 cell at ts=15 is silently overwritten: V0's
        visibility window [15, 19] is gone, and a read at
        read_timestamp=15 (which should return V0) returns
        WT_NOTFOUND.

        With the refined guard, when the on-disk cell is not yet
        GC-eligible, upd_select->upd is cleared and rec_row.c keeps
        the on-page cell unchanged (V0@15 with its original tw).
        supd_restore preserves the full chain
        [STANDARD@30, MODIFY@20]. Reads at every timestamp resolve
        correctly:
          ts=14: NOTFOUND (V0 not yet committed)
          ts=15-19: V0 (chain not visible; falls back to on-page V0@15)
          ts=20-29: V0+delta (MODIFY visible; reconstructs against
                              on-page V0@15)
          ts=30+: V2 (STANDARD visible on the chain)
        """
        v0, modified, v2 = self.setup_unstable_onpage_modify_precondition()

        # read_ts=15 -- V0's commit timestamp. The buggy first-fix
        # rewrites the on-disk start_ts from 15 to 20, erasing this
        # visibility window. With the refined guard the on-page cell
        # is untouched.
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(15)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)
        cf.set_key(1)
        self.assertEqual(
            cf.search(), 0,
            'read_ts=15 returned WT_NOTFOUND: on-disk V0@15 was '
            'overwritten during reconciliation. Reconcile wrote the '
            'MODIFY-reconstructed value at the MODIFY\'s ts=20, so '
            'V0\'s [15, 19] visibility window is gone.')
        self.assertEqual(cf.get_value(), v0)
        cf.close()
        self.session_follow.rollback_transaction()

        # read_ts=25 -- MODIFY visible, STANDARD not. Reconstructed
        # value should equal modified.
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(25)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)
        cf.set_key(1)
        self.assertEqual(cf.search(), 0)
        self.assertEqual(cf.get_value(), modified)
        cf.close()
        self.session_follow.rollback_transaction()

        # read_ts=30 -- STANDARD visible.
        self.session_follow.begin_transaction(
            f'read_timestamp={self.timestamp_str(30)}')
        cf = self.session_follow.open_cursor(self.ingest_uri)
        cf.set_key(1)
        self.assertEqual(cf.search(), 0)
        self.assertEqual(cf.get_value(), v2)
        cf.close()
        self.session_follow.rollback_transaction()

        self.teardown_follower_and_checkpoint_leader()
