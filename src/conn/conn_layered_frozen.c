/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * A frozen tree is a leader's live stable btree that a step-down made read-only and kept as the
 * follower's stable constituent. It holds every commit up to the durable timestamp recorded at
 * demote (its covering bound), is read in place under ingest, and lives until either a checkpoint
 * that covers the bound supersedes it (pickup marks the handle outdated) or a step-up makes it this
 * node's live tree again. This file owns that lifecycle; other subsystems only ask questions of it.
 */

/*
 * __wti_layered_frozen_handle --
 *     Return whether a data handle is a live frozen tree: open, a disaggregated btree, frozen, and
 *     not yet superseded by a pickup. Optionally return its covering bound. An outdated handle is
 *     not live even while its frozen flag is still set: pickup has replaced it and step-up must
 *     open a fresh tree rather than reuse its pages.
 */
bool
__wti_layered_frozen_handle(WT_DATA_HANDLE *dhandle, wt_timestamp_t *max_tsp)
{
    WT_BTREE *btree;

    if (max_tsp != NULL)
        *max_tsp = WT_TS_NONE;

    if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
      __wt_atomic_load_bool_relaxed(&dhandle->outdated))
        return (false);

    btree = (WT_BTREE *)dhandle->handle;
    if (!F_ISSET(btree, WT_BTREE_DISAGGREGATED) ||
      !F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN))
        return (false);

    if (max_tsp != NULL)
        *max_tsp = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_max_ts);
    return (true);
}

/*
 * __wt_layered_frozen_live --
 *     Return whether the session's current btree is a live frozen tree. Eviction asks this to keep
 *     a frozen tree's dirty pages resident: they are commits from after the last checkpoint with no
 *     complete checkpoint to be read back from. Outdated wins: once pickup marks the handle,
 *     eviction discards those pages instead of pinning them.
 */
bool
__wt_layered_frozen_live(WT_SESSION_IMPL *session)
{
    return (__wti_layered_frozen_handle(S2BT(session)->dhandle, NULL));
}

/*
 * __wt_layered_frozen_max_ts_raise --
 *     Advance the timestamp a frozen tree must be covered up to. A prepared transaction resolved on
 *     a frozen tree gives its updates a commit timestamp assigned after the demote, above the bound
 *     recorded then; a checkpoint must reach that timestamp before it may supersede the tree. The
 *     bound only ever moves forward.
 */
void
__wt_layered_frozen_max_ts_raise(WT_BTREE *btree, wt_timestamp_t ts)
{
    wt_timestamp_t cur;

    if (ts == WT_TS_NONE || !F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN))
        return;

    cur = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_max_ts);
    while (cur < ts) {
        if (__wt_atomic_cas_uint64(&btree->disagg_frozen_max_ts, cur, ts))
            break;
        cur = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_max_ts);
    }
}

/*
 * __wt_layered_frozen_lookup --
 *     Look a stable URI up in the handle list and report whether it names a live frozen tree, and
 *     its covering bound. A URI with no handle, or a handle that is not open, reports not frozen:
 *     either way there is no frozen content to bind. The session's data handle is left as found.
 */
int
__wt_layered_frozen_lookup(
  WT_SESSION_IMPL *session, const char *uri, bool *frozenp, wt_timestamp_t *max_tsp)
{
    WT_DATA_HANDLE *saved;
    WT_DECL_RET;

    *frozenp = false;
    if (max_tsp != NULL)
        *max_tsp = WT_TS_NONE;

    saved = session->dhandle;
    session->dhandle = NULL;
    WT_WITH_HANDLE_LIST_READ_LOCK(
      session, ret = __wt_conn_dhandle_find(session, uri, NULL); if (ret == 0) {
          *frozenp = __wti_layered_frozen_handle(session->dhandle, max_tsp);
          WT_DHANDLE_CLEAR(session);
      } else if (ret == WT_NOTFOUND) ret = 0;);
    session->dhandle = saved;
    return (ret);
}

/*
 * __layered_frozen_page_lsn --
 *     Return the LSN of the newest page-log record a page's content was written to, or none for a
 *     page never written. After a multi-block reconciliation that did not evict the page, that is
 *     the last block written.
 */
static uint64_t
__layered_frozen_page_lsn(WT_PAGE *page)
{
    return (page->disagg_info == NULL ? WT_DISAGG_LSN_NONE : page->disagg_info->rec_lsn_max);
}

/*
 * __wt_layered_frozen_page_pinned --
 *     Return whether eviction must keep a clean page of a frozen tree resident: its image is above
 *     the checkpoint the tree froze at, so a successor's abandon may delete it from the page log
 *     and a read after eviction would fail. The pin holds while the tree stays frozen, outdated or
 *     not: readers still bound to an outdated frozen tree read it in place.
 */
bool
__wt_layered_frozen_page_pinned(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_BTREE *btree;
    uint64_t ckpt_lsn;

    btree = S2BT(session);
    if (!F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN))
        return (false);
    ckpt_lsn = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_ckpt_lsn);
    return (ckpt_lsn != WT_DISAGG_LSN_NONE && __layered_frozen_page_lsn(page) > ckpt_lsn);
}

/*
 * __layered_frozen_fault_in_skip --
 *     Tree walk callback. Skip an on-disk page at or below the checkpoint the tree froze at, and
 *     its subtree: writing a child dirties its parent, so a parent written at or below the
 *     checkpoint has no descendant written above it. Skip fast-truncated pages rather than
 *     instantiate them.
 */
static int
__layered_frozen_fault_in_skip(
  WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool visible_all, bool *skipp)
{
    WT_UNUSED(visible_all);

    *skipp = false;
    switch (WT_REF_GET_STATE(ref)) {
    case WT_REF_DELETED:
        *skipp = true;
        return (0);
    case WT_REF_DISK:
        break;
    default:
        return (0);
    }

    WT_ADDR_COPY addr;
    if (!__wt_ref_addr_copy(session, ref, &addr)) {
        *skipp = true;
        return (0);
    }
    const uint8_t *p = addr.addr;
    WT_BLOCK_DISAGG_ADDRESS_COOKIE cookie;
    WT_RET(__wt_block_disagg_addr_unpack(session, &p, addr.size, &cookie));
    *skipp = cookie.lsn <= *(uint64_t *)context;
    return (0);
}

/*
 * __layered_frozen_fault_in --
 *     Read every page of the session's frozen tree written above the checkpoint it froze at, so the
 *     eviction pin holds them before a successor can abandon them.
 */
static int
__layered_frozen_fault_in(WT_SESSION_IMPL *session, uint64_t ckpt_lsn)
{
    WT_DECL_RET;
    WT_REF *ref;
    uint64_t pinned;

    ref = NULL;
    pinned = 0;
    while ((ret = __wt_tree_walk_custom_skip(session, &ref, __layered_frozen_fault_in_skip,
              &ckpt_lsn, WT_READ_NO_EVICT | WT_READ_VISIBLE_ALL)) == 0 &&
      ref != NULL)
        if (__wt_layered_frozen_page_pinned(session, ref->page))
            ++pinned;
    WT_STAT_CONN_INCRV(session, disagg_frozen_pages_pinned, pinned);
    return (ret);
}

/*
 * __wti_layered_frozen_freeze --
 *     Freeze a live stable tree at demote so the follower keeps reading its resident pages, which
 *     are this node's committed state above its last checkpoint. The caller has already made the
 *     tree read-only and holds its eviction exclusive; the tree's pages above the last checkpoint
 *     are read in by __wti_layered_frozen_fault_in once it is released.
 */
void
__wti_layered_frozen_freeze(WT_SESSION_IMPL *session, WT_BTREE *btree, wt_timestamp_t max_ts)
{
    WT_ASSERT(session, F_ISSET_ATOMIC_32(btree, WT_BTREE_READONLY));
    WT_ASSERT(session, !F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN));

    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_max_ts, max_ts);
    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_ckpt_lsn,
      __wt_atomic_load_uint64_acquire(
        &S2C(session)->disaggregated_storage.last_checkpoint_meta_lsn));
    F_SET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN);
    WT_STAT_CONN_INCR(session, disagg_frozen_handles);
}

/*
 * __wti_layered_frozen_fault_in --
 *     Make a newly frozen tree's pages above its checkpoint resident. This must finish inside the
 *     step-down: once this node is a follower, a successor may promote and abandon them.
 */
int
__wti_layered_frozen_fault_in(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_DECL_RET;
    uint64_t ckpt_lsn;

    WT_ASSERT(session, F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN));

    ckpt_lsn = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_ckpt_lsn);
    if (ckpt_lsn == WT_DISAGG_LSN_NONE)
        return (0);
    WT_WITH_BTREE(session, btree, ret = __layered_frozen_fault_in(session, ckpt_lsn));
    return (ret);
}

/*
 * __layered_frozen_rewrite_page --
 *     Write a page's disk image again under its page ID and point the page's address at the new
 *     copy. A successor's abandon may have deleted the version the page was read from, and the page
 *     log verifies a delta's chain, a full image's backlink and a discard's backlink against the
 *     records it still holds; re-reconciling instead may split the page, which discards that
 *     version. The copy is a full image linking back to the previous full image when that is at or
 *     below the checkpoint and so survives an abandon, and otherwise to nothing, as a page's first
 *     write does. The caller holds the tree's flush lock.
 */
static int
__layered_frozen_rewrite_page(WT_SESSION_IMPL *session, WT_REF *ref, uint64_t ckpt_lsn)
{
    WT_ADDR_COPY copy;
    WT_DECL_ITEM(image);
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_BLOCK_META block_meta;
    WT_REF_STATE previous_state;
    size_t addr_size, compressed_size;
    uint64_t prev_full_lsn;
    bool locked;

    page = ref->page;
    locked = false;

    block_meta = page->disagg_info->block_meta;
    prev_full_lsn = block_meta.delta_count > 0 ? block_meta.base_lsn : block_meta.backlink_lsn;
    block_meta.backlink_lsn = prev_full_lsn <= ckpt_lsn ? prev_full_lsn : WT_DISAGG_LSN_NONE;
    block_meta.base_lsn = WT_DISAGG_LSN_NONE;
    block_meta.delta_count = 0;

    WT_ERR(__wt_scr_alloc(session, page->dsk->mem_size, &image));
    memcpy(image->mem, page->dsk, page->dsk->mem_size);
    image->size = page->dsk->mem_size;
    F_CLR((WT_PAGE_HEADER *)image->mem, WT_PAGE_COMPRESSED | WT_PAGE_ENCRYPTED);
    WT_ERR(__wt_blkcache_write(session, image, &block_meta, image->size, copy.addr, &addr_size,
      &compressed_size, false, false, false));
    copy.size = (uint8_t)addr_size;

    WT_REF_LOCK(session, ref, &previous_state);
    locked = true;
    WT_ERR(__wt_ref_addr_replace(session, ref, &copy));

    __wt_block_disagg_decrease_size(session, page->disagg_info->block_meta.cumulative_size);
    page->disagg_info->block_meta = block_meta;
    page->disagg_info->old_rec_lsn_max = page->disagg_info->rec_lsn_max = block_meta.disagg_lsn;
    WT_ERR(__wt_page_parent_modify_set(session, ref, false));
    WT_STAT_CONN_INCR(session, disagg_frozen_pages_rewritten);

err:
    if (locked)
        WT_REF_UNLOCK(ref, previous_state);
    __wt_scr_free(session, &image);
    return (ret);
}

/*
 * __layered_frozen_rewrite --
 *     Rewrite every resident page of the session's tree written above the given checkpoint. The
 *     fault-in at demote and the eviction pin made every such page resident. A page reconciled
 *     since it was read, and not evicted, has no image of its newest version to copy: it is left to
 *     its next reconciliation.
 */
static int
__layered_frozen_rewrite(WT_SESSION_IMPL *session, uint64_t ckpt_lsn)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_REF *ref;

    btree = S2BT(session);
    ref = NULL;
    __wt_spin_lock(session, &btree->flush_lock);
    while ((ret = __wt_tree_walk(
              session, &ref, WT_READ_CACHE | WT_READ_NO_EVICT | WT_READ_VISIBLE_ALL)) == 0 &&
      ref != NULL) {
        WT_PAGE *page = ref->page;
        if (__layered_frozen_page_lsn(page) <= ckpt_lsn || __wt_ref_is_root(ref) ||
          page->dsk == NULL || page->disagg_info->block_meta.page_id == WT_BLOCK_INVALID_PAGE_ID ||
          (page->modify != NULL && page->modify->rec_result != 0))
            continue;
        WT_ERR(__layered_frozen_rewrite_page(session, ref, ckpt_lsn));
    }

err:
    WT_TRET(__wt_page_release(session, ref, 0));
    __wt_spin_unlock(session, &btree->flush_lock);
    return (ret);
}

/*
 * __wti_layered_frozen_unfreeze --
 *     Make a frozen tree this node's writable live tree again at step-up. Its pages written after
 *     the last checkpoint are kept: the step-up continues this lineage rather than abandoning back
 *     past them. They are rewritten because another node's abandon may have deleted them from the
 *     page log. The caller holds the tree's eviction exclusive.
 */
int
__wti_layered_frozen_unfreeze(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_DECL_RET;
    uint64_t ckpt_lsn;

    WT_ASSERT(session, F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN));

    ckpt_lsn = __wt_atomic_load_uint64_relaxed(&btree->disagg_frozen_ckpt_lsn);
    F_CLR_ATOMIC_32(btree, WT_BTREE_READONLY | WT_BTREE_DISAGG_FROZEN);
    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_max_ts, WT_TS_NONE);
    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_ckpt_lsn, WT_DISAGG_LSN_NONE);
    WT_STAT_CONN_DECR(session, disagg_frozen_handles);

    if (ckpt_lsn == WT_DISAGG_LSN_NONE)
        return (0);
    WT_WITH_BTREE(session, btree, ret = __layered_frozen_rewrite(session, ckpt_lsn));
    return (ret);
}

/*
 * __layered_frozen_uncovered --
 *     Return whether a checkpoint at the given timestamp fails to cover a frozen tree with the
 *     given bound. A bound of none means the tree recorded no durable timestamp at demote, so its
 *     layered tables took no timestamped writes and any checkpoint covers it. A checkpoint with no
 *     timestamp holds every commit in its snapshot rather than a timestamp prefix, so it cannot be
 *     compared against the bound and is treated as covering. Both are decisions about those states,
 *     not the check being skipped.
 */
static bool
__layered_frozen_uncovered(wt_timestamp_t checkpoint_ts, wt_timestamp_t frozen_max_ts)
{
    return (
      checkpoint_ts != WT_TS_NONE && frozen_max_ts != WT_TS_NONE && checkpoint_ts < frozen_max_ts);
}

/*
 * __wti_layered_frozen_assert_covered --
 *     Panic unless a checkpoint at the given timestamp covers the named tree, if it is frozen. A
 *     frozen tree holds every commit up to the bound recorded at demote, so an older checkpoint is
 *     a missing prefix. This runs mid-merge after handles have already been marked outdated, and
 *     those marks survive an unroll, so a soft failure would leave the node reading older content.
 */
int
__wti_layered_frozen_assert_covered(
  WT_SESSION_IMPL *session, const char *uri, wt_timestamp_t checkpoint_ts)
{
    wt_timestamp_t frozen_max_ts;
    char ts_string[2][WT_TS_INT_STRING_SIZE];
    bool frozen;

    WT_RET(__wt_layered_frozen_lookup(session, uri, &frozen, &frozen_max_ts));

    if (frozen && __layered_frozen_uncovered(checkpoint_ts, frozen_max_ts))
        WT_RET(__wt_panic(session, WT_PANIC,
          "picked up checkpoint timestamp %s is below the frozen tree's max timestamp %s (%s)",
          __wt_timestamp_to_string(checkpoint_ts, ts_string[0]),
          __wt_timestamp_to_string(frozen_max_ts, ts_string[1]), uri));
    return (0);
}

/*
 * __wti_layered_frozen_any_uncovered --
 *     Return whether a checkpoint at the given timestamp fails to cover some live frozen tree, and
 *     the newest bound among them. Superseding a frozen tree with an older checkpoint would drop
 *     acknowledged writes. The caller holds the handle-list lock.
 */
bool
__wti_layered_frozen_any_uncovered(
  WT_SESSION_IMPL *session, wt_timestamp_t checkpoint_ts, wt_timestamp_t *frozen_max_tsp)
{
    WT_DATA_HANDLE *dhandle;
    wt_timestamp_t max_ts, tree_max_ts;

    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_HANDLE_LIST));

    max_ts = WT_TS_NONE;
    TAILQ_FOREACH (dhandle, &S2C(session)->dhqh, q)
        if (__wti_layered_frozen_handle(dhandle, &tree_max_ts))
            max_ts = WT_MAX(max_ts, tree_max_ts);

    *frozen_max_tsp = max_ts;
    return (__layered_frozen_uncovered(checkpoint_ts, max_ts));
}
