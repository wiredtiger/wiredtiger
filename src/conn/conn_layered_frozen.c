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
 * __wti_layered_frozen_freeze --
 *     Freeze a live stable tree at demote so the follower keeps reading its resident pages, which
 *     are this node's committed state above its last checkpoint. The caller has already made the
 *     tree read-only and holds its eviction exclusive.
 */
void
__wti_layered_frozen_freeze(WT_SESSION_IMPL *session, WT_BTREE *btree, wt_timestamp_t max_ts)
{
    WT_ASSERT(session, F_ISSET_ATOMIC_32(btree, WT_BTREE_READONLY));
    WT_ASSERT(session, !F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN));

    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_max_ts, max_ts);
    F_SET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN);
    WT_STAT_CONN_INCR(session, disagg_frozen_handles);
}

/*
 * __wti_layered_frozen_unfreeze --
 *     Make a frozen tree this node's writable live tree again at step-up. Its pages written after
 *     the last checkpoint are kept: the step-up continues this lineage rather than abandoning back
 *     past them. The caller holds the tree's eviction exclusive.
 */
void
__wti_layered_frozen_unfreeze(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_ASSERT(session, F_ISSET_ATOMIC_32(btree, WT_BTREE_DISAGG_FROZEN));

    F_CLR_ATOMIC_32(btree, WT_BTREE_READONLY | WT_BTREE_DISAGG_FROZEN);
    __wt_atomic_store_uint64_relaxed(&btree->disagg_frozen_max_ts, WT_TS_NONE);
    WT_STAT_CONN_DECR(session, disagg_frozen_handles);
}

/*
 * __wti_layered_frozen_uncovered --
 *     Return whether a checkpoint at the given timestamp fails to cover a frozen tree with the
 *     given bound. A bound of none means the tree recorded no durable timestamp at demote, so its
 *     layered tables took no timestamped writes and any checkpoint covers it. A checkpoint with no
 *     timestamp holds every commit in its snapshot rather than a timestamp prefix, so it cannot be
 *     compared against the bound and is treated as covering. Both are decisions about those states,
 *     not the check being skipped.
 */
bool
__wti_layered_frozen_uncovered(wt_timestamp_t checkpoint_ts, wt_timestamp_t frozen_max_ts)
{
    return (
      checkpoint_ts != WT_TS_NONE && frozen_max_ts != WT_TS_NONE && checkpoint_ts < frozen_max_ts);
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
    return (__wti_layered_frozen_uncovered(checkpoint_ts, max_ts));
}
