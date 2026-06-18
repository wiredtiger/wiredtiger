/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * The non-inline part of btree usage sampling, deliberately out of the hot path.
 * Counters are scaled by the period, that is, if a sample fires one time in 1000,
 * we scale the counter by 1000; see btree_usage.h. To calculate variance, we
 * collect the sum of squares of the difference from the average. This number
 * can grow large, and we prevent it from overflowing.
 */

/*
 * __usage_value --
 *     Record a leaf value-size observation (period-scaled; sum-of-squares saturates).
 */
static void
__usage_value(WT_BTREE_USAGE_STATS *us, uint32_t value_size)
{
    int64_t sq;

    __wt_atomic_add_int64_relaxed(&us->v[WT_BTREE_USAGE_DATA_N], WT_BTREE_USAGE_SAMPLE_PERIOD);
    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_DATA_SUM], (int64_t)WT_BTREE_USAGE_SAMPLE_PERIOD * value_size);

    sq = (int64_t)value_size * value_size * WT_BTREE_USAGE_SAMPLE_PERIOD;
    if (__wt_atomic_load_int64_relaxed(&us->v[WT_BTREE_USAGE_DATA_SUMSQ]) <
      WT_BTREE_USAGE_SUMSQ_SATURATE)
        __wt_atomic_add_int64_relaxed(&us->v[WT_BTREE_USAGE_DATA_SUMSQ], sq);
}

/*
 * __usage_key --
 *     Record a key-size observation at a btree level (period-scaled; sum-of-squares saturates).
 */
static void
__usage_key(WT_BTREE_USAGE_STATS *us, u_int level, uint32_t key_size)
{
    int64_t sq;
    u_int sumsq;

    if (level >= WT_BTREE_USAGE_LEVEL_COUNT)
        level = WT_BTREE_USAGE_LEVEL_COUNT - 1;
    sumsq = WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_SUMSQ);

    __wt_atomic_add_int64_relaxed(&us->v[WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_N)],
      WT_BTREE_USAGE_SAMPLE_PERIOD);
    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_SUM)],
      (int64_t)WT_BTREE_USAGE_SAMPLE_PERIOD * key_size);

    sq = (int64_t)key_size * key_size * WT_BTREE_USAGE_SAMPLE_PERIOD;
    if (__wt_atomic_load_int64_relaxed(&us->v[sumsq]) < WT_BTREE_USAGE_SUMSQ_SATURATE)
        __wt_atomic_add_int64_relaxed(&us->v[sumsq], sq);
}

/*
 * __usage_int_keys --
 *     Record internal (separator) key-size observations at a btree level. Raw, not period-scaled --
 *     these come from a read-time census or a sub-sampled walk up the parent pointers, not the
 *     per-op sampler, so the count is an actual observation count; mean/variance are per level.
 *     Sum-of-squares saturates as elsewhere.
 */
static void
__usage_int_keys(
  WT_BTREE_USAGE_STATS *us, u_int level, uint64_t count, uint64_t sum, uint64_t sumsq)
{
    u_int sq;

    if (level >= WT_BTREE_USAGE_LEVEL_COUNT)
        level = WT_BTREE_USAGE_LEVEL_COUNT - 1;
    sq = WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_SUMSQ);

    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_N)], (int64_t)count);
    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_KEY_IDX(level, WT_BTREE_USAGE_KEY_FIELD_SUM)], (int64_t)sum);
    if (__wt_atomic_load_int64_relaxed(&us->v[sq]) < WT_BTREE_USAGE_SUMSQ_SATURATE)
        __wt_atomic_add_int64_relaxed(&us->v[sq], (int64_t)sumsq);
}

/*
 * __wt_btree_usage_op_fire --
 *     The out-of-line worker for a fired ~1/1000 sample: classify the leaf position and record the
 *     op, plus the leaf key/value sizes for the ops that carry them, on the same tick.
 */
void
__wt_btree_usage_op_fire(
  WT_SESSION_IMPL *session, WT_REF *ref, uint8_t op, uint32_t key_size, uint32_t value_size)
{
    WT_BTREE_USAGE_STATS *us;
    WT_REF *parent_ref;
    uint8_t pos;
    bool left, right;

    us = &S2BT(session)->usage;

    /*
     * Edge classification, in order of strength:
     *  - LEFT/RIGHT: this is the leftmost/rightmost leaf of the tree (a unique edge).
     *  - NEAR_LEFT/NEAR_RIGHT: not the edge leaf, but its parent is the edge internal page, so this
     *    leaf is within ~one fan-out of the edge -- wide "Pareto" locality. Skip the parent
     *    check when the parent is the root (its ref has no home): in a 2-level tree the root is
     *    trivially both edges and would tag every non-edge leaf.
     *  - MIDDLE: everything else. A sole-leaf "both" has no positional signal and lands here too.
     */
    left = ref != NULL && F_ISSET(ref, WT_REF_FLAG_LEFTMOST);
    right = ref != NULL && F_ISSET(ref, WT_REF_FLAG_RIGHTMOST);
    if (right && !left)
        pos = WT_BTREE_USAGE_POS_RIGHT;
    else if (left && !right)
        pos = WT_BTREE_USAGE_POS_LEFT;
    else {
        pos = WT_BTREE_USAGE_POS_MIDDLE;
        if (ref != NULL && ref->home != NULL) {
            parent_ref = ref->home->pg_intl_parent_ref;
            if (parent_ref != NULL && parent_ref->home != NULL) {
                left = F_ISSET(parent_ref, WT_REF_FLAG_LEFTMOST);
                right = F_ISSET(parent_ref, WT_REF_FLAG_RIGHTMOST);
                if (right && !left)
                    pos = WT_BTREE_USAGE_POS_NEAR_RIGHT;
                else if (left && !right)
                    pos = WT_BTREE_USAGE_POS_NEAR_LEFT;
            }
        }
    }

    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_OP_IDX(pos, op)], WT_BTREE_USAGE_SAMPLE_PERIOD);

    /* Search/search-near sample the positioned key+value; update/overwrite, value only. */
    if (op == WT_BTREE_USAGE_OP_INSERT || op == WT_BTREE_USAGE_OP_SEARCH ||
      op == WT_BTREE_USAGE_OP_SEARCH_NEAR) {
        __usage_key(us, WT_BTREE_USAGE_LEVEL_LEAF, key_size);
        __usage_value(us, value_size);
    } else if (op == WT_BTREE_USAGE_OP_UPDATE || op == WT_BTREE_USAGE_OP_INSERT_OVERWRITE)
        __usage_value(us, value_size);

    /*
     * On roughly 1/8 of fired samples (the freshly reseeded counter's low bits), walk up the parent
     * pointers from this leaf to the root and record each level's separator-key size. This gives
     * internal key-size coverage for in-memory trees that never read their internal pages from
     * disk; the levels are exact (counted while ascending), and the parent pages are pinned while
     * we hold the leaf. Row-store only -- column-store internal pages are keyed by record number.
     */
    if (ref != NULL && S2BT(session)->type == BTREE_ROW &&
      (session->random_hotpath_counter_1000 & 7) == 0) {
        WT_REF *wref;
        const void *ikey;
        size_t isize;
        u_int level;

        ikey = NULL;
        for (wref = ref, level = WT_BTREE_USAGE_LEVEL_LEAF + 1;
             level < WT_BTREE_USAGE_LEVEL_COUNT && wref->home != NULL; ++level) {
            __wt_ref_key(wref->home, wref, &ikey, &isize);
            __usage_int_keys(us, level, 1, (uint64_t)isize, (uint64_t)isize * isize);
            if ((wref = wref->home->pg_intl_parent_ref) == NULL)
                break;
        }
        WT_UNUSED(ikey);
    }
}

/*
 * __wti_btree_usage_int_keys --
 *     Record internal separator key-size observations for the current btree at a level (1 = parent
 *     of leaves). Called by the page-read census; raw counts, mean/variance taken per level.
 */
void
__wti_btree_usage_int_keys(
  WT_SESSION_IMPL *session, u_int level, uint64_t count, uint64_t sum, uint64_t sumsq)
{
    __usage_int_keys(&S2BT(session)->usage, level, count, sum, sumsq);
}

/*
 * __usage_score --
 *     Rank a btree by total sampled op activity: the contiguous op-count block of the stats array.
 */
static int64_t
__usage_score(WT_BTREE_USAGE_STATS *us)
{
    int64_t score;
    u_int i;

    score = 0;
    for (i = 0; i < WT_BTREE_USAGE_POS_COUNT * WT_BTREE_USAGE_OP_COUNT; i++)
        score += __wt_atomic_load_int64_relaxed(&us->v[WT_BTREE_USAGE_OP_BASE + i]);
    return (score);
}

/*
 * __wt_btree_usage_collect --
 *     Walk all open btrees and refresh the connection-level usage snapshot: the top-N highest-
 *     scoring user btrees, a pinned history-store slot, and one uniformly random "sample" btree
 *     from the remainder. Resets per-btree counters after taking the snapshot, and logs the
 *     slot-to-URI mapping for later FTDC correlation. Called once per sweep interval from the sweep
 *     server.
 */
void
__wt_btree_usage_collect(WT_SESSION_IMPL *session)
{
    struct {
        WT_DATA_HANDLE *dhandle;
        int64_t score;
    } top[WT_BTREE_USAGE_TOP_N];
    WT_BTREE *btree;
    WT_BTREE_USAGE_SNAPSHOT *snap;
    WT_BTREE_USAGE_STATS *us;
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle, *hs, *sample;
    int64_t min_score, score, active;
    int64_t new_streak[WT_BTREE_USAGE_TOP_N], hs_streak, sample_streak;
    uint32_t i, id, j, sample_seen, min_idx, ntop;
    bool changed;

    conn = S2C(session);
    ntop = 0;
    min_idx = 0;
    min_score = 0;
    active = 0;
    hs = NULL;
    sample = NULL;
    sample_seen = 0;

    /*
     * Hold the dhandle list read lock for the whole pass: we dereference dhandle->handle and read,
     * snapshot, and reset btree->usage, so the btree must not be discarded under us (a closing
     * handle stays marked open but its btree is torn down, which is the eviction-walk hazard too).
     * Counter reads/writes use relaxed atomics; slight imprecision from concurrent ops is fine for
     * statistics. The published snapshot holds copies, never dhandle pointers.
     */
    __wt_readlock(session, &conn->dhandle_lock);
    TAILQ_FOREACH (dhandle, &conn->dhqh, q) {
        if (!F_ISSET(dhandle, WT_DHANDLE_OPEN) || WT_IS_METADATA(dhandle) ||
          !WT_DHANDLE_BTREE(dhandle))
            continue;

        btree = dhandle->handle;
        if (btree == NULL)
            continue;
        us = &btree->usage;
        score = __usage_score(us);
        if (score > 0)
            ++active;

        /* The history store is pinned to its own slot rather than ranked with user btrees. */
        if (WT_IS_HS(dhandle)) {
            hs = dhandle;
            continue;
        }

        if (ntop < WT_BTREE_USAGE_TOP_N) {
            /* Top array not yet full; add unconditionally. */
            top[ntop].dhandle = dhandle;
            top[ntop].score = score;
            if (ntop == 0 || score < min_score) {
                min_score = score;
                min_idx = ntop;
            }
            ntop++;
        } else if (score > min_score) {
            /* Displace the current minimum; the displaced entry becomes a sample candidate. */
            sample_seen++;
            if (__wt_random(&session->rnd_random) % sample_seen == 0)
                sample = top[min_idx].dhandle;

            top[min_idx].dhandle = dhandle;
            top[min_idx].score = score;

            /* Recompute the new minimum. */
            min_score = top[0].score;
            min_idx = 0;
            for (i = 1; i < ntop; i++)
                if (top[i].score < min_score) {
                    min_score = top[i].score;
                    min_idx = i;
                }
        } else {
            /* Not in top-N: candidate for the sample slot (reservoir sampling, k=1). */
            sample_seen++;
            if (__wt_random(&session->rnd_random) % sample_seen == 0)
                sample = dhandle;
        }
    }

    /*
     * Sort the top set by score, descending, so the published slots are rank-ordered (slot 0 = most
     * active) for the leaderboard view. Reuse dhandle/score as scratch -- the walk is done with
     * them.
     */
    for (i = 1; i < ntop; i++) {
        dhandle = top[i].dhandle;
        score = top[i].score;
        for (j = i; j > 0 && top[j - 1].score < score; j--)
            top[j] = top[j - 1];
        top[j].dhandle = dhandle;
        top[j].score = score;
    }

    /* Publish the snapshot under the write lock. */
    __wt_writelock(session, &conn->btree_usage_lock);
    conn->btree_usage_active = active;

    /*
     * Compute persistence streaks before overwriting: a btree still in the top set inherits its
     * previous streak + 1, otherwise it starts at 1. Match on btree id against the previous top
     * slots (still present in the snapshot here) -- slot positions are not stable as rankings move.
     */
    for (i = 0; i < ntop; i++) {
        id = ((WT_BTREE *)top[i].dhandle->handle)->id;
        new_streak[i] = 1;
        for (j = 0; j < WT_BTREE_USAGE_TOP_N; j++)
            if (conn->btree_usage[j].valid && conn->btree_usage[j].btree_id == id) {
                new_streak[i] = conn->btree_usage[j].streak + 1;
                break;
            }
    }
    sample_streak = 1;
    if (sample != NULL) {
        id = ((WT_BTREE *)sample->handle)->id;
        for (j = 0; j < WT_BTREE_USAGE_TOP_N; j++)
            if (conn->btree_usage[j].valid && conn->btree_usage[j].btree_id == id) {
                sample_streak = conn->btree_usage[j].streak + 1;
                break;
            }
    }
    /* The history store keeps its own pinned slot, so its streak just carries forward there. */
    hs_streak = 1;
    if (hs != NULL && conn->btree_usage[WT_BTREE_USAGE_SLOT_PIN_HS].valid)
        hs_streak = conn->btree_usage[WT_BTREE_USAGE_SLOT_PIN_HS].streak + 1;

    changed = false;
    for (i = 0; i < ntop; i++) {
        snap = &conn->btree_usage[i];
        dhandle = top[i].dhandle;
        btree = dhandle->handle;
        us = &btree->usage;

        if (snap->btree_id != btree->id)
            changed = true;
        snap->valid = true;
        snap->score = top[i].score;
        snap->streak = new_streak[i];
        snap->btree_id = btree->id;
        snap->type = (uint8_t)btree->type;
        WT_IGNORE_RET(__wt_snprintf(snap->uri, sizeof(snap->uri), "%s", dhandle->name));
        memcpy(&snap->stats, us, sizeof(*us));
    }
    /* Invalidate any slots beyond the current top count. */
    for (i = ntop; i < WT_BTREE_USAGE_TOP_N; i++)
        conn->btree_usage[i].valid = false;

    /* Pinned history-store slot. */
    snap = &conn->btree_usage[WT_BTREE_USAGE_SLOT_PIN_HS];
    if (hs != NULL) {
        btree = hs->handle;
        us = &btree->usage;
        if (snap->btree_id != btree->id)
            changed = true;
        snap->valid = true;
        snap->score = __usage_score(us);
        snap->streak = hs_streak;
        snap->btree_id = btree->id;
        snap->type = (uint8_t)btree->type;
        WT_IGNORE_RET(__wt_snprintf(snap->uri, sizeof(snap->uri), "%s", hs->name));
        memcpy(&snap->stats, us, sizeof(*us));
    } else
        snap->valid = false;

    /* Random "sample" slot. */
    snap = &conn->btree_usage[WT_BTREE_USAGE_SLOT_SAMPLE];
    if (sample != NULL) {
        btree = sample->handle;
        us = &btree->usage;
        if (snap->btree_id != btree->id)
            changed = true;
        snap->valid = true;
        snap->score = __usage_score(us);
        snap->streak = sample_streak;
        snap->btree_id = btree->id;
        snap->type = (uint8_t)btree->type;
        WT_IGNORE_RET(__wt_snprintf(snap->uri, sizeof(snap->uri), "%s", sample->name));
        memcpy(&snap->stats, us, sizeof(*us));
    } else
        snap->valid = false;

    __wt_writeunlock(session, &conn->btree_usage_lock);

    /*
     * Reset per-btree counters, still under the dhandle list lock so the btrees stay live. Slight
     * imprecision from ops that land between the snapshot copy and the memset is acceptable.
     */
    for (i = 0; i < ntop; i++) {
        btree = top[i].dhandle->handle;
        if (btree != NULL)
            memset(&btree->usage, 0, sizeof(btree->usage));
    }
    if (hs != NULL && (btree = hs->handle) != NULL)
        memset(&btree->usage, 0, sizeof(btree->usage));
    if (sample != NULL && (btree = sample->handle) != NULL)
        memset(&btree->usage, 0, sizeof(btree->usage));

    __wt_readunlock(session, &conn->dhandle_lock);

    /*
     * Log the slot-to-URI mapping at info level so FTDC snapshots can be correlated with btree
     * names without requiring verbose logging. Emit every interval so the mapping is always
     * recoverable from a log window around any FTDC sample.
     */
    if (ntop > 0 || changed)
        for (i = 0; i < ntop; i++) {
            snap = &conn->btree_usage[i];
            __wt_verbose_level(session, WT_VERB_SWEEP, WT_VERBOSE_INFO,
              "btree usage slot %" PRIu32 ": id=%" PRIu32 " score=%" PRId64 " uri=%s", i,
              snap->btree_id, snap->score, snap->uri);
        }
    if (hs != NULL) {
        snap = &conn->btree_usage[WT_BTREE_USAGE_SLOT_PIN_HS];
        __wt_verbose_level(session, WT_VERB_SWEEP, WT_VERBOSE_INFO,
          "btree usage history store: id=%" PRIu32 " score=%" PRId64 " uri=%s", snap->btree_id,
          snap->score, snap->uri);
    }
    if (sample != NULL) {
        snap = &conn->btree_usage[WT_BTREE_USAGE_SLOT_SAMPLE];
        __wt_verbose_level(session, WT_VERB_SWEEP, WT_VERBOSE_INFO,
          "btree usage sample: id=%" PRIu32 " score=%" PRId64 " uri=%s", snap->btree_id,
          snap->score, snap->uri);
    }
}

/*
 * __usage_split_skip_estimate --
 *     Estimate the entry count of a skiplist from its shape: the top populated level d holds about
 *     N / 4^d entries, so counting just that sparse level gives N ~= count << 2d.
 */
static uint64_t
__usage_split_skip_estimate(WT_INSERT_HEAD *ins_head)
{
    WT_INSERT *ins;
    uint64_t count;
    int d;

    if (ins_head == NULL)
        return (0);

    for (d = WT_SKIP_MAXDEPTH - 1; d > 0; --d)
        if (ins_head->head[d] != NULL)
            break;

    /* Level 0 is exact (every entry is linked there); higher levels are estimated. */
    count = 0;
    for (ins = ins_head->head[d]; ins != NULL; ins = ins->next[d])
        ++count;

    return (count << (2 * d));
}

/*
 * __usage_split_leaf_keys --
 *     Estimate the live key count on a leaf page: exact on-disk entries plus a per-skiplist
 *     estimate of the inserts. Deletes are ignored --
 *     this is a usage estimate, not a census.
 */
static uint64_t
__usage_split_leaf_keys(WT_PAGE *page)
{
    uint64_t keys;
    uint32_t i;

    keys = page->entries;

    if (page->modify == NULL)
        return (keys);

    if (page->type == WT_PAGE_ROW_LEAF) {
        if (page->modify->mod_row_insert != NULL)
            /* One insert list per slot, plus the "smallest" list past the last slot. */
            for (i = 0; i <= page->entries; ++i)
                keys += __usage_split_skip_estimate(page->modify->mod_row_insert[i]);
    } else if (page->type == WT_PAGE_COL_VAR)
        keys += __usage_split_skip_estimate(WT_COL_APPEND(page));

    return (keys);
}

/*
 * __wti_btree_usage_split_sample --
 *     Sample a leaf split: gate at 1/WT_BTREE_USAGE_SPLIT_PERIOD with the session RNG, then record
 *     the period-scaled split count, resulting page count, and estimated pre-split key total across
 *     the splitting page(s). Splits are rare and heavyweight, so the gate is checked before the key
 *     estimate to keep the walk off splits that aren't sampled. page2 may be NULL.
 */
void
__wti_btree_usage_split_sample(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_PAGE *page2, uint32_t result_pages)
{
    WT_BTREE_USAGE_STATS *us;
    uint64_t est_keys;

    /* Internal-page splits aren't sampled; only leaf key counts are meaningful here. */
    if (WT_PAGE_IS_INTERNAL(page))
        return;

    if (__wt_random(&session->rnd_random) % WT_BTREE_USAGE_SPLIT_PERIOD != 0)
        return;

    est_keys = __usage_split_leaf_keys(page);
    if (page2 != NULL)
        est_keys += __usage_split_leaf_keys(page2);

    us = &S2BT(session)->usage;
    __wt_atomic_add_int64_relaxed(&us->v[WT_BTREE_USAGE_SPLIT_COUNT], WT_BTREE_USAGE_SPLIT_PERIOD);
    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_SPLIT_PAGES], (int64_t)WT_BTREE_USAGE_SPLIT_PERIOD * result_pages);
    __wt_atomic_add_int64_relaxed(
      &us->v[WT_BTREE_USAGE_SPLIT_KEYS], (int64_t)(WT_BTREE_USAGE_SPLIT_PERIOD * est_keys));
}
