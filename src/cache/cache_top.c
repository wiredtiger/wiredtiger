/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * These rankings answer "which tables are holding the cache" without walking the cache or the data
 * handle array. That matters on a deployment with millions of open tables, where either walk is too
 * expensive to run on a live connection.
 *
 * Five rankings are kept, one per metric: update bytes, dirty leaf bytes, and resident bytes are
 * levels, read directly off a tree's own counters; recent bytes read and recent bytes evicted are
 * flows, computed from a per-tree value that is aged backward as it is read, so a burst from an
 * hour ago fades out instead of accumulating forever.
 *
 * Each ranking is a fixed array of slots, guarded by one spinlock, plus a threshold a tree's metric
 * must reach to be worth a slot. The threshold is what makes the ranking complete rather than a
 * sample: every metric is bounded by the cache size (that is what the decay on the flow metrics
 * buys), so a threshold of cache size / slot count guarantees at most that many trees can ever
 * qualify, and a tree missing from the array is provably below it. The very first threshold is
 * seeded from the average resident tree size, since on a deployment with far more trees than slots,
 * cache size / slot count alone is nowhere near the size of the trees actually worth naming; every
 * later value comes from adjusting it, each time a report runs, to keep the array usefully full.
 *
 * A tree enters the accounting path already knowing where it stands. Each tree carries, per metric,
 * the value it must grow past before it is worth another look at the ranking; until then, the
 * accounting path pays nothing but its own comparison. It also carries which slot it currently
 * occupies, if any, so refreshing an already-tracked tree is a direct update instead of a search;
 * only admitting a genuinely new tree costs a scan of the whole array, to find the entry to
 * displace. There is no symmetric hook on the decrement side: a tree that shrinks is not actively
 * removed. It does not need to be, because both the slot-selection scan and report generation
 * re-read every occupied slot's live value before acting on it, so a shrunk tree is correctly
 * treated as small, and dropped from the array once a report actually looks, whether that look was
 * triggered by a new tree contending for a slot or by a report being generated. Metadata and the
 * history store never enter any of this: they are excluded for good when a tree is opened, by
 * setting their values to one no counter can reach, rather than have the hot path discover the
 * exclusion on every byte it accounts for.
 *
 * A report walks every ranking's array under its lock, copying out entries that still clear the
 * threshold, adjusting the threshold for next time, and building lines to print once the lock is
 * dropped. It only runs when something will be printed: on demand, through a debug_info category,
 * or from the sweep server's periodic pass, which prints when the verbose category for this
 * reporting is on or update bytes are over the configured target. A quiet connection with neither
 * condition true does no work at all; nothing is lost by skipping those ticks, since the next
 * report that does run reads everything fresh and catches up immediately.
 *
 * What this cannot see: a tree that was never touched after opening (no updates, no reads, no
 * eviction) has nothing to trigger its own consideration and so never enters a ranking even if it
 * is large, and a tracked tree with no activity at all similarly never gets re-examined. In
 * practice this does not matter for the case this file exists for, cache pressure, since a tree
 * under pressure is by definition being evicted from, which is what re-examines it.
 */

/*
 * One line of a report, filled in under the array lock and printed after the lock is dropped. The
 * name is copied into its own allocation, rather than referenced or copied into a fixed-size
 * buffer, for two reasons: the tree it came from may be gone by the time we print it, and there is
 * no fixed length that is both large enough for every table name and not wasteful for the common,
 * short ones.
 */
struct __wt_cache_top_report_entry {
    char *name; /* Freed by the caller; NULL when the slot is unused. */
    uint64_t value;
};
typedef struct __wt_cache_top_report_entry WT_CACHE_TOP_REPORT_ENTRY;

/*
 * __cache_top_entries_free --
 *     Free the names owned by the first count entries of a report array, leaving the entries
 *     themselves ready to reuse for the next ranking.
 */
static void
__cache_top_entries_free(
  WT_SESSION_IMPL *session, WT_CACHE_TOP_REPORT_ENTRY *entries, uint32_t count)
{
    uint32_t i;

    for (i = 0; i < count; ++i)
        __wt_free(session, entries[i].name);
}

/*
 * __cache_top_threshold --
 *     Return the value a tree's metric must reach before the tree is added to this ranking,
 *     computing it the first time it is needed for this ranking.
 */
static uint64_t
__cache_top_threshold(WT_SESSION_IMPL *session, WT_CACHE_TOP_ARRAY *array)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t threshold;
    uint32_t trees;

    conn = S2C(session);

    threshold = __wt_atomic_load_uint64_relaxed(&array->threshold);
    if (threshold != 0)
        return (threshold);

    /*
     * Cache size divided by the slot count is the highest threshold we can justify: it guarantees
     * no more than that many trees can qualify. But on a deployment with far more trees than slots,
     * even the largest tree is nowhere near that big, so start lower, from a multiple of the
     * average resident tree, and let the threshold adjust upward or downward from there each time a
     * report runs. Both the tree count and the cache-in-use figure are plain counters, so computing
     * this costs no walk.
     */
    trees = __wt_atomic_load_uint32_relaxed(&conn->open_btree_count);
    threshold = conn->cache_size / WT_CACHE_TOP_SLOTS;
    if (trees > WT_CACHE_TOP_SLOTS)
        threshold = WT_MIN(
          threshold, (__wt_cache_bytes_inuse(conn->cache) / trees) * WT_CACHE_TOP_SEED_MULTIPLIER);
    threshold = WT_MAX(threshold, WT_MEGABYTE);

    /*
     * Store it before returning. Some callers only act when a threshold is already set, so the
     * first caller to compute one has to save it, not just use it.
     */
    __wt_atomic_store_uint64_relaxed(&array->threshold, threshold);

    return (threshold);
}

/*
 * __cache_top_flow_storage --
 *     Return pointers to the stored value and clock backing a flow ranking (bytes read, bytes
 *     evicted), for a caller that needs to update them. The stored value is only decayed as of the
 *     clock it was last written with, not as of now; a caller that just wants the current value
 *     should use __cache_top_flow_value instead of decaying this itself. Both pointers come back
 *     NULL for a ranking that tracks a level, since a level has no decay state to point at.
 */
static void
__cache_top_flow_storage(
  WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, uint64_t **valuep, uint64_t **clockp)
{
    switch (metric) {
    case WT_CACHE_TOP_READ:
        *valuep = &btree->bytes_read_decayed;
        *clockp = &btree->bytes_read_decay_clock;
        break;
    case WT_CACHE_TOP_EVICT:
        *valuep = &btree->bytes_evict_decayed;
        *clockp = &btree->bytes_evict_decay_clock;
        break;
    case WT_CACHE_TOP_UPDATES:
    case WT_CACHE_TOP_DIRTY:
    case WT_CACHE_TOP_INMEM:
        *valuep = *clockp = NULL;
        break;
    }
}

/*
 * __cache_top_decay --
 *     Apply time decay to a value that tracks a flow (bytes read, bytes evicted) rather than a
 *     level. Decay is what keeps a flow's total bounded by the cache size, which the completeness
 *     of the ranking depends on.
 *
 * Decay only advances in whole half-lives, so it can leave up to one half-life of elapsed time
 *     unaccounted for. The caller must save the clock this function returns, not the one it was
 *     given: if a caller kept using the original clock, a tree touched more often than once per
 *     half-life would never accumulate enough elapsed time to decay at all, and its value would
 *     grow forever. A caller that only wants to read the current value, and does not intend to
 *     store anything back, passes NULL and the time already recorded is left untouched.
 */
static uint64_t
__cache_top_decay(
  WT_SESSION_IMPL *session, uint64_t value, uint64_t clock, uint64_t now, uint64_t *newclockp)
{
    uint64_t halflife, halvings;

    if (newclockp != NULL)
        *newclockp = clock;

    if (value == 0 || clock == 0 || now <= clock)
        return (value);

    halflife = S2C(session)->cache->cache_top.halflife_ticks;
    halvings = (now - clock) / halflife;
    if (halvings == 0)
        return (value);

    if (newclockp != NULL)
        *newclockp = clock + halvings * halflife;

    return (halvings >= WT_CACHE_TOP_DECAY_MAX_HALVINGS ? 0 : value >> halvings);
}

/*
 * __cache_top_flow_value --
 *     Return a flow ranking's current value (bytes read, bytes evicted), decayed up to now. Unlike
 *     __cache_top_flow_storage, this has nothing to do with where the value is stored; it is a
 *     plain read for a caller that only wants a number.
 */
static uint64_t
__cache_top_flow_value(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric)
{
    uint64_t *clockp, *valuep;

    __cache_top_flow_storage(btree, metric, &valuep, &clockp);
    return (__cache_top_decay(session, __wt_atomic_load_uint64_relaxed(valuep),
      __wt_atomic_load_uint64_relaxed(clockp), __wt_clock(session), NULL));
}

/*
 * __cache_top_value --
 *     Return a tree's current value for a ranking.
 */
static uint64_t
__cache_top_value(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric)
{
    switch (metric) {
    case WT_CACHE_TOP_UPDATES:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_updates));
    case WT_CACHE_TOP_DIRTY:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_dirty_leaf));
    case WT_CACHE_TOP_INMEM:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_inmem));
    case WT_CACHE_TOP_READ:
    case WT_CACHE_TOP_EVICT:
        return (__cache_top_flow_value(session, btree, metric));
    }

    return (0);
}

/*
 * __cache_top_recheck_at_set --
 *     Set the value a tree must reach before it is considered for a ranking again. Waiting for the
 *     tree to grow by a fraction of the threshold, rather than checking on every change, bounds how
 *     many times a tree can cost a visit over its lifetime, no matter how much data moves through
 *     it. Two threads can race to set this value for the same tree; the one that loses just causes
 *     one extra, harmless visit later.
 */
static void
__cache_top_recheck_at_set(
  WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, uint64_t threshold, uint64_t value)
{
    __wt_atomic_store_uint64_relaxed(&btree->cache_top_recheck_at[metric],
      WT_MAX(threshold, value + threshold / WT_CACHE_TOP_RECHECK_DIVISOR));
}

/*
 * __cache_top_smallest --
 *     Return the index of an unused slot if one exists, or otherwise the index of the slot holding
 *     the smallest value. Refreshes every slot's value along the way, since this is also the one
 *     place that has to look at all of them.
 */
static uint32_t
__cache_top_smallest(
  WT_SESSION_IMPL *session, WT_CACHE_TOP_ARRAY *array, WT_CACHE_TOP_METRIC metric)
{
    uint32_t i, smallest;

    smallest = 0;
    for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i) {
        if (array->slots[i].btree == NULL)
            return (i);
        array->slots[i].value = __cache_top_value(session, array->slots[i].btree, metric);
        if (array->slots[i].value < array->slots[smallest].value)
            smallest = i;
    }

    return (smallest);
}

/*
 * __wt_cache_top_track --
 *     Decide whether a tree belongs in a ranking now. Called from the accounting path once a tree's
 *     counter has reached the value it was told to wait for, so in the common case, where the
 *     counter has not reached that value, the only cost paid is the caller's comparison, not a call
 *     into this function at all.
 */
void
__wt_cache_top_track(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, uint64_t value)
{
    WT_CACHE_TOP_ARRAY *array;
    uint64_t threshold;
    uint32_t slot;

    /*
     * A tree excluded from this ranking (see __wt_cache_top_btree_open) has its recheck value
     * pinned at UINT64_MAX, so its counter is never going to reach it and this function is never
     * called for it in practice; there is nothing left to check here.
     */
    array = &S2C(session)->cache->cache_top.arrays[metric];
    threshold = __cache_top_threshold(session, array);

    /*
     * Most visits are a tree that is still below the threshold; it has nothing to add to the
     * ranking, and the recheck value it needs depends only on its own size. Handle that case
     * without touching the array lock at all.
     */
    if (value < threshold) {
        __cache_top_recheck_at_set(btree, metric, threshold, value);
        return;
    }

    __wt_spin_lock(session, &array->lock);

    /*
     * A tree remembers which slot it occupies, so refreshing an already-tracked tree is a direct
     * update, not a search: this field is only ever written under this same lock, so once we hold
     * the lock, the value we read here is guaranteed current, not merely a hint.
     */
    slot = __wt_atomic_load_uint8_relaxed(&btree->cache_top_slot[metric]);
    if (slot < WT_CACHE_TOP_SLOTS) {
        WT_ASSERT(session, array->slots[slot].btree == btree);
        array->slots[slot].value = value;
        goto done;
    }

    slot = __cache_top_smallest(session, array, metric);
    if (array->slots[slot].btree != NULL && array->slots[slot].value >= value)
        goto done;

    if (array->slots[slot].btree != NULL)
        __wt_atomic_store_uint8_relaxed(
          &array->slots[slot].btree->cache_top_slot[metric], WT_CACHE_TOP_NOT_TRACKED);
    array->slots[slot].btree = btree;
    array->slots[slot].value = value;
    __wt_atomic_store_uint8_relaxed(&btree->cache_top_slot[metric], (uint8_t)slot);

done:
    __cache_top_recheck_at_set(btree, metric, threshold, value);

    __wt_spin_unlock(session, &array->lock);
}

/*
 * __cache_top_levels_refresh --
 *     Give a tree another chance at the level rankings (update bytes, dirty bytes, resident bytes)
 *     after a threshold has dropped below the value the tree was told to wait for. Normally only
 *     growth gets a tree reconsidered, so a tree that stopped growing right before its threshold
 *     fell would otherwise never be looked at again, even though it may now be large enough to
 *     qualify. Eviction happening on the tree is the signal used to trigger this: it proves the
 *     tree is still resident and worth a second look.
 */
static void
__cache_top_levels_refresh(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    static const WT_CACHE_TOP_METRIC levels[] = {
      WT_CACHE_TOP_UPDATES, WT_CACHE_TOP_DIRTY, WT_CACHE_TOP_INMEM};
    WT_CACHE_TOP *top;
    WT_CACHE_TOP_METRIC metric;
    uint64_t recheck_at, threshold;
    u_int i;

    top = &S2C(session)->cache->cache_top;

    for (i = 0; i < WT_ELEMENTS(levels); ++i) {
        metric = levels[i];

        /* A tree already occupying a slot is already visible; checking again would only cost a
         * lock. */
        if (__wt_atomic_load_uint8_relaxed(&btree->cache_top_slot[metric]) < WT_CACHE_TOP_SLOTS)
            continue;

        /* The maximum value means this tree is permanently excluded, not merely overdue. */
        recheck_at = __wt_atomic_load_uint64_relaxed(&btree->cache_top_recheck_at[metric]);
        if (recheck_at == UINT64_MAX)
            continue;

        threshold = __wt_atomic_load_uint64_relaxed(&top->arrays[metric].threshold);
        if (threshold != 0 && recheck_at > threshold)
            __wt_atomic_store_uint64_relaxed(&btree->cache_top_recheck_at[metric], threshold);
    }
}

/*
 * __wt_cache_top_flow_incr --
 *     Record that a tree just read or evicted some number of bytes, and check whether that changes
 *     its standing in the corresponding ranking.
 */
void
__wt_cache_top_flow_incr(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, size_t size)
{
    uint64_t clock, now, value, *clockp, *valuep;

    if (btree == NULL)
        return;

    __cache_top_flow_storage(btree, metric, &valuep, &clockp);
    WT_ASSERT_ALWAYS(session, valuep != NULL, "cache top: %d does not track a flow", (int)metric);

    /*
     * Concurrent callers on the same tree can lose an increment here. The rankings are a diagnostic
     * ordering, not accounting that has to balance.
     */
    now = __wt_clock(session);
    clock = __wt_atomic_load_uint64_relaxed(clockp);
    value =
      __cache_top_decay(session, __wt_atomic_load_uint64_relaxed(valuep), clock, now, &clock) +
      size;
    __wt_atomic_store_uint64_relaxed(valuep, value);
    __wt_atomic_store_uint64_relaxed(clockp, clock == 0 ? now : clock);

    if (value >= __wt_atomic_load_uint64_relaxed(&btree->cache_top_recheck_at[metric]))
        __wt_cache_top_track(session, btree, metric, value);

    if (metric == WT_CACHE_TOP_EVICT)
        __cache_top_levels_refresh(session, btree);
}

/*
 * __wt_cache_top_btree_open --
 *     Set up a tree's cache-consumer tracking state when it is opened (including a re-open, since
 *     the tree's fields are cleared then too). Every tree starts out of every ranking's slots,
 *     which has to be set explicitly because a slot index of 0 is valid and zero-initialization
 *     would otherwise claim it. Metadata and the history store are excluded from every ranking here
 *     as well, rather than being discovered by the accounting path later: they already have their
 *     own connection-level statistics, are never the table an operator asking for the largest cache
 *     consumers wants named, and the history store in particular is too hot a tree to be checking
 *     its identity from that path on every byte it accounts for.
 */
void
__wt_cache_top_btree_open(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    u_int metric;
    bool excluded;

    WT_UNUSED(session);
    excluded = WT_IS_ANY_METADATA(btree->dhandle) || WT_IS_HS(btree->dhandle);

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric) {
        btree->cache_top_slot[metric] = WT_CACHE_TOP_NOT_TRACKED;
        if (excluded)
            btree->cache_top_recheck_at[metric] = UINT64_MAX;
    }
}

/*
 * __wt_cache_top_btree_discard --
 *     Remove a tree from every ranking it may be part of. Must be called before the tree's memory
 *     is freed, since a ranking can otherwise be left holding a pointer to it.
 */
void
__wt_cache_top_btree_discard(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_CACHE *cache;
    WT_CACHE_TOP_ARRAY *array;
    uint32_t i;
    u_int metric;

    if ((cache = S2C(session)->cache) == NULL)
        return;

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric) {
        array = &cache->cache_top.arrays[metric];
        __wt_spin_lock(session, &array->lock);
        for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i)
            if (array->slots[i].btree == btree) {
                __wt_atomic_store_uint8_relaxed(
                  &btree->cache_top_slot[metric], WT_CACHE_TOP_NOT_TRACKED);
                array->slots[i].btree = NULL;
                array->slots[i].value = 0;
            }
        __wt_spin_unlock(session, &array->lock);
    }
}

/*
 * __cache_top_snapshot --
 *     Copy one ranking's current entries into a caller-supplied array, ready to print once the
 *     array lock is released. Along the way, drop any tracked tree that has fallen below the
 *     threshold, and adjust the threshold so the ranking stays usefully full.
 */
static int
__cache_top_snapshot(WT_SESSION_IMPL *session, WT_CACHE_TOP_METRIC metric,
  WT_CACHE_TOP_REPORT_ENTRY *entries, uint32_t *countp, uint64_t *thresholdp)
{
    WT_CACHE_TOP_ARRAY *array;
    WT_CACHE_TOP_REPORT_ENTRY tmp;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    uint64_t in_force, smallest, threshold, value;
    uint32_t count, i, j;

    array = &S2C(session)->cache->cache_top.arrays[metric];
    in_force = threshold = __cache_top_threshold(session, array);
    count = 0;
    smallest = UINT64_MAX;

    __wt_spin_lock(session, &array->lock);
    for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i) {
        if (array->slots[i].btree == NULL)
            continue;

        /*
         * A dropped or closed table's handle is not cleaned up right away; sweep does that later.
         * Report it as holding nothing rather than naming a table that no longer exists or no
         * longer holds any cache.
         */
        dhandle = array->slots[i].btree->dhandle;
        value = !F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
            F_ISSET(dhandle, WT_DHANDLE_DEAD | WT_DHANDLE_DROPPED) ?
          0 :
          __cache_top_value(session, array->slots[i].btree, metric);
        if (value < threshold) {
            __wt_atomic_store_uint8_relaxed(
              &array->slots[i].btree->cache_top_slot[metric], WT_CACHE_TOP_NOT_TRACKED);
            array->slots[i].btree = NULL;
            array->slots[i].value = 0;
            continue;
        }
        array->slots[i].value = value;
        smallest = WT_MIN(smallest, value);

        WT_ERR(__wt_strdup(session, dhandle->name, &entries[count].name));
        entries[count].value = value;
        ++count;
    }

    /*
     * Adjust the threshold so it tracks the smallest table actually worth ranking. When the ranking
     * is completely full, we know exactly where to put the bar: just above the smallest entry we
     * kept, which also cuts down on how often trees get revisited. When nothing at all qualified,
     * the threshold is telling us nothing useful, so drop it sharply; when only a few tables
     * qualified, drop it more gently. Either way, the ranking only fills back up as trees grow into
     * the new, lower threshold.
     */
    if (count == WT_CACHE_TOP_SLOTS)
        threshold = smallest + 1;
    else if (count == 0)
        threshold = WT_MAX(threshold / 8, WT_MEGABYTE);
    else if (count < WT_CACHE_TOP_SLOTS / 2)
        threshold = WT_MAX(threshold / 2, WT_MEGABYTE);
    __wt_atomic_store_uint64_relaxed(&array->threshold, threshold);

err:
    __wt_spin_unlock(session, &array->lock);
    if (ret != 0) {
        /* This function owns whatever it allocated so far; it failed before telling the caller. */
        __cache_top_entries_free(session, entries, count);
        return (ret);
    }

    /* Insertion sort, descending: the array is at most a slot count long. */
    for (i = 1; i < count; ++i)
        for (j = i; j > 0 && entries[j].value > entries[j - 1].value; --j) {
            tmp = entries[j];
            entries[j] = entries[j - 1];
            entries[j - 1] = tmp;
        }

    *countp = count;

    /* The threshold the entries were selected against, not the one adjusted for the next report. */
    *thresholdp = in_force;
    return (0);
}

/*
 * __cache_top_emit --
 *     Print one line of a report: to the log if the report was explicitly requested, or through the
 *     verbose category if it was generated on our own initiative.
 */
static int
__cache_top_emit(WT_SESSION_IMPL *session, bool force, const char *line)
{
    if (force)
        return (__wt_msg(session, "%s", line));

    __wt_verbose_level(session, WT_VERB_CACHE_TOP, WT_VERBOSE_INFO, "%s", line);
    return (0);
}

/*
 * __cache_top_report --
 *     Build and print all of the rankings. Callers only reach this when they already intend to
 *     print something, so it always does both.
 */
static int
__cache_top_report(WT_SESSION_IMPL *session, bool force)
{
    static const char *metric_desc[] = {"update bytes", "dirty leaf bytes", "total cache bytes",
      "recent bytes read", "recent bytes evicted"};
    static_assert(WT_ELEMENTS(metric_desc) == WT_CACHE_TOP_METRICS,
      "every cache-consumer ranking needs a description");

    WT_CACHE *cache;
    WT_CACHE_TOP_REPORT_ENTRY *entries;
    WT_DECL_RET;
    WT_ITEM *line;
    uint64_t listed, threshold;
    uint64_t connection_total = 0;
    u_int metric;
    uint32_t count, i;
    bool has_total;

    /*
     * Zero, not left uninitialized: if allocation or the very first snapshot fails before count is
     * ever set, the error path below still needs a safe value to free entries up to.
     */
    count = 0;

    cache = S2C(session)->cache;
    WT_RET(__wt_calloc_def(session, WT_CACHE_TOP_SLOTS, &entries));
    WT_ERR(__wt_scr_alloc(session, 0, &line));

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric) {
        WT_ERR(
          __cache_top_snapshot(session, (WT_CACHE_TOP_METRIC)metric, entries, &count, &threshold));

        for (i = 0, listed = 0; i < count; ++i)
            listed += entries[i].value;

        /*
         * A ranking that tracks a level (update bytes, dirty bytes, resident bytes) can show the
         * listed tables as a fraction of a connection-wide total; that is how an operator tells a
         * real heavy hitter from usage that is just spread across many tables. The decayed rankings
         * have no connection-wide total to compare against. Whether a ranking has a total is fixed
         * for that ranking, not something that varies report to report, so a reader always sees the
         * same line format for a given ranking.
         */
        has_total = true;
        switch (metric) {
        case WT_CACHE_TOP_UPDATES:
            connection_total = __wt_cache_bytes_updates(cache);
            break;
        case WT_CACHE_TOP_DIRTY:
            connection_total = __wt_cache_dirty_leaf_inuse(cache);
            break;
        case WT_CACHE_TOP_INMEM:
            connection_total = __wt_cache_bytes_inuse(cache);
            break;
        case WT_CACHE_TOP_READ:
        case WT_CACHE_TOP_EVICT:
            has_total = false;
            break;
        }

        if (has_total)
            WT_ERR(__wt_buf_fmt(session, line,
              "cache top %s: %" PRIu32 " tables above %" PRIu64 "B hold %" PRIu64 "B of %" PRIu64
              "B",
              metric_desc[metric], count, threshold, listed, connection_total));
        else
            WT_ERR(__wt_buf_fmt(session, line,
              "cache top %s: %" PRIu32 " tables above %" PRIu64 "B hold %" PRIu64 "B",
              metric_desc[metric], count, threshold, listed));
        WT_ERR(__cache_top_emit(session, force, (const char *)line->data));

        for (i = 0; i < count; ++i) {
            WT_ERR(__wt_buf_fmt(
              session, line, "    %" PRIu64 "B %s", entries[i].value, entries[i].name));
            WT_ERR(__cache_top_emit(session, force, (const char *)line->data));
        }

        __cache_top_entries_free(session, entries, count);
    }

err:
    /*
     * Every metric already processed has freed its own entries. Only the metric that was in
     * progress when something failed, if any, still has names to clean up here.
     */
    __cache_top_entries_free(session, entries, count);
    __wt_scr_free(session, &line);
    __wt_free(session, entries);
    return (ret);
}

/*
 * __wt_cache_top_report --
 *     Print all of the rankings, unconditionally. Used by WT_CONNECTION::debug_info.
 */
int
__wt_cache_top_report(WT_SESSION_IMPL *session)
{
    return (__cache_top_report(session, true));
}

/*
 * __wt_cache_top_maintain --
 *     Called periodically. Does nothing unless there is a reason to actually produce a report:
 *     verbose output for this category is on, or update bytes are over the configured target. A
 *     report always recomputes every ranking's thresholds and drops stale entries using live data,
 *     so nothing is lost by skipping a quiet tick; the first report generated after a quiet spell
 *     catches back up immediately, using whatever the connection looks like right then.
 */
int
__wt_cache_top_maintain(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t updates_target;

    conn = S2C(session);
    updates_target =
      (uint64_t)((double)conn->cache_size * conn->evict->eviction_updates_target / 100);

    if (!WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_CACHE_TOP, WT_VERBOSE_INFO) &&
      __wt_cache_bytes_updates(conn->cache) <= updates_target)
        return (0);

    return (__cache_top_report(session, false));
}

/*
 * __wti_cache_top_init --
 *     Set up the cache consumption rankings for a newly created cache.
 */
int
__wti_cache_top_init(WT_SESSION_IMPL *session)
{
    WT_CACHE_TOP *top;
    u_int metric;

    top = &S2C(session)->cache->cache_top;

    top->halflife_ticks = (uint64_t)((double)WT_CACHE_TOP_FLOW_HALFLIFE_US * (double)WT_THOUSAND *
      __wt_process.tsc_nsec_ratio);
    top->halflife_ticks = WT_MAX(top->halflife_ticks, 1);

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric)
        WT_RET(__wt_spin_init(session, &top->arrays[metric].lock, "cache top consumers"));

    return (0);
}

/*
 * __wti_cache_top_destroy --
 *     Tear down the cache consumption rankings when the cache is destroyed.
 */
void
__wti_cache_top_destroy(WT_SESSION_IMPL *session)
{
    WT_CACHE_TOP *top;
    u_int metric;

    top = &S2C(session)->cache->cache_top;

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric)
        __wt_spin_destroy(session, &top->arrays[metric].lock);
}
