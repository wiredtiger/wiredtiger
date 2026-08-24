/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * The rankings answer "which tables are holding the cache" without walking either the cache or the
 * data handle list: a tree enters a ranking from the accounting path when it grows past a
 * threshold, and the report is a pass over the slots. Deployments with millions of tables are the
 * reason: any approach whose cost scales with the number of open handles is unusable on them.
 */

/*
 * Reported entry, populated under the list lock and formatted after dropping it. Names are copied
 * rather than referenced so the report can format them without holding the lock; the buffer is
 * generous because a truncated name loses the digits that distinguish one table from another.
 */
#define WT_CACHE_TOP_NAME_MAX 256
struct __wt_cache_top_report_entry {
    char name[WT_CACHE_TOP_NAME_MAX];
    uint64_t value;
};
typedef struct __wt_cache_top_report_entry WT_CACHE_TOP_REPORT_ENTRY;

/*
 * __cache_top_threshold --
 *     The value a tree must reach to be tracked.
 */
static uint64_t
__cache_top_threshold(WT_SESSION_IMPL *session, WT_CACHE_TOP_LIST *list)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t threshold;
    uint32_t trees;

    conn = S2C(session);

    threshold = __wt_atomic_load_uint64_relaxed(&list->threshold);
    if (threshold != 0)
        return (threshold);

    /*
     * The cache divided by the slot count is the largest threshold that can be justified: no more
     * than that many trees can be above it. On a deployment with far more trees than slots the
     * largest tree is nowhere near that big, so start from a multiple of the average resident tree
     * instead and let the adjustment at report time take it from there. Both terms are counters, so
     * neither costs a walk.
     */
    trees = __wt_atomic_load_uint32_relaxed(&conn->open_btree_count);
    threshold = conn->cache_size / WT_CACHE_TOP_SLOTS;
    if (trees > WT_CACHE_TOP_SLOTS)
        threshold = WT_MIN(threshold, (__wt_cache_bytes_inuse(conn->cache) / trees) * 8);
    threshold = WT_MAX(threshold, WT_MEGABYTE);

    /* Publish it: paths that only react to a threshold already in force depend on seeing one. */
    __wt_atomic_store_uint64_relaxed(&list->threshold, threshold);

    return (threshold);
}

/*
 * __cache_top_flow_fields --
 *     The decayed value and its clock for a ranking that tracks a flow, NULL for one that tracks a
 *     level.
 */
static void
__cache_top_flow_fields(
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
    case WT_CACHE_TOP_METRICS:
        *valuep = *clockp = NULL;
        break;
    }
}

/*
 * __cache_top_decay --
 *     Apply the time decay for a ranking that tracks a flow. Decaying is what bounds the flow by
 *     the cache size, which the completeness of the ranking depends on.
 *
 * Decay moves in whole half-lives, so the caller must adopt the returned clock rather than the
 *     current one: advancing the clock past time that has not yet been charged would leave a tree
 *     touched more often than a half-life never decaying at all, and its value would grow without
 *     bound. Callers that only read pass NULL and leave the carried time in place.
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

    return (halvings >= 64 ? 0 : value >> halvings);
}

/*
 * __cache_top_value --
 *     Read a tree's current value for a ranking.
 */
static uint64_t
__cache_top_value(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric)
{
    uint64_t *clockp, *valuep;

    switch (metric) {
    case WT_CACHE_TOP_UPDATES:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_updates));
    case WT_CACHE_TOP_DIRTY:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_dirty_leaf));
    case WT_CACHE_TOP_INMEM:
        return (__wt_atomic_load_uint64_relaxed(&btree->bytes_inmem));
    case WT_CACHE_TOP_READ:
    case WT_CACHE_TOP_EVICT:
        __cache_top_flow_fields(btree, metric, &valuep, &clockp);
        return (__cache_top_decay(session, __wt_atomic_load_uint64_relaxed(valuep),
          __wt_atomic_load_uint64_relaxed(clockp), __wt_clock(session), NULL));
    case WT_CACHE_TOP_METRICS:
        break;
    }

    return (0);
}

/*
 * __cache_top_recheck_at_set --
 *     Set the value at which a tree is next considered for a ranking. Coming back only once the
 *     tree has grown by a fraction of the threshold is what bounds the number of visits a tree
 *     costs over its lifetime, however many bytes flow through it. Racing writers here are two
 *     threads storing two plausible values, and the loser only means one extra visit.
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
 *     Find an unused slot, or failing that the slot holding the smallest value, refreshing the
 *     values as we go.
 */
static uint32_t
__cache_top_smallest(WT_SESSION_IMPL *session, WT_CACHE_TOP_LIST *list, WT_CACHE_TOP_METRIC metric)
{
    uint32_t i, smallest;

    smallest = 0;
    for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i) {
        if (list->slots[i].btree == NULL)
            return (i);
        list->slots[i].value = __cache_top_value(session, list->slots[i].btree, metric);
        if (list->slots[i].value < list->slots[smallest].value)
            smallest = i;
    }

    return (smallest);
}

/*
 * __wt_cache_top_track --
 *     Consider a tree for a ranking. Called from the accounting path once the tree has grown to the
 *     value it asked to be rechecked at, so the cost of the common case is the caller's comparison,
 *     not this function.
 */
void
__wt_cache_top_track(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, uint64_t value)
{
    WT_CACHE_TOP_LIST *list;
    uint64_t threshold;
    uint32_t i, slot;

    /*
     * Metadata and the history store are tracked by the connection-level statistics and are never
     * what an operator is looking for here. The exclusion is a permanent property of the tree, so
     * record it as a recheck value no counter can reach: the history store is among the hottest
     * trees under exactly the pressure this reporting targets, and it must not pay this call on
     * every increment.
     */
    if (WT_IS_ANY_METADATA(btree->dhandle) || WT_IS_HS(btree->dhandle)) {
        __wt_atomic_store_uint64_relaxed(&btree->cache_top_recheck_at[metric], UINT64_MAX);
        return;
    }

    list = &S2C(session)->cache->cache_top.lists[metric];
    threshold = __cache_top_threshold(session, list);

    /*
     * A tree below the threshold has nothing to say to the ranking, and the value it wants to be
     * rechecked at is its own. That is the overwhelmingly common visit, so keep it off the list
     * lock entirely.
     */
    if (value < threshold) {
        __cache_top_recheck_at_set(btree, metric, threshold, value);
        return;
    }

    __wt_spin_lock(session, &list->lock);

    /* Already tracked: refresh the value in place. */
    for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i)
        if (list->slots[i].btree == btree) {
            list->slots[i].value = value;
            goto done;
        }

    slot = __cache_top_smallest(session, list, metric);
    if (list->slots[slot].btree != NULL && list->slots[slot].value >= value)
        goto done;

    if (list->slots[slot].btree != NULL)
        __wt_atomic_store_uint8_relaxed(&list->slots[slot].btree->cache_top_tracked[metric], 0);
    list->slots[slot].btree = btree;
    list->slots[slot].value = value;
    __wt_atomic_store_uint8_relaxed(&btree->cache_top_tracked[metric], 1);

done:
    __cache_top_recheck_at_set(btree, metric, threshold, value);

    __wt_spin_unlock(session, &list->lock);
}

/*
 * __cache_top_levels_refresh --
 *     Reconsider a tree's standing in the rankings that track a level, after a threshold change
 *     left the tree asking to be rechecked at a value it no longer has to reach. Growth is what
 *     normally earns a tree a look, so one that stopped growing before the threshold dropped would
 *     otherwise stay invisible; that it is being evicted from is the signal that it is still
 *     resident.
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

        /* A tree already in the ranking has nothing to gain, and asking would cost a lock. */
        if (__wt_atomic_load_uint8_relaxed(&btree->cache_top_tracked[metric]) != 0)
            continue;

        /* The maximum marks a tree excluded from the rankings, not one that is behind. */
        recheck_at = __wt_atomic_load_uint64_relaxed(&btree->cache_top_recheck_at[metric]);
        if (recheck_at == UINT64_MAX)
            continue;

        threshold = __wt_atomic_load_uint64_relaxed(&top->lists[metric].threshold);
        if (threshold != 0 && recheck_at > threshold)
            __wt_atomic_store_uint64_relaxed(&btree->cache_top_recheck_at[metric], threshold);
    }
}

/*
 * __wt_cache_top_flow_incr --
 *     Account for bytes flowing into or out of the cache for a tree.
 */
void
__wt_cache_top_flow_incr(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_CACHE_TOP_METRIC metric, size_t size)
{
    uint64_t clock, now, value, *clockp, *valuep;

    if (btree == NULL)
        return;

    __cache_top_flow_fields(btree, metric, &valuep, &clockp);
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
 * __wt_cache_top_btree_discard --
 *     Stop tracking a tree. Must be called before the tree's memory is freed.
 */
void
__wt_cache_top_btree_discard(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_CACHE *cache;
    WT_CACHE_TOP_LIST *list;
    uint32_t i;
    u_int metric;

    if ((cache = S2C(session)->cache) == NULL)
        return;

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric) {
        list = &cache->cache_top.lists[metric];
        __wt_spin_lock(session, &list->lock);
        for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i)
            if (list->slots[i].btree == btree) {
                __wt_atomic_store_uint8_relaxed(&btree->cache_top_tracked[metric], 0);
                list->slots[i].btree = NULL;
                list->slots[i].value = 0;
            }
        __wt_spin_unlock(session, &list->lock);
    }
}

/*
 * __cache_top_snapshot --
 *     Copy a ranking's live values out from under its lock, dropping trees that have fallen below
 *     the threshold, and adjust the threshold to keep the ranking usefully full.
 */
static void
__cache_top_snapshot(WT_SESSION_IMPL *session, WT_CACHE_TOP_METRIC metric,
  WT_CACHE_TOP_REPORT_ENTRY *entries, uint32_t *countp, uint64_t *thresholdp)
{
    WT_CACHE_TOP_LIST *list;
    WT_CACHE_TOP_REPORT_ENTRY tmp;
    WT_DATA_HANDLE *dhandle;
    uint64_t in_force, smallest, threshold, value;
    uint32_t count, i, j;
    const char *name;

    list = &S2C(session)->cache->cache_top.lists[metric];
    in_force = threshold = __cache_top_threshold(session, list);
    count = 0;
    smallest = UINT64_MAX;

    __wt_spin_lock(session, &list->lock);
    for (i = 0; i < WT_CACHE_TOP_SLOTS; ++i) {
        if (list->slots[i].btree == NULL)
            continue;

        /*
         * A dropped or closed table keeps its handle until sweep gets to it. Naming a table that is
         * no longer holding the cache, or no longer exists at all, is worse than saying nothing
         * about it.
         */
        dhandle = list->slots[i].btree->dhandle;
        value = !F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
            F_ISSET(dhandle, WT_DHANDLE_DEAD | WT_DHANDLE_DROPPED) ?
          0 :
          __cache_top_value(session, list->slots[i].btree, metric);
        if (value < threshold) {
            __wt_atomic_store_uint8_relaxed(&list->slots[i].btree->cache_top_tracked[metric], 0);
            list->slots[i].btree = NULL;
            list->slots[i].value = 0;
            continue;
        }
        list->slots[i].value = value;
        smallest = WT_MIN(smallest, value);

        name = list->slots[i].btree->dhandle->name;
        WT_IGNORE_RET(__wt_snprintf(entries[count].name, sizeof(entries[count].name), "%s", name));
        entries[count].value = value;
        ++count;
    }

    /*
     * Keep the threshold close to the smallest table worth ranking. A full ranking gives us the
     * answer directly: raise the bar just past its smallest entry and the bar sits where the data
     * is, which also keeps the revisit rate down. A ranking nothing has reached says nothing at
     * all, so drop the bar hard; a sparse one drops it gently, and either way a ranking only
     * refills as trees grow into it.
     */
    if (count == WT_CACHE_TOP_SLOTS)
        threshold = smallest + 1;
    else if (count == 0)
        threshold = WT_MAX(threshold / 8, WT_MEGABYTE);
    else if (count < WT_CACHE_TOP_SLOTS / 2)
        threshold = WT_MAX(threshold / 2, WT_MEGABYTE);
    __wt_atomic_store_uint64_relaxed(&list->threshold, threshold);
    __wt_spin_unlock(session, &list->lock);

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
}

/*
 * __cache_top_emit --
 *     Emit one report line, to the log when the report was asked for and to the verbose category
 *     when it was volunteered.
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
 *     Report the rankings.
 */
static int
__cache_top_report(WT_SESSION_IMPL *session, bool force, bool emit)
{
    static const char *metric_desc[] = {"update bytes", "dirty leaf bytes", "total cache bytes",
      "recent bytes read", "recent bytes evicted"};
    static_assert(WT_ELEMENTS(metric_desc) == WT_CACHE_TOP_METRICS,
      "every cache-consumer ranking needs a description");

    WT_CACHE *cache;
    WT_CACHE_TOP_REPORT_ENTRY *entries;
    WT_DECL_RET;
    uint64_t listed, threshold;
    uint64_t connection_total = 0;
    u_int metric;
    uint32_t count, i;
    char line[WT_CACHE_TOP_NAME_MAX + 128];
    bool has_total;

    cache = S2C(session)->cache;
    WT_RET(__wt_calloc_def(session, WT_CACHE_TOP_SLOTS, &entries));

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric) {
        __cache_top_snapshot(session, (WT_CACHE_TOP_METRIC)metric, entries, &count, &threshold);

        if (!emit)
            continue;

        for (i = 0, listed = 0; i < count; ++i)
            listed += entries[i].value;

        /*
         * Rankings that track a level can say what fraction of the connection total the listed
         * tables account for, which is how an operator tells a heavy hitter from usage that is
         * simply spread thin. The decayed rankings have no connection-wide equivalent. Which of the
         * two lines a ranking reports is a property of the ranking, not of the numbers, so that a
         * reader never has to handle both shapes for the same ranking.
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
        case WT_CACHE_TOP_METRICS:
            has_total = false;
            break;
        }

        if (has_total)
            WT_ERR(__wt_snprintf(line, sizeof(line),
              "cache top %s: %" PRIu32 " tables above %" PRIu64 "B hold %" PRIu64 "B of %" PRIu64
              "B",
              metric_desc[metric], count, threshold, listed, connection_total));
        else
            WT_ERR(__wt_snprintf(line, sizeof(line),
              "cache top %s: %" PRIu32 " tables above %" PRIu64 "B hold %" PRIu64 "B",
              metric_desc[metric], count, threshold, listed));
        WT_ERR(__cache_top_emit(session, force, line));

        for (i = 0; i < count; ++i) {
            WT_ERR(__wt_snprintf(
              line, sizeof(line), "    %" PRIu64 "B %s", entries[i].value, entries[i].name));
            WT_ERR(__cache_top_emit(session, force, line));
        }
    }

err:
    __wt_free(session, entries);
    return (ret);
}

/*
 * __wt_cache_top_report --
 *     Report the rankings unconditionally, for WT_CONNECTION::debug_info.
 */
int
__wt_cache_top_report(WT_SESSION_IMPL *session)
{
    return (__cache_top_report(session, true, true));
}

/*
 * __wt_cache_top_maintain --
 *     Periodic maintenance. The rankings are aged and their thresholds adjusted on every pass,
 *     whether or not anyone is listening, because a threshold that never adjusts leaves an
 *     on-demand report answering against a bar that was set when the connection was idle. The
 *     report is emitted under update pressure as well as on request, so an incident does not depend
 *     on someone having enabled verbose output in advance.
 */
int
__wt_cache_top_maintain(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t updates_target;
    bool emit;

    conn = S2C(session);
    updates_target =
      (uint64_t)((double)conn->cache_size * conn->evict->eviction_updates_target / 100);

    emit = WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_CACHE_TOP, WT_VERBOSE_INFO) ||
      __wt_cache_bytes_updates(conn->cache) > updates_target;

    return (__cache_top_report(session, false, emit));
}

/*
 * __wti_cache_top_init --
 *     Initialize the cache consumption rankings.
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
        WT_RET(__wt_spin_init(session, &top->lists[metric].lock, "cache top consumers"));

    return (0);
}

/*
 * __wti_cache_top_destroy --
 *     Discard the cache consumption rankings.
 */
void
__wti_cache_top_destroy(WT_SESSION_IMPL *session)
{
    WT_CACHE_TOP *top;
    u_int metric;

    top = &S2C(session)->cache->cache_top;

    for (metric = 0; metric < WT_CACHE_TOP_METRICS; ++metric)
        __wt_spin_destroy(session, &top->lists[metric].lock);
}
