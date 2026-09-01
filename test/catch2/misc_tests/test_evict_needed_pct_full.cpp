/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace {

/*
 * Set the cache accounting directly: the triggers are evaluated against these counters, and driving
 * them with a real workload cannot hit the exact boundaries this test needs.
 */
void
set_cache_usage(WT_SESSION_IMPL *session, double pct_inmem, double pct_dirty, double pct_updates)
{
    WT_CONNECTION_IMPL *conn = S2C(session);
    uint64_t bytes_max = conn->cache_size;

    conn->cache->overhead_pct = 0;
    __wt_atomic_store_uint64_relaxed(
      &conn->cache->bytes_inmem, (uint64_t)(bytes_max * pct_inmem / 100.0));
    __wt_atomic_store_uint64_relaxed(
      &conn->cache->bytes_dirty_leaf, (uint64_t)(bytes_max * pct_dirty / 100.0));
    __wt_atomic_store_uint64_relaxed(
      &conn->cache->bytes_updates, (uint64_t)(bytes_max * pct_updates / 100.0));
}

} // namespace

TEST_CASE("Eviction trigger check reports a full cache whenever it fires", "[evict]")
{
    connection_wrapper conn_wrapper = connection_wrapper(".", "create,cache_size=100MB");
    WT_SESSION *session;
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_EVICT *evict = S2C(session_impl)->evict;

    WT_CACHE *cache = S2C(session_impl)->cache;
    uint64_t saved_inmem = __wt_atomic_load_uint64_relaxed(&cache->bytes_inmem);
    uint64_t saved_dirty = __wt_atomic_load_uint64_relaxed(&cache->bytes_dirty_leaf);
    uint64_t saved_updates = __wt_atomic_load_uint64_relaxed(&cache->bytes_updates);
    u_int saved_overhead = cache->overhead_pct;

    evict->eviction_trigger = 95.0;
    __wt_atomic_store_double_relaxed(&evict->eviction_dirty_trigger, 20.0);
    __wt_atomic_store_double_relaxed(&evict->eviction_updates_trigger, 10.0);
    __wt_atomic_store_uint8_relaxed(
      &S2C(session_impl)->cache->cache_eviction_controls.app_eviction_min_cache_fill_ratio, 0);

    /* Cache usage as a percentage of the configured size: total, dirty leaf, updates. */
    struct {
        double inmem, dirty, updates;
    } usage[] = {
      {10.0, 1.0, 1.0},   /* Nothing over trigger. */
      {94.0, 19.0, 9.0},  /* Everything just under trigger. */
      {96.0, 1.0, 1.0},   /* Clean over trigger. */
      {50.0, 25.0, 1.0},  /* Dirty over trigger. */
      {50.0, 1.0, 15.0},  /* Updates over trigger. */
      {96.0, 25.0, 15.0}, /* All three over trigger. */
    };

    int needed_count = 0;
    for (auto &u : usage)
        for (bool busy : {false, true})
            for (bool readonly : {false, true}) {
                set_cache_usage(session_impl, u.inmem, u.dirty, u.updates);

                double pct_full = -1.0;
                bool needed = __wt_evict_needed(session_impl, busy, readonly, true, &pct_full);

                /*
                 * The reported percentage is one hundred minus the smallest margin to a trigger, so
                 * exceeding any trigger puts it at or above one hundred. Application threads
                 * therefore cannot use it to distinguish degrees of cache pressure once they are
                 * assisting with eviction.
                 */
                INFO("inmem " << u.inmem << " dirty " << u.dirty << " updates " << u.updates
                              << " busy " << busy << " readonly " << readonly);
                if (needed) {
                    REQUIRE(pct_full >= 100.0);
                    ++needed_count;
                }
            }

    /* The invariant above is only meaningful if some of these scenarios crossed a trigger. */
    REQUIRE(needed_count > 0);

    /* Closing the connection checks the accounting this test overwrote. */
    __wt_atomic_store_uint64_relaxed(&S2C(session_impl)->cache->bytes_inmem, saved_inmem);
    __wt_atomic_store_uint64_relaxed(&S2C(session_impl)->cache->bytes_dirty_leaf, saved_dirty);
    __wt_atomic_store_uint64_relaxed(&S2C(session_impl)->cache->bytes_updates, saved_updates);
    S2C(session_impl)->cache->overhead_pct = saved_overhead;
}
