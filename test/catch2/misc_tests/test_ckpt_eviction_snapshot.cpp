/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include <filesystem>

#include "wiredtiger.h"
#include "../wrappers/connection_wrapper.h"
#include "wt_internal.h"

/*
 * Under precise checkpoints eviction bounds itself with the snapshot the running checkpoint
 * publishes. The checkpoint generation is bumped before that snapshot is published, so for an
 * interval at the start of every checkpoint the published buffer still holds the previous
 * checkpoint's snapshot, whose ids can sit entirely below the global oldest id. Adopting it makes
 * every update in the tree look invisible. These cases cover the stamp comparison that tells the
 * two apart.
 */

static constexpr const char *k_db = "test_db_ckpt_eviction_snapshot";

/* Publish a snapshot into the inactive buffer, stamped with the given generation. */
static void
publish(WT_CONNECTION_IMPL *conn, uint64_t gen)
{
    uint32_t new_idx = 1 - conn->ckpt_eviction_snap_idx;

    conn->ckpt_eviction_snap[new_idx].snap.snap_min = 100;
    conn->ckpt_eviction_snap[new_idx].snap.snap_max = 200;
    conn->ckpt_eviction_snap[new_idx].snap.snapshot_count = 0;
    conn->ckpt_eviction_snap[new_idx].gen = gen;
    conn->ckpt_eviction_snap_idx = new_idx;
    conn->ckpt_eviction_snap_published = true;
}

/* Check if a page being evicted adopts the published snapshot. */
static bool
adoptable(WT_SESSION_IMPL *session, uint64_t ckpt_gen)
{
    return (__wt_ckpt_eviction_snap_current(session, ckpt_gen) != nullptr);
}

/* The snapshot the index currently points at, ignoring its stamp. */
static WT_TXN_SNAPSHOT *
published(WT_CONNECTION_IMPL *conn)
{
    return (&conn->ckpt_eviction_snap[conn->ckpt_eviction_snap_idx].snap);
}

static uint64_t
stamp(WT_CONNECTION_IMPL *conn, uint32_t snap_idx)
{
    return (conn->ckpt_eviction_snap[snap_idx].gen);
}

/*
 * Closing a precise-checkpoint connection takes a checkpoint, which requires a stable timestamp.
 * Without one the connection wrapper's destructor throws.
 */
static void
set_stable(connection_wrapper &wrapper)
{
    WT_CONNECTION *conn = wrapper.get_wt_connection();

    REQUIRE(conn->set_timestamp(conn, "stable_timestamp=1") == 0);
}

TEST_CASE("Checkpoint eviction snapshot: a snapshot no running checkpoint published is declined",
  "[ckpt_eviction_snapshot]")
{
    std::filesystem::remove_all(k_db);
    connection_wrapper wrapper(k_db, "create,precise_checkpoint=true");
    WT_CONNECTION_IMPL *conn = wrapper.get_wt_connection_impl();
    WT_SESSION_IMPL *session = wrapper.create_session();
    set_stable(wrapper);

    SECTION("no checkpoint has ever published")
    {
        REQUIRE_FALSE(conn->ckpt_eviction_snap_published);
        REQUIRE_FALSE(adoptable(session, 47));
    }

    SECTION("the running checkpoint published it")
    {
        publish(conn, 47);
        REQUIRE(adoptable(session, 47));
    }

    SECTION("a previous checkpoint published it")
    {
        /*
         * The generation has moved to 48 but checkpoint 48 has not published yet, so the buffer is
         * still checkpoint 47's.
         */
        publish(conn, 47);
        REQUIRE_FALSE(adoptable(session, 48));
    }

    SECTION("the publishing checkpoint retired it")
    {
        publish(conn, 47);
        __ut_checkpoint_eviction_snapshot_retire(session);
        REQUIRE_FALSE(adoptable(session, 47));
    }

    SECTION("the next checkpoint has not published yet")
    {
        /*
         * The interval at the start of a checkpoint, before it takes its snapshot. Nothing is
         * published, so eviction bounds itself another way rather than adopting the last
         * checkpoint's snapshot.
         */
        publish(conn, 47);
        __ut_checkpoint_eviction_snapshot_retire(session);
        REQUIRE_FALSE(adoptable(session, 48));
    }

    SECTION("the next checkpoint published into the other buffer")
    {
        publish(conn, 47);
        uint32_t first_idx = conn->ckpt_eviction_snap_idx;
        publish(conn, 48);

        REQUIRE(conn->ckpt_eviction_snap_idx != first_idx);
        REQUIRE(adoptable(session, 48));
        /* The buffer left behind is unreachable, even though its stamp still reads 47. */
        REQUIRE_FALSE(adoptable(session, 47));
    }

    SECTION("a zero generation never matches an unpublished buffer")
    {
        REQUIRE_FALSE(adoptable(session, 0));
    }
}

TEST_CASE("Checkpoint eviction snapshot: the reader sees the published buffer's contents",
  "[ckpt_eviction_snapshot]")
{
    std::filesystem::remove_all(k_db);
    connection_wrapper wrapper(k_db, "create,precise_checkpoint=true");
    WT_CONNECTION_IMPL *conn = wrapper.get_wt_connection_impl();
    WT_SESSION_IMPL *session = wrapper.create_session();
    set_stable(wrapper);

    publish(conn, 47);

    WT_TXN_SNAPSHOT *snap = __wt_ckpt_eviction_snap_current(session, 47);
    REQUIRE(snap == published(conn));
    REQUIRE(snap->snap_min == 100);
    REQUIRE(snap->snap_max == 200);
}

TEST_CASE(
  "Checkpoint eviction snapshot: retire leaves the buffers alone", "[ckpt_eviction_snapshot]")
{
    std::filesystem::remove_all(k_db);
    connection_wrapper wrapper(k_db, "create,precise_checkpoint=true");
    WT_CONNECTION_IMPL *conn = wrapper.get_wt_connection_impl();
    WT_SESSION_IMPL *session = wrapper.create_session();
    set_stable(wrapper);

    publish(conn, 47);
    uint32_t live_idx = conn->ckpt_eviction_snap_idx;

    __ut_checkpoint_eviction_snapshot_retire(session);

    /* Only the published flag is cleared; the buffer and the index it named are untouched. */
    REQUIRE_FALSE(conn->ckpt_eviction_snap_published);
    REQUIRE(conn->ckpt_eviction_snap_idx == live_idx);
    REQUIRE(stamp(conn, live_idx) == 47);
}
