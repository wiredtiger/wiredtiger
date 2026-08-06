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
#include "../../utility/test_util.h"

/*
 * Under precise checkpoints eviction bounds itself with the snapshot the running checkpoint
 * publishes. The checkpoint generation is bumped before that snapshot is published, so for an
 * interval at the start of every checkpoint the published buffer still holds the previous
 * checkpoint's snapshot, whose ids can sit entirely below the global oldest id. Adopting it makes
 * every update in the tree look invisible. These cases cover the stamp comparison that tells the
 * two apart.
 */

namespace {

/* A connection, its session, and the buffer manipulation the cases below share. */
class snapshot_test_env {
public:
    explicit snapshot_test_env(bool precise)
        : _home("WT_TEST.ckpt_eviction_snapshot"),
          _wrapper(
            (clean_home(_home), _home), precise ? "create,precise_checkpoint=true" : "create")
    {
        conn = _wrapper.get_wt_connection_impl();
        session = _wrapper.create_session();

        /*
         * Closing a precise-checkpoint connection takes a checkpoint, which requires a stable
         * timestamp. Without one the wrapper's destructor throws.
         */
        WT_CONNECTION *wt_conn = _wrapper.get_wt_connection();
        REQUIRE(wt_conn->set_timestamp(wt_conn, "stable_timestamp=1") == 0);
    }

    /* Publish a snapshot into the inactive buffer, stamped with the given generation. */
    void
    publish(uint64_t gen)
    {
        uint32_t new_idx = 1 - conn->ckpt_eviction_snap_idx;

        conn->ckpt_eviction_snap[new_idx].snap.snap_min = 100;
        conn->ckpt_eviction_snap[new_idx].snap.snap_max = 200;
        conn->ckpt_eviction_snap[new_idx].snap.snapshot_count = 0;
        conn->ckpt_eviction_snap[new_idx].gen = gen;
        conn->ckpt_eviction_snap_idx = new_idx;
    }

    /* Check if a page being evicted adopts the published snapshot */
    bool
    adoptable(uint64_t ckpt_gen)
    {
        uint32_t snap_idx = UINT32_MAX;

        return (__wt_ckpt_eviction_snap_current(session, ckpt_gen, &snap_idx));
    }

    uint64_t
    stamp(uint32_t snap_idx)
    {
        return (conn->ckpt_eviction_snap[snap_idx].gen);
    }

    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;

private:
    static void
    clean_home(const std::string &home)
    {
        testutil_system("rm -rf %s && mkdir -p %s", home.c_str(), home.c_str());
    }

    const std::string _home;
    connection_wrapper _wrapper;
};

} // namespace

TEST_CASE("Checkpoint eviction snapshot: the stamp decides whether eviction may adopt it",
  "[ckpt_eviction_snapshot]")
{
    snapshot_test_env env(true);

    SECTION("no checkpoint has ever published")
    {
        /* Both buffers are stamped zero, which no generation matches. */
        REQUIRE(env.adoptable(47) == false);
    }

    SECTION("the running checkpoint published it")
    {
        env.publish(47);
        REQUIRE(env.adoptable(47) == true);
    }

    SECTION("a previous checkpoint published it")
    {
        /*
         * The generation has moved to 48 but checkpoint 48 has not published yet, so the buffer is
         * still checkpoint 47's.
         */
        env.publish(47);
        REQUIRE(env.adoptable(48) == false);
    }

    SECTION("the publishing checkpoint retired it")
    {
        env.publish(47);
        __ut_checkpoint_eviction_snapshot_retire(env.session);
        REQUIRE(env.adoptable(47) == false);
    }

    SECTION("the next checkpoint published into the other buffer")
    {
        env.publish(47);
        uint32_t first_idx = env.conn->ckpt_eviction_snap_idx;
        env.publish(48);

        REQUIRE(env.conn->ckpt_eviction_snap_idx != first_idx);
        REQUIRE(env.adoptable(48) == true);
        /* The buffer left behind is unreachable, even though its stamp still reads 47. */
        REQUIRE(env.adoptable(47) == false);
    }

    SECTION("a zero generation never matches an unpublished buffer")
    {
        REQUIRE(env.adoptable(0) == false);
    }
}

TEST_CASE("Checkpoint eviction snapshot: the reader sees the published buffer's contents",
  "[ckpt_eviction_snapshot]")
{
    snapshot_test_env env(true);
    uint32_t snap_idx = UINT32_MAX;

    env.publish(47);

    REQUIRE(__wt_ckpt_eviction_snap_current(env.session, 47, &snap_idx) == true);
    REQUIRE(snap_idx == env.conn->ckpt_eviction_snap_idx);
    REQUIRE(env.conn->ckpt_eviction_snap[snap_idx].snap.snap_min == 100);
    REQUIRE(env.conn->ckpt_eviction_snap[snap_idx].snap.snap_max == 200);
}

TEST_CASE("Checkpoint eviction snapshot: retire clears only the published buffer",
  "[ckpt_eviction_snapshot]")
{
    snapshot_test_env env(true);

    env.publish(47);
    uint32_t first_idx = env.conn->ckpt_eviction_snap_idx;
    env.publish(48);
    uint32_t live_idx = env.conn->ckpt_eviction_snap_idx;

    __ut_checkpoint_eviction_snapshot_retire(env.session);

    REQUIRE(env.stamp(live_idx) == 0);
    REQUIRE(env.stamp(first_idx) == 47);
}

TEST_CASE("Checkpoint eviction snapshot: retire is a no-op without precise checkpoints",
  "[ckpt_eviction_snapshot]")
{
    snapshot_test_env env(false);

    env.publish(47);
    __ut_checkpoint_eviction_snapshot_retire(env.session);

    REQUIRE(env.stamp(env.conn->ckpt_eviction_snap_idx) == 47);
}
