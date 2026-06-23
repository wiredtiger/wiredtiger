/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "wrappers/mock_session.h"

/*
 * Unit tests for the root-size bookkeeping reset in the disagg block manager.
 *
 * WT_BLOCK_DISAGG handles are cached by filename (conn->blockhash) and reused across opens.
 * current_root_size and previous_root_size are per-open state: they track how much the running
 * total has been adjusted for the root page during this open. set_size resets both to 0 so a
 * subsequent root-size transition on a reused handle does not subtract a stale value left by a
 * prior open.
 *
 * The scenario under test:
 *   1. A prior open calls apply_root_size, leaving current_root_size = STALE (non-zero).
 *   2. The connection restarts; set_size(0) is called for an empty-checkpoint open.
 *   3. ckpt_load returns early because addr_size == 0: current_root_size is not updated.
 *   4. apply_root_size(0) fires: previous = STALE, decrease(STALE from 0) underflows.
 *
 * With the reset in set_size, current_root_size = 0 on step 2, so apply_root_size(0) in
 * step 4 subtracts 0 from 0, which is safe.
 */

static const uint64_t STALE_ROOT_SIZE = 1920;
static const uint32_t FRESH_ROOT_SIZE = 61;

TEST_CASE("set_size resets stale root-size bookkeeping", "[disagg][block_disagg_size]")
{
    auto session_wrapper = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = session_wrapper->get_wt_session_impl();

    SECTION(
      "stale current_root_size on a zero-size handle violates the apply_root_size precondition")
    {
        WT_BLOCK_DISAGG block_disagg{};

        /* set_size(0) WITHOUT reset: stale bookkeeping from a prior open survives. */
        (void)__wt_atomic_store_uint64(&block_disagg.size, 0);
        block_disagg.current_root_size = STALE_ROOT_SIZE;

        /*
         * ckpt_load returns early (addr_size == 0), so current_root_size stays STALE.
         * apply_root_size(0) would set previous = STALE then decrease(STALE from 0), which
         * underflows. Verify the precondition is violated without the reset.
         */
        uint64_t size = __wt_atomic_load_uint64(&block_disagg.size);
        REQUIRE(size < block_disagg.current_root_size);
    }

    SECTION("set_size reset allows apply_root_size(0) on a reused handle with empty ckpt_load")
    {
        WT_BLOCK_DISAGG block_disagg{};

        /* Simulate a prior open leaving non-zero bookkeeping. */
        block_disagg.current_root_size = STALE_ROOT_SIZE;
        block_disagg.previous_root_size = STALE_ROOT_SIZE - 60;

        /* set_size(0) WITH the reset. */
        (void)__wt_atomic_store_uint64(&block_disagg.size, 0);
        block_disagg.current_root_size = 0;
        block_disagg.previous_root_size = 0;

        /* ckpt_load returns early: current_root_size stays 0. */

        /* apply_root_size(0): previous=0, decrease(0 from 0) is safe. */
        __wti_block_disagg_apply_root_size(session, &block_disagg, 0);

        REQUIRE(__wt_atomic_load_uint64(&block_disagg.size) == 0);
        REQUIRE(block_disagg.current_root_size == 0);
        REQUIRE(block_disagg.previous_root_size == 0);
    }

    SECTION("set_size reset followed by ckpt_load and apply_root_size produces correct accounting")
    {
        WT_BLOCK_DISAGG block_disagg{};

        /* Simulate a prior open leaving non-zero bookkeeping. */
        block_disagg.current_root_size = STALE_ROOT_SIZE;
        block_disagg.previous_root_size = STALE_ROOT_SIZE - 60;

        /* set_size(0) WITH reset. */
        (void)__wt_atomic_store_uint64(&block_disagg.size, 0);
        block_disagg.current_root_size = 0;
        block_disagg.previous_root_size = 0;

        /* ckpt_load: sets current_root_size and seeds the running total (the ckpt_load bump). */
        block_disagg.current_root_size = FRESH_ROOT_SIZE;
        (void)__wti_block_disagg_increase_size(&block_disagg, FRESH_ROOT_SIZE);

        REQUIRE(__wt_atomic_load_uint64(&block_disagg.size) == FRESH_ROOT_SIZE);

        /* apply_root_size(FRESH_ROOT_SIZE): previous=FRESH_ROOT_SIZE, transition to same size. */
        __wti_block_disagg_apply_root_size(session, &block_disagg, FRESH_ROOT_SIZE);

        REQUIRE(__wt_atomic_load_uint64(&block_disagg.size) == FRESH_ROOT_SIZE);
        REQUIRE(block_disagg.current_root_size == FRESH_ROOT_SIZE);
        REQUIRE(block_disagg.previous_root_size == FRESH_ROOT_SIZE);
    }
}
