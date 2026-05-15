/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block][alloc]: block_ext.c
 *
 * Tests for the compact-allocator-divergence two-branch routing in __wti_block_alloc:
 *
 *  - Threshold == 0: today's best-fit on the full avail list.
 *  - Threshold set: compact is running; all callers (including compact's own relocations) use
 *    restricted best-fit within [0, threshold), falling back to full-list best-fit then
 *    __block_extend. old_offset is not consulted.
 *
 * These tests encode the design contract from the compact-allocator-divergence design spec.
 */

#include <catch2/catch.hpp>
#include <cstring>
#include <filesystem>
#include <memory>
#include <utility>
#include <vector>

#include "wt_internal.h"
#include "../util_block.h"
#include "../utils_extlist.h"
#include "../../wrappers/mock_session.h"

namespace {

const std::string ALLOCATION_SIZE = "512";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";

/*
 * Build a WT_BM with a real WT_BLOCK, populate block->live.avail with the supplied (off, size)
 * entries, set block->size and block->compact_first_fit_threshold, and acquire block->live_lock so
 * the spinlock ownership assertion in __wti_block_alloc passes.
 *
 * Caller must invoke teardown_block() to release the lock, close the block, and drop the underlying
 * file.
 */
struct test_block_ctx {
    std::shared_ptr<mock_session> session;
    WT_BM bm;
    std::string file_path;
};

void
setup_block_with_avail(test_block_ctx &ctx, const std::vector<std::pair<wt_off_t, wt_off_t>> &avail,
  wt_off_t threshold, wt_off_t file_size)
{
    ctx.session = mock_session::build_test_mock_session();
    auto path = std::filesystem::current_path();
    /*
     * Use a unique-ish file name per test case to avoid colliding with leftovers from any earlier
     * crashed test. setup_bm + block_manager_create will fail if the file already exists, so wipe
     * up front.
     */
    ctx.file_path = path.string() + "/test_block_alloc_threshold.wt";
    std::filesystem::remove(ctx.file_path);

    /*
     * WT_BM contains a WT_RWLOCK that cannot be copy-assigned in C++, so memset the memory directly
     * instead of using struct-initializer assignment. setup_bm() will memset again internally
     * before populating the methods, so this is just defensive.
     */
    std::memset(&ctx.bm, 0, sizeof(ctx.bm));
    setup_bm(ctx.session, &ctx.bm, ctx.file_path, ALLOCATION_SIZE, BLOCK_ALLOCATION, OS_CACHE_MAX,
      OS_CACHE_DIRTY_MAX, ACCESS_PATTERN);

    WT_SESSION_IMPL *s = ctx.session->get_wt_session_impl();
    WT_BLOCK *block = ctx.bm.block;

    /*
     * Insert avail entries via the by-size insertion path so both block->live.avail.off and
     * block->live.avail.sz skiplists are populated. __wti_block_alloc reads both.
     */
    for (const auto &entry : avail) {
        REQUIRE(__ut_block_off_insert(s, &block->live.avail, entry.first, entry.second) == 0);
    }

    block->size = file_size;
    block->compact_first_fit_threshold = threshold;

    /* __wti_block_alloc asserts the live_lock is owned. */
    __wt_spin_lock(s, &block->live_lock);
}

void
teardown_block(test_block_ctx &ctx)
{
    WT_SESSION_IMPL *s = ctx.session->get_wt_session_impl();
    __wt_spin_unlock(s, &ctx.bm.block->live_lock);
    REQUIRE(__wt_block_close(s, ctx.bm.block) == 0);
    REQUIRE(__wt_block_manager_drop(s, ctx.file_path.c_str(), false) == 0);
}

} // namespace

TEST_CASE("Block alloc threshold: threshold=0 falls through to today's best-fit", "[block][alloc]")
{
    test_block_ctx ctx;
    /*
     * File: [0, 32768). No threshold. Best-fit for 4096 should pick the 4096-sized extent at offset
     * 4096 (lowest-offset entry of the smallest-fit size class).
     */
    setup_block_with_avail(
      ctx, {{4096, 4096}, {8192, 8192}, {16384, 4096}}, /*threshold=*/0, /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              WT_BLOCK_INVALID_OFFSET) == 0);
    REQUIRE(offset == 4096);

    teardown_block(ctx);
}

TEST_CASE(
  "Block alloc threshold: old_offset >= threshold still uses restricted-best-fit", "[block][alloc]")
{
    test_block_ctx ctx;
    /*
     * File: [0, 32768). Threshold T=16384. Avail: (4096,4096), (8192,8192), (20480,4096). Old
     * offset 24576 >= T: old_offset is ignored; restricted best-fit in [0, T) picks the
     * smallest-fit extent => offset 4096. Same outcome as the concurrent-write test.
     */
    setup_block_with_avail(ctx, {{4096, 4096}, {8192, 8192}, {20480, 4096}}, /*threshold=*/16384,
      /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/24576) == 0);
    /* Restricted best-fit in [0,16384): smallest-fit class has entry at offset 4096. */
    REQUIRE(offset == 4096);

    teardown_block(ctx);
}

TEST_CASE(
  "Block alloc threshold: old_offset < threshold takes restricted-best-fit", "[block][alloc]")
{
    test_block_ctx ctx;
    /* Same avail. Old offset 8192 < T=16384: concurrent-write path, best-fit in [0, T). */
    setup_block_with_avail(ctx, {{4096, 4096}, {8192, 8192}, {20480, 4096}}, /*threshold=*/16384,
      /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/8192) == 0);
    /*
     * Best-fit for 4096 picks the size-4096 entry. There are two size-4096 entries; the by-size
     * skiplist groups by size class, with the lowest-offset element first, so we get offset 4096.
     */
    REQUIRE(offset == 4096);

    teardown_block(ctx);
}

TEST_CASE(
  "Block alloc threshold: old_offset == INVALID is treated as concurrent write", "[block][alloc]")
{
    test_block_ctx ctx;
    setup_block_with_avail(ctx, {{4096, 4096}, {8192, 8192}, {20480, 4096}}, /*threshold=*/16384,
      /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              WT_BLOCK_INVALID_OFFSET) == 0);
    /* Same outcome as old_offset < threshold: best-fit in [0, threshold) picks offset 4096. */
    REQUIRE(offset == 4096);

    teardown_block(ctx);
}

TEST_CASE("Block alloc threshold: low region full, falls back to high region", "[block][alloc]")
{
    test_block_ctx ctx;
    /*
     * Avail in low region is too small to satisfy size=8192. High region has a fit at offset
     * 20480. With threshold=16384, restricted best-fit in [0, T) finds nothing >=8192, then
     * falls back to the full-list best-fit which picks 20480. old_offset is ignored.
     */
    setup_block_with_avail(
      ctx, {{4096, 4096}, {20480, 8192}}, /*threshold=*/16384, /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 8192,
              /*old_offset=*/24576) == 0);
    REQUIRE(offset == 20480);

    teardown_block(ctx);
}

TEST_CASE("Block alloc threshold: no fit anywhere extends the file", "[block][alloc]")
{
    test_block_ctx ctx;
    /* Empty avail; allocator must call __block_extend at end of file. */
    setup_block_with_avail(ctx, {}, /*threshold=*/16384, /*file_size=*/32768);

    wt_off_t offset = 0;
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/8192) == 0);
    REQUIRE(offset == 32768);

    teardown_block(ctx);
}

TEST_CASE(
  "Block alloc threshold: threshold update mid-run takes effect on next call", "[block][alloc]")
{
    test_block_ctx ctx;
    setup_block_with_avail(ctx, {{4096, 4096}, {8192, 8192}, {20480, 4096}}, /*threshold=*/8192,
      /*file_size=*/32768);

    wt_off_t offset = 0;
    /* Call 1: threshold=8192, old=12288 (ignored) => restricted best-fit in [0, 8192). */
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/12288) == 0);
    REQUIRE(offset == 4096);

    /* Refresh: widen the threshold (still under live_lock). */
    ctx.bm.block->compact_first_fit_threshold = 24576;

    /*
     * Call 2: threshold=24576, old=12288 => restricted-best-fit in [0, 24576). Available now (after
     * Call 1 removed the 4096-sized at offset 4096): (8192,8192), (20480,4096). Best-fit-by-size
     * picks the smallest-fitting class (4096) which has only one entry at offset 20480.
     */
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/12288) == 0);
    REQUIRE(offset == 20480);

    teardown_block(ctx);
}

TEST_CASE(
  "Block alloc threshold: threshold cleared mid-run reverts to today's best-fit", "[block][alloc]")
{
    test_block_ctx ctx;
    setup_block_with_avail(
      ctx, {{4096, 4096}, {20480, 8192}}, /*threshold=*/16384, /*file_size=*/32768);

    /* Clear the threshold. */
    ctx.bm.block->compact_first_fit_threshold = 0;

    wt_off_t offset = 0;
    /* old_offset is ignored when threshold=0. Best-fit picks the 4096-sized entry. */
    REQUIRE(__wti_block_alloc(ctx.session->get_wt_session_impl(), ctx.bm.block, &offset, 4096,
              /*old_offset=*/24576) == 0);
    REQUIRE(offset == 4096);

    teardown_block(ctx);
}
