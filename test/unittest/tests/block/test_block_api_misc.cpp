/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_api_misc]: block_mgr.c
 * This file unit tests the miscellaneous block manager API functions:
 *     - bm->addr_invalid
 *     - bm->addr_string
 *     - bm->block_header
 *     - bm->is_mapped
 *     - bm->size
 *     - bm->stat
 */

#include <catch2/catch.hpp>
#include <string>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"

TEST_CASE("Block manager invalid address", "[block_api_misc]")
{
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_BLOCK b;
    b.allocsize = 2;
    b.objectid = 5;

    // Pack all the stuff into an address cookie.
    uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
    pp = p;
    REQUIRE(__wt_block_addr_pack(&b, &pp, WT_TIERED_OBJECTID_NONE, 10, 4, 12345) == 0);
    size_t addr_size = WT_PTRDIFF(pp, p);

    WT_BM bm;
    WT_BM *bmp;
    bmp = &bm;
    __wt_bm_method_set(bmp, false);
    bmp->block = &b;
    bmp->is_live = false;

    WT_BLOCK_CKPT ci;
    WT_CLEAR(ci);
    bmp->block->live = ci;
    REQUIRE(__wt_spin_init(session->get_wt_session_impl(), &b.live_lock, "block manager") == 0);
    REQUIRE(__wti_block_ckpt_init(session->get_wt_session_impl(), &bmp->block->live, "live") == 0);
    SECTION("Test valid address")
    {
        pp = p;
        REQUIRE(bmp->addr_invalid(bmp, session->get_wt_session_impl(), pp, addr_size) == 0);
    }

    SECTION("Test addr string")
    {
        WT_ITEM buf;
        WT_CLEAR(buf);
        pp = p;
        REQUIRE(bmp->addr_string(bmp, session->get_wt_session_impl(), &buf, pp, addr_size) == 0);
        CHECK(static_cast<std::string>(((char *)(buf.data))).compare("[0: 10-14, 4, 12345]") == 0);
        __wt_free(session->get_wt_session_impl(), buf.data);
    }

    SECTION("Test address past end of file")
    {
        b.objectid = 0;
        b.size = 10;
        pp = p;
        REQUIRE(bmp->addr_invalid(bmp, session->get_wt_session_impl(), pp, addr_size) == EINVAL);
    }

    __wti_block_ckpt_destroy(session->get_wt_session_impl(), &bmp->block->live);
}

TEST_CASE("Block header", "[block_api_misc]")
{
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    REQUIRE(bmp->block_header(bmp) == (u_int)WT_BLOCK_HEADER_SIZE);
}

TEST_CASE("Block manager is mapped", "[block_api_misc]")
{
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    SECTION("Test block manager is mapped")
    {
        uint8_t i;
        bmp->map = &i;
        REQUIRE(bmp->is_mapped(bmp, NULL) == true);
    }

    SECTION("Test block manager is not mapped")
    {
        bmp->map = NULL;
        REQUIRE(bmp->is_mapped(bmp, NULL) == false);
    }
}

TEST_CASE("Block manager size", "[block_api_misc]")
{
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    WT_BLOCK b1;
    WT_BLOCK b2;
    b1.size = 10;
    b2.size = 20;

    // Test that block manager returns the expected size.
    wt_off_t result;
    bmp->block = &b1;
    REQUIRE(bmp->size(bmp, NULL, &result) == 0);
    CHECK(result == 10);

    // Test that block manager returns the correct size once its block is updated.
    bmp->block = &b2;
    REQUIRE(bmp->size(bmp, NULL, &result) == 0);
    CHECK(result == 20);
}

TEST_CASE("Block manager stat", "[block_api_misc]")
{
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    WT_BLOCK_CKPT ci;
    WT_CLEAR(ci);
    ci.ckpt_size = 1212;
    WT_EXTLIST l;
    WT_CLEAR(l);
    ci.avail = l;
    ci.avail.bytes = 398;

    WT_BLOCK b;
    b.allocsize = 2;
    b.live = ci;
    b.size = 2543;
    bmp->block = &b;

    WT_DSRC_STATS stats;

    S2C(session->get_wt_session_impl())->stat_flags = 1;
    REQUIRE(bmp->stat(bmp, session->get_wt_session_impl(), &stats) == 0);
    CHECK(stats.allocation_size == b.allocsize);
    CHECK(stats.block_checkpoint_size == (int64_t)b.live.ckpt_size);
    CHECK(stats.block_magic == WT_BLOCK_MAGIC);
    CHECK(stats.block_major == WT_BLOCK_MAJOR_VERSION);
    CHECK(stats.block_minor == WT_BLOCK_MINOR_VERSION);
    CHECK(stats.block_reuse_bytes == (int64_t)b.live.avail.bytes);
    CHECK(stats.block_size == b.size);
}
