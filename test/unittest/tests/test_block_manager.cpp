/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "utils.h"
#include "wiredtiger.h"
#include "wt_internal.h"
#include "wrappers/connection_wrapper.h"

TEST_CASE("Block manager invalid address cookie", "[block_manager_invalid_addr]")
{
    uint8_t p, *pp;
    WT_BLOCK b;
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    b.allocsize = 2;
    b.objectid = 5;
    bmp->block = &b;

    pp = &p;

    // Pack all the stuff into an address cookie.
    REQUIRE(__wt_block_addr_pack(&b, &pp, WT_TIERED_OBJECTID_NONE, 10, 4, 12345) == 0);

    SECTION("Valid address cookie")
    {
        size_t addr_size = 5;
        REQUIRE(__wt_block_addr_invalid(NULL, &b, &p, addr_size, false) == 0);
    }
}

TEST_CASE("Block manager addr string", "[block_manager_addr_string]")
{
    uint8_t p, *pp;
    WT_BLOCK b;
    WT_BM bm;
    WT_BM *bmp;
    WT_ITEM buf;

    bmp = &bm;
    b.allocsize = 2;
    b.objectid = 5;
    bmp->block = &b;

    pp = &p;

    // Pack all the stuff into an address cookie.
    REQUIRE(__wt_block_addr_pack(&b, &pp, WT_TIERED_OBJECTID_NONE, 10, 4, 12345) == 0);

    SECTION("Valid address string")
    {
        size_t addr_size = 5;
        REQUIRE(__wt_block_addr_string(NULL, &b, &buf, &p, addr_size) == 0);
    }
}

TEST_CASE("Block manager header", "[block_manager_header]")
{
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    REQUIRE(bmp->block_header(bmp) == (u_int)WT_BLOCK_HEADER_SIZE);
}

// TEST_CASE("Block manager checkpoint", "[block_manager_checkpoint]") {}

TEST_CASE("Block manager checkpoint start", "[block_manager_checkpoint_start]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();

    WT_BLOCK b;
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);
    bmp->block = &b;
    //bmp->block->ckpt_state = WT_CKPT_NONE;

    // Test invalid checkpoint states.
    // SECTION("Test invalid checkpoint states") {
    //     // blah blah
    // }
    // Test valid checkpoint states.

    SECTION("Checkpoint state is none")
    {
        REQUIRE(bmp->checkpoint_start(bmp, session_impl) == 0);
        REQUIRE(bmp->block->ckpt_state == 1);
    }
}

TEST_CASE("Block manager close", "[block_manager_close]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();

    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    SECTION("Block manager close single handle")
    {
        bmp->is_multi_handle = false;
        // Add the block.
        // Then close/remove the block.
        // Depends on connection and block checkpoint states.
        REQUIRE(bmp->close(bmp, session_impl) == 0);
    }

    SECTION("Block manager close multiple handles")
    {
        bmp->is_multi_handle = true;
        // REQUIRE(bmp->close(bmp, NULL) == 0);
    }
}

TEST_CASE("Block manager is mapped", "[block_manager_mapped]")
{
    uint8_t i;
    WT_BM bm;
    WT_BM *bmp;

    bmp = &bm;
    __wt_bm_method_set(bmp, false);

    // Block manager is mapped.
    bmp->map = &i;
    REQUIRE(bmp->is_mapped(bmp, NULL) == true);

    // Block manager is not mapped.
    bmp->map = NULL;
    REQUIRE(bmp->is_mapped(bmp, NULL) == false);
}

TEST_CASE("Block manager size", "[block_manager_size]")
{
    WT_BLOCK b;
    WT_BM bm;
    WT_BM *bmp;
    wt_off_t size;

    b.size = 10;
    bmp = &bm;
    bmp->block = &b;
    __wt_bm_method_set(bmp, false);

    REQUIRE(bmp->size(bmp, NULL, &size) == 0);
    REQUIRE(size == b.size);
}

// TEST_CASE("Block manager read", "[block_manager_read]")
// {

// }
