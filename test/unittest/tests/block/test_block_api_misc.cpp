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
 *     - bm->addr_string
 *     - bm->block_header
 *     - bm->is_mapped
 *     - bm->size
 *     - bm->stat
 */

#include <catch2/catch.hpp>
#include <filesystem>
#include <string>

#include "wt_internal.h"
#include "../wrappers/config_parser.h"
#include "../wrappers/mock_session.h"

const int DEFAULT_BLOCK_SIZE = 256;
const std::string ALLOCATION_SIZE = "256";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

// Tested using a white-box approach as we need knowledge of internal structures to test various
// inputs.
TEST_CASE("Block manager addr string", "[block_api_misc]")
{
    // Build a mock session, this will automatically create a mock connection.
    // Additionally, initialize a session implementation variable for easy access.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session->get_wt_session_impl();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE(session->get_mock_connection()->setup_block_manager(s) == 0);
    session->setup_block_manager_file_operations();

    // Declare a block manager and set it up so that we can use its legal API methods.
    WT_BM bm;
    WT_CLEAR(bm);
    __ut_bm_method_set(&bm);

    // Initialization steps for the block manager. We shouldn't need to touch the block checkpoint
    // logic and this shows that WiredTiger has poor module separation.
    auto path = std::filesystem::current_path();
    path += "/test.wt";
    REQUIRE(__wt_block_manager_create(s, path.c_str(), DEFAULT_BLOCK_SIZE) == 0);
    REQUIRE(__wt_block_open(s, path.c_str(), WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false,
              false, false, DEFAULT_BLOCK_SIZE, &bm.block) == 0);
    REQUIRE(__wti_block_ckpt_init(s, &bm.block->live, nullptr) == 0);

    // Initialize a buffer.
    WT_ITEM buf;
    WT_CLEAR(buf);

    // Generate an address cookie - technically, we shouldn't know about internal details of the
    // address cookie, but this allows for more rigorous testing.
    uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
    pp = p;
    // (512, 1024, 12345) -> (offset, size, checksum)
    REQUIRE(__wt_block_addr_pack(bm.block, &pp, WT_TIERED_OBJECTID_NONE, 512, 1024, 12345) == 0);
    size_t addr_size = WT_PTRDIFF(pp, p);

    // Test that the block manager's addr_string method produces the expected string representation.
    pp = p;
    REQUIRE(bm.addr_string(&bm, nullptr, &buf, pp, addr_size) == 0);
    CHECK(
      static_cast<std::string>(((char *)(buf.data))).compare("[0: 512-1536, 1024, 12345]") == 0);
    __wt_free(s, buf.data);
    REQUIRE(__wt_block_close(s, bm.block) == 0);
}

// Tested using a black-box approach.
TEST_CASE("Block header", "[block_api_misc]")
{
    // Declare a block manager and set it up so that we can use its legal API methods.
    WT_BM bm;
    WT_CLEAR(bm);
    __ut_bm_method_set(&bm);

    REQUIRE(bm.block_header(&bm) == (u_int)WT_BLOCK_HEADER_SIZE);
}

// Tested using a white-box approach as using a black-box approach would be pretty involved for the
// purposes of this test.
TEST_CASE("Block manager is mapped", "[block_api_misc]")
{
    // Declare a block manager and set it up so that we can use its legal API methods.
    WT_BM bm;
    WT_CLEAR(bm);
    __ut_bm_method_set(&bm);

    SECTION("Test block manager is mapped")
    {
        uint8_t i;
        bm.map = &i;
        REQUIRE(bm.is_mapped(&bm, nullptr) == true);
    }

    SECTION("Test block manager is not mapped")
    {
        bm.map = nullptr;
        REQUIRE(bm.is_mapped(&bm, nullptr) == false);
    }
}

TEST_CASE("Block manager stat", "[block_api_misc]")
{
    // Build a mock session, this will automatically create a mock connection.
    // Additionally, initialize a session implementation variable for easy access.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session->get_wt_session_impl();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE(session->get_mock_connection()->setup_block_manager(s) == 0);
    session->setup_block_manager_file_operations();

    // Declare a block manager and set it up so that we can use its legal API methods.
    WT_BM bm;
    WT_CLEAR(bm);
    __ut_bm_method_set(&bm);

    // Initialization steps for the block manager. We shouldn't need to touch the block checkpoint
    // logic and this shows that WiredTiger has poor module separation.
    auto path = std::filesystem::current_path();
    path += "/test.wt";
    REQUIRE(__wt_block_manager_create(s, path.c_str(), DEFAULT_BLOCK_SIZE) == 0);
    REQUIRE(__wt_block_open(s, path.c_str(), WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false,
              false, false, DEFAULT_BLOCK_SIZE, &bm.block) == 0);
    REQUIRE(__wti_block_ckpt_init(s, &bm.block->live, nullptr) == 0);

    WT_DSRC_STATS stats;
    S2C(s)->stat_flags = 1;
    REQUIRE(bm.stat(&bm, s, &stats) == 0);
    CHECK(stats.allocation_size == bm.block->allocsize);
    CHECK(stats.block_checkpoint_size == (int64_t)bm.block->live.ckpt_size);
    CHECK(stats.block_magic == WT_BLOCK_MAGIC);
    CHECK(stats.block_major == WT_BLOCK_MAJOR_VERSION);
    CHECK(stats.block_minor == WT_BLOCK_MINOR_VERSION);
    CHECK(stats.block_reuse_bytes == (int64_t)bm.block->live.avail.bytes);
    CHECK(stats.block_size == bm.block->size);
    REQUIRE(__wt_block_close(s, bm.block) == 0);
}
