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

/*
 * Test and validate the bm->write_size() function.
 */
void
test_and_validate_write_size(WT_BM *bm, std::shared_ptr<mock_session> session, const size_t size)
{
    size_t ret_size = size;
    // This function internally reads and changes the variable.
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &ret_size) == 0);
    CHECK(ret_size % std::stoi(ALLOCATION_SIZE) == 0);
    CHECK(ret_size == ((size / std::stoi(ALLOCATION_SIZE)) + 1) * std::stoi(ALLOCATION_SIZE));
}

/*
 * Initialize a write buffer to perform bm->write().
 */
void
create_write_buffer(WT_BM *bm, std::shared_ptr<mock_session> session, std::string contents,
  WT_ITEM *buf, size_t buf_memsize)
{
    // Fetch write buffer size from block manager.
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &buf_memsize) == 0);
    test_and_validate_write_size(bm, session, buf_memsize);

    // Initialize the buffer with aligned size.
    F_SET(buf, WT_ITEM_ALIGNED);
    REQUIRE(__wt_buf_initsize(session->get_wt_session_impl(), buf, buf_memsize) == 0);

    /*
     * Copy content string into the buffer.
     *
     * Following the architecture guide, it seems that the block manager expects a block header. I
     * have tried to mimic that here.
     */
    REQUIRE(__wt_buf_grow_worker(session->get_wt_session_impl(), buf, buf->size) == 0);
    memcpy(WT_BLOCK_HEADER_BYTE(buf->mem), contents.c_str(), contents.length());
}

void
check_bm_stats(WT_SESSION_IMPL *session, WT_BM *bm)
{
    WT_DSRC_STATS stats;

    // Enable statistics on the connection to allow the statistic macros to update correctly.
    S2C(session)->stat_flags = 1;
    REQUIRE(bm->stat(bm, session, &stats) == 0);
    CHECK(stats.allocation_size == bm->block->allocsize);
    CHECK(stats.block_checkpoint_size == (int64_t)bm->block->live.ckpt_size);
    CHECK(stats.block_magic == WT_BLOCK_MAGIC);
    CHECK(stats.block_major == WT_BLOCK_MAJOR_VERSION);
    CHECK(stats.block_minor == WT_BLOCK_MINOR_VERSION);
    CHECK(stats.block_reuse_bytes == (int64_t)bm->block->live.avail.bytes);
    CHECK(stats.block_size == bm->block->size);

    // Disable statistics on the connection when finished so that the mock session destructor
    // doesn't try to dereference invalid memory.
    S2C(session)->stat_flags = 0;
}

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

TEST_CASE("Block manager size and stat", "[block_api_misc]")
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

    check_bm_stats(s, &bm);

    // Perform a write.
    WT_ITEM buf;
    WT_CLEAR(buf);
    std::string test_string("test123");
    create_write_buffer(&bm, session, test_string, &buf, 0);
    uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];
    size_t addr_size;
    wt_off_t bm_size;
    REQUIRE(bm.write(&bm, s, &buf, addr, &addr_size, false, false) == 0);
    REQUIRE(bm.size(&bm, s, &bm_size) == 0);

    check_bm_stats(s, &bm);

    REQUIRE(__wt_block_close(s, bm.block) == 0);
}
