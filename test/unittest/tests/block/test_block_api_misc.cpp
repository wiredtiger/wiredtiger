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
#include <string>

#include "wt_internal.h"
#include "../wrappers/config_parser.h"
#include "../wrappers/mock_session.h"

const int DEFAULT_BLOCK_SIZE = 512;
const std::string ALLOCATION_SIZE = "512";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

void
test_and_validate_write_size(WT_BM *bm, std::shared_ptr<mock_session> session, size_t size)
{
    size_t init_size = size;
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &size) == 0);
    CHECK(size % DEFAULT_BLOCK_SIZE == 0);
    CHECK(size == ((init_size / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE);
}

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

    // Copy content string into the buffer.
    // Following the architecture guide, it seems that the block manager expects a block header.
    // I have tried to mimic that here.
    REQUIRE(__wt_buf_grow_worker(session->get_wt_session_impl(), buf, buf->size) == 0);
    memcpy(WT_BLOCK_HEADER_BYTE(buf->mem), contents.c_str(), contents.length());
    // REQUIRE(__wt_buf_set(session->get_wt_session_impl(), buf, contents.c_str(),
    // contents.length()) == 0);
}

// Test only a basic scenario as we shouldn't have knowledge of internal structures here.
TEST_CASE("Block manager addr string", "[block_api_string]")
{
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session->get_wt_session_impl();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE(session->get_mock_connection()->setup_block_manager(s) == 0);
    WT_BM *bm;

    REQUIRE(__wt_blkcache_open(s, "file:test", cp.get_config_array(), false, false, DEFAULT_BLOCK_SIZE, &bm) == 0);

    // Pack all the stuff into an address cookie.
    uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
    pp = p;
    REQUIRE(__wt_block_addr_pack(bm->block, &pp, WT_TIERED_OBJECTID_NONE, 10, 4, 12345) == 0);
    size_t addr_size = WT_PTRDIFF(pp, p);

    SECTION("Test addr string")
    {
        WT_ITEM buf;
        WT_CLEAR(buf);
        pp = p;
        REQUIRE(__ut_bm_addr_string(bm, s, &buf, p, addr_size) == 0);
        printf("addr size: %zu\n", addr_size);
        printf("buf data string: %s\n", (char *)(buf.data));
        CHECK(static_cast<std::string>(((char *)(buf.data))).compare("[0: 10-14, 4, 12345]") == 0);
        __wt_free(s, buf.data);
    }
    bm->close(bm, s);
}

TEST_CASE("Block header", "[block_api_misc]")
{
    WT_BM bm;
    REQUIRE(__ut_bm_block_header(&bm) == (u_int)WT_BLOCK_HEADER_SIZE);
}

TEST_CASE("Block manager is mapped", "[block_api_misc]")
{
    WT_BM bm;

    SECTION("Test block manager is mapped")
    {
        uint8_t i;
        bm.map = &i;
        REQUIRE(__ut_bm_is_mapped(&bm, nullptr) == true);
    }

    SECTION("Test block manager is not mapped")
    {
        bm.map = nullptr;
        REQUIRE(__ut_bm_is_mapped(&bm, nullptr) == false);
    }
}

TEST_CASE("Block manager size and stat", "[block_api_stat]")
{
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session->get_wt_session_impl();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE(session->get_mock_connection()->setup_block_manager(s) == 0);
    session->setup_block_manager_file_operations();
    WT_BM *bm;
    REQUIRE(__wt_blkcache_open(s, "file:test", cp.get_config_array(), false, false, DEFAULT_BLOCK_SIZE, &bm) == 0);

    // Load the checkpoint as its required to perform a block write.
    size_t root_addr_size;
    bm->checkpoint_load(bm, s, NULL, 0, NULL, &root_addr_size, false);
    WT_UNUSED(root_addr_size);

    WT_DSRC_STATS stats;

    // TODO: Enable statistics on the connection.
    // session->get_mock_connection()->enable_statistics(s);
    S2C(session->get_wt_session_impl())->stat_flags = 1;
    // FLD_SET(S2C(s)->stat_flags, WT_STAT_TYPE_ALL);
    REQUIRE(__ut_bm_stat(bm, s, &stats) == 0);
    // CHECK(stats.allocation_size == bm->block->allocsize);
    // CHECK(stats.block_checkpoint_size == bm->block->live.ckpt_size);
    // CHECK(stats.block_magic == WT_BLOCK_MAGIC);
    // CHECK(stats.block_major == WT_BLOCK_MAJOR_VERSION);
    // CHECK(stats.block_minor == WT_BLOCK_MINOR_VERSION);
    // CHECK(stats.block_reuse_bytes == bm->block->live.avail.bytes);
    // CHECK(stats.block_size == bm->block->size);

    // Perform a write.
    WT_ITEM buf;
    WT_CLEAR(buf);
    std::string contents = "blahblahblah";
    create_write_buffer(bm, session, contents, &buf, 0);
    uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];
    size_t addr_size, expected_size;
    REQUIRE(bm->write(bm, s, &buf, addr, &addr_size, false, false) == 0);
    REQUIRE(bm->write_size(bm, s, &expected_size) == 0);
    printf("expected size: %ld\n", expected_size);
    wt_off_t result;
    REQUIRE(__ut_block_manager_size(bm, nullptr, &result) == 0);
    printf("result: %ld\n", result);
    __wt_buf_free(nullptr, &buf);

    // Perform another write.
    // bm->checkpoint_load(bm, s, NULL, 0, NULL, &root_addr_size, false);
    // WT_ITEM buf2;
    // WT_CLEAR(buf);
    // std::string more_contents = "dogwifhat";
    // create_write_buffer(bm, session, more_contents, &buf2, 0);
    // uint8_t addr2[WT_BTREE_MAX_ADDR_COOKIE];
    // size_t addr_size2;
    // REQUIRE(bm->write(bm, s, &buf2, addr2, &addr_size2, false, false) == 0);
    // REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &size) == 0);
    // wt_off_t result2;
    // REQUIRE(__ut_block_manager_size(bm, nullptr, &result2) == 0);
    // printf("result2: %ld\n", result2);

    bm->close(bm, s);
}
