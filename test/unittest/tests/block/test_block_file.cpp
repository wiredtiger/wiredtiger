/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_file]: block_open.c
 * The block manager extent list consists of both extent blocks and size blocks. This unit test
 * suite tests aims to test all of the allocation and frees of the extent and size block functions.
 *
 * The block session manages an internal caching mechanism for both block and size blocks that are
 * created or discarded.
 */
#include "wt_internal.h"
#include <catch2/catch.hpp>
#include "../wrappers/mock_session.h"
#include "../wrappers/config_parser.h"
#include <iostream>

const std::string ALLOCATION_SIZE = "512";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

void
validate_block_fh(WT_BLOCK *block, std::string const& name)
{
    REQUIRE(block->fh != nullptr);
    REQUIRE(std::string(block->fh->name) == name);
    REQUIRE(block->fh->file_type == WT_FS_OPEN_FILE_TYPE_DATA);
    REQUIRE(block->fh->ref == 1);
}

void
validate_block_config(WT_BLOCK *block, config_parser& cp)
{
    std::map<std::string, std::string> config_map = cp.get_config_map();
    auto it = config_map.find("allocation_size");
    uint32_t expected_alloc_size =
      it != config_map.end() ? std::stoi(it->second) : std::stoi(ALLOCATION_SIZE);
    REQUIRE(block->allocsize == expected_alloc_size);

    it = config_map.find("block_allocation");
    uint32_t expected_block_allocation =
      it != config_map.end() && it->second.compare(BLOCK_ALLOCATION) == 0 ? 0 : 1;

    REQUIRE(block->allocfirst == expected_block_allocation);
    REQUIRE(block->os_cache_max == std::stoi(config_map.at("os_cache_max")));
    REQUIRE(block->os_cache_dirty_max == std::stoi(config_map.at("os_cache_dirty_max")));
}

void
validate_block(std::shared_ptr<MockSession> session, WT_BLOCK *block, config_parser& cp,
  uint expected_ref, std::string const& name, bool readonly = false)
{

    REQUIRE(block != nullptr);

    // Test Block immediate members.
    REQUIRE(std::string(block->name) == name);
    REQUIRE(block->objectid == WT_TIERED_OBJECTID_NONE);
    // REQUIRE(block->compact_session_id = WT_SESSION_ID_INVALID);
    REQUIRE(block->ref == expected_ref);
    REQUIRE(block->readonly == readonly);
    REQUIRE(block->created_during_backup == false);
    REQUIRE(block->extend_len == 0);

    // Test Block file handle members.
    validate_block_fh(block, name);
    REQUIRE(std::string(block->live_lock.name) == std::string("block manager"));
    REQUIRE(block->live_lock.initialized == true);

    // Test Block configuration members.
    validate_block_config(block, cp);

    // Connection block lock should not be locked after the function completes.
    WT_CONNECTION_IMPL *conn = session->getMockConnection()->getWtConnectionImpl();
    REQUIRE(conn->block_lock.initialized == true);
    REQUIRE(conn->block_lock.session_id != session->getWtSessionImpl()->id);
}

void
validate_free_block(std::shared_ptr<MockSession> session, WT_BLOCK *block, config_parser& cp,
  uint expected_ref, std::string const& name, bool readonly = false)
{
    WT_CONNECTION_IMPL *conn = session->getMockConnection()->getWtConnectionImpl();
    if (expected_ref == 0) {
        //REQUIRE(block == nullptr);

        uint64_t hash = __wt_hash_city64(name.c_str(), name.length());
        uint64_t bucket = hash & (conn->hash_size - 1);
        TAILQ_FOREACH (block, &conn->blockhash[bucket], hashq)
            REQUIRE(std::string(block->name) != name);
    } else {
        REQUIRE(block != nullptr);
        validate_block(session, block, cp, expected_ref, DEFAULT_FILE_NAME, readonly);
        block->sync_on_checkpoint = false;
    }

    // Connection block lock should not be locked after the function completes.
    REQUIRE(conn->block_lock.initialized == true);
    REQUIRE(conn->block_lock.session_id != session->getWtSessionImpl()->id);
}

TEST_CASE("Block: __wt_block_open and __wti_bm_close_block", "[block_file]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE((session->getMockConnection()->setupBlockManager(session->getWtSessionImpl())) == 0);

    SECTION("Test block open and block close with default configuration")
    {
        WT_BLOCK *block = nullptr;
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        // Test already made item in hashmap.
        WT_BLOCK *block2 = nullptr;
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block2)) == 0);
        validate_block(session, block2, cp, 2, DEFAULT_FILE_NAME);

        // Test block close, frees the block correctly.
        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block2) == 0);
        validate_free_block(session, block2, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);

        /*
         * Test already made item in hashmap but with different configuration.
         * WT_BLOCK *block3;
         * cp.get_config_map()["allocation_size"] = "1024";
         * REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
         * WT_TIERED_OBJECTID_NONE,
         *           cp.get_config_array(), false, false, false, 0, &block3)) == 0);
         * validate_and_free_block(session->getWtSessionImpl(), block, cp, 3, DEFAULT_FILE_NAME);
         */
    }

    SECTION("Test the configuration of allocation size")
    {
        // Test that argument allocation size should be priority over configuration string.
        WT_BLOCK *block = nullptr;
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 1024,
                  &block)) == 0);
        cp.get_config_map()["allocation_size"] = "1024";
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);
  
        // Test that no allocation size in configuration should fail.
        REQUIRE(cp.get_config_map().erase("allocation_size") == 1);
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0,
                  &block)) == WT_NOTFOUND);
        REQUIRE(block == nullptr);
    }

    SECTION("Test block_allocation configuration")
    {
        // Test that block allocation is configured to first.
        cp.get_config_map()["block_allocation"] = "first";
        WT_BLOCK *block = nullptr;
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);

        // Test when block allocation is not configured.
        REQUIRE(cp.get_config_map().erase("block_allocation") == 1);
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0,
                  &block)) == WT_NOTFOUND);
        REQUIRE(block == nullptr);

        // If block allocation is set to garbage, it should default back to "best".
        cp.get_config_map()["block_allocation"] = "garbage";
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 512,
                  &block)) == 0);
        cp.get_config_map()["block_allocation"] = "best";
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);
    }

    SECTION("Test os_cache_max and os_cache_dirty_max configuration")
    {
        // Test when os_cache_max is not configured.
        WT_BLOCK *block = nullptr;
        REQUIRE(cp.get_config_map().erase("os_cache_max") == 1);
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0,
                  &block)) == WT_NOTFOUND);
        REQUIRE(block == nullptr);

        // Test when os_cache_max is configured to 512.
        cp.get_config_map()["os_cache_max"] = "512";
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);

        // Test when os_cache_dirty_max is not configured.
        REQUIRE(cp.get_config_map().erase("os_cache_dirty_max") == 1);
        REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
                  WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0,
                  &block)) == WT_NOTFOUND);
        REQUIRE(block == nullptr);

        // Test when os_cache_dirty_max is configured to 512.
        cp.get_config_map()["os_cache_dirty_max"] = "512";
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);
    }

    SECTION("Test block open with read only configuration")
    {
        WT_BLOCK *block = nullptr;
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, true, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME, true);

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);
    }

    // SECTION("Test block close with nullptr")
    // {
    //     REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), nullptr) == 0);
    // }

    SECTION("Test block close with block sync")
    {
        WT_BLOCK *block = nullptr;
        REQUIRE(
          (__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
            WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, true, false, 0, &block)) == 0);
        validate_block(session, block, cp, 1, DEFAULT_FILE_NAME, true);
        block->sync_on_checkpoint = true;

        REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
        validate_free_block(session, block, cp, 0, DEFAULT_FILE_NAME);
    }
}
