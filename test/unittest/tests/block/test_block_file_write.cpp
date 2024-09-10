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
#include "../wrappers/item_wrapper.h"
#include <iostream>


const int DEFAULT_BLOCK_SIZE = 512;
const std::string ALLOCATION_SIZE = "512";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

void validate_block_write(WT_SESSION_IMPL* session, WT_BLOCK* block, wt_off_t offset, uint32_t size, uint32_t checksum, int num_writes) {
    WT_BLOCK_MGR_SESSION *bms = reinterpret_cast<WT_BLOCK_MGR_SESSION *>(session->block_manager);
    printf("%ld %u %u %u %u\n", offset, size, checksum, bms->ext_cache_cnt, bms->sz_cache_cnt);
    REQUIRE(offset == DEFAULT_BLOCK_SIZE * num_writes);
    REQUIRE(size == DEFAULT_BLOCK_SIZE);
    REQUIRE(checksum != 0);
    
    REQUIRE(session->block_manager_cleanup != nullptr);
    //REQUIRE(bms->ext_cache_cnt == 4);
    REQUIRE(bms->ext_cache != nullptr);
    REQUIRE(bms->sz_cache_cnt == 5);
    REQUIRE(bms->sz_cache != nullptr);
}

WT_BLOCK* create_block(std::shared_ptr<MockSession> session, config_parser& cp) {
    WT_BLOCK *block = nullptr;
    REQUIRE((__wt_block_open(session->getWtSessionImpl(), DEFAULT_FILE_NAME.c_str(),
              WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);

    REQUIRE(__wti_block_ckpt_init(session->getWtSessionImpl(), &block->live, "live") == 0);
    block->size = DEFAULT_BLOCK_SIZE; 
    return block;
}

TEST_CASE("Block: __wti_block_write_off", "[block_write]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    
    REQUIRE((session->getMockConnection()->setupBlockManager(session->getWtSessionImpl())) == 0);
    WT_BLOCK* block = create_block(session, cp);

    __wt_process.checksum = wiredtiger_crc32c_func();
    wt_off_t offset;
    uint32_t size, checksum;
    WT_ITEM* test_buf;
    REQUIRE(__wt_scr_alloc(session->getWtSessionImpl(), 0, &test_buf) == 0);
    REQUIRE(__wt_buf_initsize(session->getWtSessionImpl(), test_buf, DEFAULT_BLOCK_SIZE) == 0);
    
    std::string str ("hello");
    str.copy(reinterpret_cast<char *>(const_cast<void *>(test_buf->data)), str.length(), 0);
    
    SECTION("Test the functional arguments checksum, size and offset")
    {
        // Test normal write.
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
                  &checksum, false, false, false)) == 0); 
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum, 1);

        uint32_t checksum2;
        // Test that the checksum should follow the same with same string.
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
          &checksum2, false, false, false)) == 0); 
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum2, 2);
        REQUIRE(checksum2 == checksum);

        std::string str2 ("1234567");
        str2.copy(reinterpret_cast<char *>(const_cast<void *>(test_buf->data)), str2.length(), 0);
        // Test that the checksum should be different with a different string.
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
          &checksum2, false, false, false)) == 0); 
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum, 3);
        REQUIRE(checksum2 != checksum);
    }

    SECTION("Test the data checksum")
    {
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
          &checksum, true, false, false)) == 0);
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum, 1);
    }
    
    SECTION("Test checkpoint io")
    {
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
          &checksum, false, true, false)) == 0);
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum, 1);
    }

    SECTION("Test caller locked")
    {
        __wt_spin_lock(session->getWtSessionImpl(), &block->live_lock);
        REQUIRE((__ut_block_write_off(session->getWtSessionImpl(), block, test_buf, &offset, &size,
          &checksum, false, false, true)) == 0);
        validate_block_write(session->getWtSessionImpl(), block, offset, size, checksum, 1);
        __wt_spin_unlock(session->getWtSessionImpl(), &block->live_lock);
    }
    
    REQUIRE(__wti_bm_close_block(session->getWtSessionImpl(), block) == 0);
}
