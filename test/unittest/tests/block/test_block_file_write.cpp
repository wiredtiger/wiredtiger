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
#include <catch2/catch.hpp>
#include <iostream>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"
#include "../wrappers/config_parser.h"
#include "../wrappers/item_wrapper.h"

const int DEFAULT_BLOCK_SIZE = 512;
const std::string ALLOCATION_SIZE = "512";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

void
validate_block_write(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t offset, uint32_t size,
  uint32_t checksum, const std::string &expected_str, uint32_t expected_size,
  wt_off_t &expected_offset)
{ 
    // Test offset, size and checksum.
    expected_offset += expected_size;
    REQUIRE(offset == expected_offset);
    REQUIRE(size == expected_size);
    REQUIRE(checksum != 0);

    // Test size and extent cache is created after block write.
    REQUIRE(session->block_manager_cleanup != nullptr);
    WT_BLOCK_MGR_SESSION *bms = reinterpret_cast<WT_BLOCK_MGR_SESSION *>(session->block_manager);
    REQUIRE(bms->ext_cache != nullptr);
    REQUIRE(bms->sz_cache != nullptr);

    // Test that the block was correctly written.
    std::string buf(expected_str.length(), ' ');
    block->fh->handle->fh_read(block->fh->handle, (WT_SESSION *)session, offset,
      expected_str.length(), static_cast<void *>(buf.data()));
    REQUIRE(buf == expected_str);
}

WT_BLOCK *
create_block(std::shared_ptr<mock_session> session, config_parser &cp)
{
    WT_BLOCK *block = nullptr;
    REQUIRE(
      (__wt_block_open(session->get_wt_session_impl(), DEFAULT_FILE_NAME.c_str(),
        WT_TIERED_OBJECTID_NONE, cp.get_config_array(), false, false, false, 0, &block)) == 0);

    // Create required extent list for block writes.
    REQUIRE(__wti_block_ckpt_init(session->get_wt_session_impl(), &block->live, "live") == 0);
    block->size = DEFAULT_BLOCK_SIZE;
    return block;
}

TEST_CASE("Block: __wti_block_write_off", "[block_write]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE((session->get_mock_connection()->setup_block_manager(session->get_wt_session_impl())) == 0);

    // Initialize the correct checksum function.
    __wt_process.checksum = wiredtiger_crc32c_func();

    // Create WT_ITEM buffer and copy a string into it.
    WT_ITEM *buf;
    REQUIRE(__wt_scr_alloc(session->get_wt_session_impl(), 0, &buf) == 0);
    REQUIRE(__wt_buf_initsize(session->get_wt_session_impl(), buf, DEFAULT_BLOCK_SIZE) == 0);
    std::string expected_str("hello");
    expected_str.copy(
      reinterpret_cast<char *>(const_cast<void *>(buf->data)), expected_str.length(), 0);

    WT_BLOCK *block = create_block(session, cp);
    wt_off_t offset, expected_offset;
    uint32_t size, checksum;
    SECTION("Test that the arguments checksum, size and offset is correct")
    {
        // Test default write case.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        uint32_t checksum2;
        // Test that the checksum should follow the same with same string.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum2, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);
        REQUIRE(checksum2 == checksum);

        std::string str2("1234567");
        str2.copy(reinterpret_cast<char *>(const_cast<void *>(buf->data)), str2.length(), 0);
        // Test that the checksum should be different with a different string.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum2, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum, str2,
          buf->size, expected_offset);
        REQUIRE(checksum2 != checksum);
    }

    SECTION("Test the data checksum functional argument")
    {
        // Test that the checksum returned is different when the data_checksum is true or not.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        uint32_t data_checksum;
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &data_checksum, true, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);
        REQUIRE(data_checksum != checksum);
    }

    SECTION("Test os cache dirty max should be correctly be calling fsync")
    {
        // When this is set, when the file bytes written is greater an fsync should be called, and
        // the file bytes written is set back to zero.
        block->os_cache_dirty_max = 800;

        // The first block write should succeed.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        REQUIRE(block->fh->written == DEFAULT_BLOCK_SIZE);

        // At this point the file written is greater than os_cache_dirty_max, make sure that
        // the session flag must be set before the fh->written is cleared.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        REQUIRE(block->fh->written == DEFAULT_BLOCK_SIZE * 2);

        // Flag is now set, the block write should flushed with fsync.
        F_SET(session->get_wt_session_impl(), WT_SESSION_CAN_WAIT);
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        REQUIRE(block->fh->written == 0);
    }

    SECTION("Test writes bigger than block")
    {
        // Perform a normal write.
        REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
                  &checksum, false, false, false)) == 0);
        validate_block_write(session->get_wt_session_impl(), block, offset, size, checksum,
          expected_str, buf->size, expected_offset);

        /*
         * Test what happens when a bigger buffer than the block allocation size. - Doesn't work is
         * there a bug? std::string str2(DEFAULT_BLOCK_SIZE, 'a');
         * REQUIRE(__wt_buf_initsize(session->get_wt_session_impl(), buf, DEFAULT_BLOCK_SIZE 2) ==
         * 0); str2.copy(reinterpret_cast<char *>(const_cast<void *>(buf->data)), str2.length(), 0);
         * std::cout << str2 << std::endl;
         * REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
         *   &checksum, false, false, false)) == 0);
         * validate_block_write(session->get_wt_session_impl(), block, str2, offset, size, checksum, 2,
         * buf);
         */
    }

    /*
     * Does this need a test? It is hard to test whether the function below respects this or not.
     * SECTION("Test caller locked")
     * {
     *     __wt_spin_lock(session->get_wt_session_impl(), &block->live_lock);
     *     REQUIRE((__ut_block_write_off(session->get_wt_session_impl(), block, buf, &offset, &size,
     *       &checksum, false, false, true)) == 0);
     *     validate_block_write(session->get_wt_session_impl(), block, str, offset, size, checksum, 1,
     *     buf);
     *     __wt_spin_unlock(session->get_wt_session_impl(), &block->live_lock);
     * }
     */
    expected_offset = 0;
    REQUIRE(__wti_bm_close_block(session->get_wt_session_impl(), block) == 0);
}
