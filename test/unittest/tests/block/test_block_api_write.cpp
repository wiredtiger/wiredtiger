/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_api]: block_write.c
 * The block manager performs file operations on specific blocks which eventually gets translated
 * onto files. This test suite aims to test the possible cases for the write(), read()
 * and write_size() API operations.
 */
#include <catch2/catch.hpp>
#include <iostream>
#include <filesystem>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"
#include "../wrappers/config_parser.h"
#include "../wrappers/item_wrapper.h"

const int DEFAULT_BLOCK_SIZE = 256;
const std::string ALLOCATION_SIZE = "256";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

typedef std::pair<std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE>, size_t> addr_cookie;

void
test_and_validate_write_size(WT_BM *bm, std::shared_ptr<mock_session> session, size_t size)
{
    size_t init_size = size;
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &size) == 0);
    CHECK(size % DEFAULT_BLOCK_SIZE == 0);
    CHECK(size == ((init_size / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE);
}

void
validate_block_contents(WT_BM *bm, std::shared_ptr<mock_session> session, WT_ITEM *write_buf,
  addr_cookie cookie, wt_off_t offset, uint32_t size)
{
    // Test that the write was correctly written via __wt_read().
    WT_ITEM read_buf;
    WT_CLEAR(read_buf);
    REQUIRE(__wt_buf_initsize(session->get_wt_session_impl(), &read_buf, write_buf->memsize) == 0);
    REQUIRE(
      __wt_read(session->get_wt_session_impl(), bm->block->fh, offset, size, read_buf.mem) == 0);
    CHECK(memcmp(write_buf->mem, read_buf.mem, write_buf->size) == 0);

    // Test that the write buffer was correctly via bm->read().
    REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie.first.data(),
              cookie.second) == 0);
    WT_BLOCK_HEADER *blk = reinterpret_cast<WT_BLOCK_HEADER *>(WT_BLOCK_HEADER_REF(write_buf->mem));
    blk->checksum = 0;
    CHECK(memcmp(write_buf->mem, read_buf.mem, write_buf->size) == 0);
    __wt_buf_free(nullptr, &read_buf);
}

void
validate_write_block(WT_BM *bm, std::shared_ptr<mock_session> session, WT_ITEM *write_buf,
  addr_cookie cookie, const std::string &expected_str, bool data_checksum)
{
    // Test that the cookie is in an valid state.
    REQUIRE(bm->addr_invalid(
              bm, session->get_wt_session_impl(), cookie.first.data(), cookie.second) == 0);

    // Test that the write buffer doesn't get modified after performing write.
    CHECK(
      memcmp(expected_str.c_str(), WT_BLOCK_HEADER_BYTE(write_buf->mem), expected_str.size()) == 0);

    // Test address cookie itself, offset, size, checksum
    WT_BLOCK_HEADER *blk = reinterpret_cast<WT_BLOCK_HEADER *>(WT_BLOCK_HEADER_REF(write_buf->mem));
    wt_off_t offset;
    uint32_t objectid, size, checksum;
    REQUIRE(__wt_block_addr_unpack(session->get_wt_session_impl(), bm->block, cookie.first.data(),
              cookie.second, &objectid, &offset, &size, &checksum) == 0);
    REQUIRE(offset % DEFAULT_BLOCK_SIZE == 0);
    REQUIRE(size == write_buf->memsize);
    REQUIRE(checksum == blk->checksum);

    // Test block header members.
    CHECK(blk->disk_size == write_buf->memsize);
    if (data_checksum)
        CHECK(blk->flags == WT_BLOCK_DATA_CKSUM);
    else
        CHECK(blk->flags == 0);

    // Validate block content through bm->read() and __wt_read().
    validate_block_contents(bm, session, write_buf, cookie, offset, size);
}

// Test that previous write buffers are still present in the block manager.
void
test_validate_cookies(WT_BM *bm, std::shared_ptr<mock_session> session,
  const std::vector<addr_cookie> &cookies, const std::vector<std::string> &expected_strings)
{
    for (int i = 0; i < cookies.size(); i++) {
        const auto &cookie = cookies.at(i);

        // Make sure that the cookie is valid still.
        REQUIRE(bm->addr_invalid(
                  bm, session->get_wt_session_impl(), cookie.first.data(), cookie.second) == 0);

        WT_ITEM read_buf;
        WT_CLEAR(read_buf);
        REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie.first.data(),
                  cookie.second) == 0);

        // The data should match the expected string.
        const auto &str = expected_strings.at(i);
        CHECK(memcmp(str.data(), WT_BLOCK_HEADER_BYTE(read_buf.mem), str.size()) == 0);
        __wt_buf_free(nullptr, &read_buf);
    }
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

    // Copy content string into the buffer. Following the architecture guide, it seems that the
    // block manager expects a block header. I have tried to mimic that here.
    REQUIRE(__wt_buf_grow_worker(session->get_wt_session_impl(), buf, buf->size) == 0);
    memcpy(WT_BLOCK_HEADER_BYTE(buf->mem), contents.c_str(), contents.length());
}

TEST_CASE("Block manager: file operation read, write and write_size functions", "[block_api]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE(
      (session->get_mock_connection()->setup_block_manager(session->get_wt_session_impl())) == 0);
    session->setup_block_manager_file_operations();

    WT_BM bm;
    WT_CLEAR(bm);
    __ut_bm_method_set(&bm);

    auto path = std::filesystem::current_path();
    path += "/test.wt";
    REQUIRE(__wt_block_manager_create(
              session->get_wt_session_impl(), path.c_str(), DEFAULT_BLOCK_SIZE) == 0);
    REQUIRE(__wt_block_open(session->get_wt_session_impl(), path.c_str(), WT_TIERED_OBJECTID_NONE,
              cp.get_config_array(), false, false, false, DEFAULT_BLOCK_SIZE, &bm.block) == 0);
    REQUIRE(__wti_block_ckpt_init(session->get_wt_session_impl(), &bm.block->live, nullptr) == 0);

    SECTION("Test write_size api")
    {
        test_and_validate_write_size(&bm, session, 0);
        test_and_validate_write_size(&bm, session, 800);
        test_and_validate_write_size(&bm, session, 1234);
        test_and_validate_write_size(&bm, session, 5000);
        test_and_validate_write_size(&bm, session, 5120);
        test_and_validate_write_size(&bm, session, 9999);

        size_t init_size = UINT32_MAX - 1000;
        REQUIRE(bm.write_size(&bm, session->get_wt_session_impl(), &init_size) == EINVAL);
    }

    SECTION("Test generic write api")
    {
        WT_ITEM buf;
        WT_CLEAR(buf);
        std::string test_string("hello");
        create_write_buffer(&bm, session, test_string, &buf, 0);

        addr_cookie cookie;
        // Perform a generic write.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                  &cookie.second, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);

        // Validate data checksum.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                  &cookie.second, true, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, true);

        __wt_buf_free(nullptr, &buf);
    }

    SECTION("Test complex write api with same write buffer size")
    {
        std::vector<std::string> test_strings(
          {"hello", "testing", "1234567890", std::move(std::string(64, 'a')),
            std::move(std::string(128, 'b')), std::move(std::string(190, 'c'))});
        std::vector<addr_cookie> cookies;
        for (const auto &str : test_strings) {
            WT_ITEM buf;
            WT_CLEAR(buf);
            create_write_buffer(&bm, session, str, &buf, 0);

            addr_cookie cookie;
            REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                      &cookie.second, false, false) == 0);

            validate_write_block(&bm, session, &buf, cookie, str, false);
            cookies.push_back({std::move(cookie.first), cookie.second});
            __wt_buf_free(nullptr, &buf);
        }
        test_validate_cookies(&bm, session, cookies, test_strings);
    }

    SECTION("Test complex write api with changing write size")
    {
        std::vector<std::string> test_strings(
          {"hello", std::move(std::string(300, 'a')), std::move(std::string(550, 'c')),
            std::move(std::string(900, 'd')), std::move(std::string(1400, 'd'))});
        std::vector<addr_cookie> cookies;
        for (const auto &str : test_strings) {
            WT_ITEM buf;
            WT_CLEAR(buf);
            test_and_validate_write_size(&bm, session, str.length());
            create_write_buffer(&bm, session, str, &buf, str.length());

            addr_cookie cookie;
            REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                      &cookie.second, false, false) == 0);

            validate_write_block(&bm, session, &buf, cookie, str, false);
            cookies.push_back({std::move(cookie.first), cookie.second});
            __wt_buf_free(nullptr, &buf);
        }
        test_validate_cookies(&bm, session, cookies, test_strings);
    }

    SECTION("Test os_cache_dirty_max option")
    {
        // When this is set, when the file bytes written is greater an fsync should be called, and
        // the file bytes written is set back to zero.
        bm.block->os_cache_dirty_max = 500;

        std::string test_string(200, 'a');
        WT_ITEM buf;
        WT_CLEAR(buf);
        create_write_buffer(&bm, session, test_string, &buf, 0);

        // The first block write should succeed.
        addr_cookie cookie;
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                  &cookie.second, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == DEFAULT_BLOCK_SIZE);

        // At this point the file written is greater than os_cache_dirty_max, make sure that
        // the session flag must be set before the fh->written is cleared.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                  &cookie.second, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == DEFAULT_BLOCK_SIZE * 2);

        // Flag is now set, the block write should flushed with fsync.
        F_SET(session->get_wt_session_impl(), WT_SESSION_CAN_WAIT);
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.first.data(),
                  &cookie.second, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == 0);
    }
    REQUIRE(__wt_block_close(session->get_wt_session_impl(), bm.block) == 0);
}
