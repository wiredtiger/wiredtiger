/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_api]: block_write.c
 * The block manager writes to files in discrete chunks known as blocks. This set of test validates
 * the write(), read() and write_size() APIs
 */
#include <catch2/catch.hpp>
#include <array>
#include <iostream>
#include <filesystem>

#include "wt_internal.h"
#include "../wrappers/config_parser.h"
#include "../wrappers/item_wrapper.h"
#include "../wrappers/mock_session.h"

const std::string ALLOCATION_SIZE = "256";
const std::string BLOCK_ALLOCATION = "best";
const std::string OS_CACHE_MAX = "0";
const std::string OS_CACHE_DIRTY_MAX = "0";
const std::string ACCESS_PATTERN = "random";
const std::string DEFAULT_FILE_NAME = "test.txt";

struct addr_cookie {
    std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE> addr;
    size_t size;
};

void
setup_bm(std::shared_ptr<mock_session> &session, WT_BM *bm, const std::string &file_path)
{
    REQUIRE(
      (session->get_mock_connection()->setup_block_manager(session->get_wt_session_impl())) == 0);
    session->setup_block_manager_file_operations();

    /*
     * Manually set all the methods in WT_BM. The __wt_blkcache_open() function also initializes the
     * block manager methods, however the function exists within the WiredTiger block cache and is
     * a layer above the block manager module. This violates the testing layer concept as we would
     * be technically testing a whole another module. Therefore we chosen to manually setup the
     * block manager instead
     * .
     */
    WT_CLEAR(*bm);
    __wti_bm_method_set(bm, false);

    // Create the underlying file in the filesystem.
    REQUIRE(__wt_block_manager_create(
              session->get_wt_session_impl(), file_path.c_str(), std::stoi(ALLOCATION_SIZE)) == 0);

    // Open the file and return the block handle.
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});
    REQUIRE(
      __wt_block_open(session->get_wt_session_impl(), file_path.c_str(), WT_TIERED_OBJECTID_NONE,
        cp.get_config_array(), false, false, false, std::stoi(ALLOCATION_SIZE), &bm->block) == 0);

    // Initialize the extent lists inside the block handle.
    REQUIRE(__wti_block_ckpt_init(session->get_wt_session_impl(), &bm->block->live, nullptr) == 0);
}

/*
 * Test and validate the bm->write_size() function.
 */
void
test_and_validate_write_size(WT_BM *bm, std::shared_ptr<mock_session> session, const size_t size)
{
    size_t ret_size = size;
    // This function internally reads and changes the variable.
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &ret_size) == 0);
    // It is expected that the size is a modulo of the allocation size and is aligned to the nearest
    // greater allocation size.
    CHECK(ret_size % std::stoi(ALLOCATION_SIZE) == 0);
    CHECK(ret_size == ((size / std::stoi(ALLOCATION_SIZE)) + 1) * std::stoi(ALLOCATION_SIZE));
}

/*
 * Validate that the write buffer contents was correctly written to the file. We do this through
 * performing a bm->read and a file read and making sure that the read() matches the original write
 * buffer.
 */
void
validate_block_contents(WT_BM *bm, std::shared_ptr<mock_session> session, WT_ITEM *write_buf,
  addr_cookie cookie, wt_off_t offset, uint32_t size)
{
    // Using the non-block manager read function read the file where the block should've been
    // written. Then compare that with the original write buffer.
    WT_ITEM read_buf;
    WT_CLEAR(read_buf);
    REQUIRE(__wt_buf_initsize(session->get_wt_session_impl(), &read_buf, write_buf->memsize) == 0);
    REQUIRE(
      __wt_read(session->get_wt_session_impl(), bm->block->fh, offset, size, read_buf.mem) == 0);
    CHECK(memcmp(write_buf->mem, read_buf.mem, write_buf->size) == 0);

    /*
     * Using the block manager read function read in the block using the address cookie. Testing the
     * using the bm->read() function isn't a complete test as it could be prone to the same bugs as
     * the write function. We check this by doing a standard WT read later on.
     */
    REQUIRE(bm->read(
              bm, session->get_wt_session_impl(), &read_buf, cookie.addr.data(), cookie.size) == 0);
    WT_BLOCK_HEADER *blk = reinterpret_cast<WT_BLOCK_HEADER *>(WT_BLOCK_HEADER_REF(write_buf->mem));
    // Clear the write buf checksum to match the block manager read buffer. The bm->read clears
    // the checksum before returning.
    blk->checksum = 0;
    CHECK(memcmp(write_buf->mem, read_buf.mem, write_buf->size) == 0);
    __wt_buf_free(nullptr, &read_buf);
}

/*
 * Validate that the bm->write() performed correctly.
 */
void
validate_write_block(WT_BM *bm, std::shared_ptr<mock_session> session, WT_ITEM *write_buf,
  addr_cookie cookie, const std::string &expected_str, bool data_checksum)
{
    // Test that the cookie is in an valid state.
    REQUIRE(
      bm->addr_invalid(bm, session->get_wt_session_impl(), cookie.addr.data(), cookie.size) == 0);

    // Test that the write buffer doesn't get modified after performing write.
    CHECK(
      memcmp(expected_str.c_str(), WT_BLOCK_HEADER_BYTE(write_buf->mem), expected_str.size()) == 0);

    // Test address cookie itself, offset, size, checksum
    WT_BLOCK_HEADER *blk = reinterpret_cast<WT_BLOCK_HEADER *>(WT_BLOCK_HEADER_REF(write_buf->mem));
    wt_off_t offset;
    uint32_t objectid, size, checksum;
    REQUIRE(__wt_block_addr_unpack(session->get_wt_session_impl(), bm->block, cookie.addr.data(),
              cookie.size, &objectid, &offset, &size, &checksum) == 0);
    REQUIRE(offset % std::stoi(ALLOCATION_SIZE) == 0);
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

// Test that all previous write performed are still present in the block and file.
void
test_validate_cookies(WT_BM *bm, std::shared_ptr<mock_session> session,
  const std::vector<addr_cookie> &cookies, const std::vector<std::string> &expected_strings)
{
    for (int i = 0; i < cookies.size(); i++) {
        const auto &cookie = cookies.at(i);

        // Make sure that the cookie is valid still.
        REQUIRE(bm->addr_invalid(
                  bm, session->get_wt_session_impl(), cookie.addr.data(), cookie.size) == 0);

        WT_ITEM read_buf;
        WT_CLEAR(read_buf);
        REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie.addr.data(),
                  cookie.size) == 0);

        // The data should match the expected string.
        const auto &str = expected_strings.at(i);
        CHECK(memcmp(str.data(), WT_BLOCK_HEADER_BYTE(read_buf.mem), str.size()) == 0);
        __wt_buf_free(nullptr, &read_buf);
    }
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

TEST_CASE("Block manager: file operation read, write and write_size functions", "[block_api]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();

    WT_BM bm;
    WT_CLEAR(bm);
    auto path = std::filesystem::current_path();
    std::string file_path(path.string() + "/test.wt");
    setup_bm(session, &bm, file_path);

    SECTION("Test write_size api")
    {
        test_and_validate_write_size(&bm, session, 0);
        test_and_validate_write_size(&bm, session, 800);
        test_and_validate_write_size(&bm, session, 1234);
        test_and_validate_write_size(&bm, session, 5000);
        test_and_validate_write_size(&bm, session, 5120);
        test_and_validate_write_size(&bm, session, 9999);

        // The write size function should fail, if the initial write size is too large.
        size_t init_size = UINT32_MAX - 1000;
        REQUIRE(bm.write_size(&bm, session->get_wt_session_impl(), &init_size) == EINVAL);
    }

    SECTION("Test simple cases: writing a single string using the block manager")
    {
        WT_ITEM buf;
        WT_CLEAR(buf);
        std::string test_string("hello");
        create_write_buffer(&bm, session, test_string, &buf, 0);

        addr_cookie cookie;
        // Perform a write.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                  &cookie.size, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);

        // Validate data checksum.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                  &cookie.size, true, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, true);

        __wt_buf_free(nullptr, &buf);
    }

    SECTION("Test complex cases: write api less than the block allocation size")
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
            REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                      &cookie.size, false, false) == 0);

            validate_write_block(&bm, session, &buf, cookie, str, false);
            // Keep track of all the cookies, so that we can validate it again later.
            cookies.push_back({std::move(cookie.addr), cookie.size});
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
            REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                      &cookie.size, false, false) == 0);

            validate_write_block(&bm, session, &buf, cookie, str, false);
            // Keep track of all the cookies, so that we can validate it again later.
            cookies.push_back({std::move(cookie.addr), cookie.size});
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
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                  &cookie.size, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == std::stoi(ALLOCATION_SIZE));

        // At this point the file written is greater than os_cache_dirty_max, make sure that
        // the session flag must be set before the fh->written is cleared.
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                  &cookie.size, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == std::stoi(ALLOCATION_SIZE) * 2);

        // Flag is now set, the block write should flushed with fsync.
        F_SET(session->get_wt_session_impl(), WT_SESSION_CAN_WAIT);
        REQUIRE(bm.write(&bm, session->get_wt_session_impl(), &buf, cookie.addr.data(),
                  &cookie.size, false, false) == 0);
        validate_write_block(&bm, session, &buf, cookie, test_string, false);
        REQUIRE(bm.block->fh->written == 0);
        __wt_buf_free(nullptr, &buf);
    }
    REQUIRE(__wt_block_close(session->get_wt_session_impl(), bm.block) == 0);
    // Remove file from filesystem.
    REQUIRE(__wt_block_manager_drop(session->get_wt_session_impl(), file_path.c_str(), false) == 0);
}