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
 * onto files. This test suite aims to test the possible cases for the file write operation. 
 */
#include <catch2/catch.hpp>
#include <iostream>

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

void test_and_validate_write_size(WT_BM* bm, std::shared_ptr<mock_session> session, size_t size) {
    size_t init_size = size;
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &size) == 0);
    CHECK(size % DEFAULT_BLOCK_SIZE == 0);
    CHECK(size == ((init_size / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE);
}

void validate_block(WT_BM* bm, std::shared_ptr<mock_session> session, WT_ITEM* write_buf, 
    uint8_t *cookie, size_t cookie_size, bool data_checksum) {
    // Test that the write buffer was correctly written via performing a read.
    WT_ITEM read_buf;
    WT_CLEAR(read_buf);
    REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie, cookie_size) == 0);
    WT_BLOCK_HEADER *blk = reinterpret_cast<WT_BLOCK_HEADER*>(WT_BLOCK_HEADER_REF(write_buf->mem));
    blk->checksum = 0;

    CHECK(memcmp(write_buf->mem, read_buf.mem, write_buf->size) == 0);
    CHECK(blk->disk_size == write_buf->memsize);
    if (data_checksum)
        CHECK(blk->flags == WT_BLOCK_DATA_CKSUM);
    else
        CHECK(blk->flags == 0);
    __wt_buf_free(nullptr, &read_buf);
}

void create_write_buffer(WT_BM* bm, std::shared_ptr<mock_session> session, std::string contents, WT_ITEM* buf, size_t buf_memsize) {
    // Fetch write buffer size from block manager.
    REQUIRE(bm->write_size(bm, session->get_wt_session_impl(), &buf_memsize) == 0);
    
    // Intialise the buffer with aligned size.
    F_SET(buf, WT_ITEM_ALIGNED);
    REQUIRE(__wt_buf_init(session->get_wt_session_impl(), buf, buf_memsize) == 0);
    
    // Copy content string into the buffer.
    // Following the architecture guide, it seems that the block manager expects a block header. 
    // I have tried to mimic that here.
    void* p = WT_BLOCK_HEADER_BYTE(buf->mem);
    memset(buf->mem, 0, buf->memsize);
    buf->data = contents.c_str();
    buf->size = contents.length() + WT_BLOCK_HEADER_BYTE_SIZE;
    memcpy(p, buf->data, buf->size);
    //REQUIRE(__wt_buf_set(session->get_wt_session_impl(), buf, contents.c_str(), contents.length()) == 0);
}

TEST_CASE("Block manager: file operation read, write and write_size functions", "[block_api]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<mock_session> session = mock_session::build_test_mock_session();
    config_parser cp({{"allocation_size", ALLOCATION_SIZE}, {"block_allocation", BLOCK_ALLOCATION},
      {"os_cache_max", OS_CACHE_MAX}, {"os_cache_dirty_max", OS_CACHE_DIRTY_MAX},
      {"access_pattern_hint", ACCESS_PATTERN}});

    REQUIRE((session->get_mock_connection()->setup_block_manager(session->get_wt_session_impl())) == 0);
    session->setup_block_manager_file_operations();
    WT_BM *bm;
    REQUIRE(__wt_blkcache_open(session->get_wt_session_impl(), "file:test", cp.get_config_array(), false, false, DEFAULT_BLOCK_SIZE, &bm) == 0);

    size_t root_addr_size;
    bm->checkpoint_load(bm, session->get_wt_session_impl(), NULL, 0, NULL, &root_addr_size, false);

    SECTION("Test write_size api")
    {   
        test_and_validate_write_size(bm, session, 0);
        test_and_validate_write_size(bm, session, 800);
        test_and_validate_write_size(bm, session, 1234);
        test_and_validate_write_size(bm, session, 5000);
        test_and_validate_write_size(bm, session, 5120);
        test_and_validate_write_size(bm, session, 9999);
    }

    // Make sure that the contents of write are okay
    SECTION("Test generic write api")
    {   
        // Should the buffer size be 512 at this point? For alignment purposes.
        WT_ITEM buf;
        WT_CLEAR(buf);
        create_write_buffer(bm, session, "hello", &buf, 0);

        uint8_t addr[WT_BTREE_MAX_ADDR_COOKIE];
        size_t addr_size;

        // Checksum gets inserted in the buffer here.
        REQUIRE(bm->write(bm, session->get_wt_session_impl(), &buf, addr, &addr_size, false, false) == 0);
        validate_block(bm, session, &buf, addr, addr_size, false);
        REQUIRE(bm->addr_invalid(bm, session->get_wt_session_impl(), addr, addr_size) == 0);
    }

    SECTION("Test complex write api with same write buffer size")
    {   
        //Should the buffer size be 256 at this point? For alignment purposes.
        std::vector<std::string> test_strings({"hello", "testing", "1234567890", std::move(std::string(64, 'a'))
            , std::move(std::string(128, 'b')), std::move(std::string(190, 'c'))});
        std::vector<std::pair<std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE>, size_t>> cookies;
        for (const auto& str: test_strings) {
            WT_ITEM buf;
            WT_CLEAR(buf);
            create_write_buffer(bm, session, str, &buf, 0);

            std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE> addr;
            size_t addr_size;
            REQUIRE(bm->write(bm, session->get_wt_session_impl(), &buf, addr.data(), &addr_size, false, false) == 0);
            // Test that the write buffer doesn't get modified after performing write.
            void *p = WT_BLOCK_HEADER_BYTE(buf.mem);
            CHECK(memcmp(str.c_str(), p, str.size()) == 0);
            
            validate_block(bm, session, &buf, addr.data(), addr_size, false);
            cookies.push_back({std::move(addr), addr_size});
            __wt_buf_free(nullptr, &buf);
        }

        // Test that previous write buffers are still present in the block manager.
        for (int i = 0; i < cookies.size(); i++) {
            const auto& cookie = cookies.at(i);
            const auto& str = test_strings.at(i);

            WT_ITEM read_buf;
            WT_CLEAR(read_buf);
            REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie.first.data(), cookie.second) == 0);
            void *p = WT_BLOCK_HEADER_BYTE(read_buf.mem);
            CHECK(memcmp(str.data(), p, str.size()) == 0);
            __wt_buf_free(nullptr, &read_buf);
        }
    }


    SECTION("Test complex write api with fluctating write size")
    {   
        // std::vector<std::string> test_strings({"hello", std::move(std::string(300, 'a')), 
        //     std::move(std::string(550, 'c')), std::move(std::string(900, 'd')), std::move(std::string(1400, 'd'))});
        // std::vector<std::pair<std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE>, size_t>> cookies;
        // for (const auto& str: test_strings) {
        //     WT_ITEM buf;
        //     WT_CLEAR(buf);
        //     test_and_validate_write_size(bm, session, str.length());
        //     create_write_buffer(bm, session, str, &buf, str.length());

        //     std::array<uint8_t, WT_BTREE_MAX_ADDR_COOKIE> addr;
        //     size_t addr_size;
        //     REQUIRE(bm->write(bm, session->get_wt_session_impl(), &buf, addr.data(), &addr_size, false, false) == 0);
        //     // Test that the write buffer doesn't get modified after performing write.
        //     void *p = WT_BLOCK_HEADER_BYTE(buf.mem);
        //     CHECK(memcmp(str.c_str(), p, str.size()) == 0);
            
        //     validate_block(bm, session, &buf, addr.data(), addr_size, false);
        //     cookies.push_back({std::move(addr), addr_size});
        //     __wt_buf_free(nullptr, &buf);
        // }

        // // Test that previous write buffers are still present in the block manager.
        // for (int i = 0; i < cookies.size(); i++) {
        //     const auto& cookie = cookies.at(i);
        //     const auto& str = test_strings.at(i);

        //     WT_ITEM read_buf;
        //     WT_CLEAR(read_buf);
        //     REQUIRE(bm->read(bm, session->get_wt_session_impl(), &read_buf, cookie.first.data(), cookie.second) == 0);
        //     void *p = WT_BLOCK_HEADER_BYTE(read_buf.mem);
        //     CHECK(memcmp(str.data(), p, str.size()) == 0);
        //     __wt_buf_free(nullptr, &read_buf);
        //}
    }

    SECTION("Test capacity throttling functional argument")
    {   
        // Should the buffer size be 512 at this point? For alignment purposes.
        // WT_ITEM buf;
        // create_write_buffer(bm, session, "hello", &buf);

        // Check capacity throlling through checkpoint IO
        //REQUIRE(bm->write(bm, session->get_wt_session_impl(), &buf, addr, &addr_size, false, true) == 0);
    }
    bm->close(bm, session->get_wt_session_impl());
}
