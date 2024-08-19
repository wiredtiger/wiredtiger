/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_session]: block_session.c
 * The block manager extent list consists of both extent blocks and size blocks. This unit test
 * suite tests aims to test all of the allocation and frees of the extent and size block functions.
 *
 * The block session manages an internal caching mechanism for both block and size blocks that are
 * created or discarded.
 */
#include "wt_internal.h"
#include <catch2/catch.hpp>
#include "../wrappers/mock_session.h"

void
validate_and_cleanup_ext_block(WT_EXT *ext)
{
    REQUIRE(ext != nullptr);
    REQUIRE(ext->depth != 0);
    __wt_free(nullptr, ext);
}

void
validate_and_cleanup_size_block(WT_SIZE *size)
{
    REQUIRE(size != nullptr);
    __wt_free(nullptr, size);
}

void
cleanup_ext_list(WT_BLOCK_MGR_SESSION *bms)
{
    WT_EXT *curr = bms->ext_cache;
    WT_EXT *tmp;
    while (curr != NULL) {
        tmp = curr;
        curr = curr->next[0];
        __wt_free(nullptr, tmp);
    }
}

void
validate_and_cleanup_ext_list(WT_BLOCK_MGR_SESSION *bms, int expected_items)
{
    int i = 0;

    REQUIRE(bms != nullptr);
    WT_EXT *curr = bms->ext_cache;

    REQUIRE(bms->ext_cache_cnt == expected_items);
    for (; i < expected_items; i++) {
        REQUIRE(bms->ext_cache != nullptr);
        curr = curr->next[0];
    }
    REQUIRE(curr == nullptr);
    cleanup_ext_list(bms);
}

void
cleanup_size_list(WT_BLOCK_MGR_SESSION *bms)
{
    WT_SIZE *curr = bms->sz_cache;
    WT_SIZE *tmp;
    while (curr != NULL) {
        tmp = curr;
        curr = curr->next[0];
        __wt_free(nullptr, tmp);
    }
}

void
validate_and_cleanup_size_list(WT_BLOCK_MGR_SESSION *bms, int expected_items)
{
    int i = 0;

    REQUIRE(bms != nullptr);
    WT_SIZE *curr = bms->sz_cache;

    REQUIRE(bms->sz_cache_cnt == expected_items);
    for (; i < expected_items; i++) {
        REQUIRE(bms->sz_cache != nullptr);
        curr = curr->next[0];
    }
    REQUIRE(curr == nullptr);
    cleanup_size_list(bms);
}

TEST_CASE("Block session: __block_ext_alloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_EXT *ext;

    ext = nullptr;
    __wt_random_init(&session->getWtSessionImpl()->rnd);

    REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
    validate_and_cleanup_ext_block(ext);
}

TEST_CASE("Block session: __block_ext_prealloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Allocate zero extent blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 0) == 0);
        validate_and_cleanup_ext_list(bms, 0);
    }

    SECTION("Allocate one ext blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 1) == 0);
        validate_and_cleanup_ext_list(bms, 1);
    }

    SECTION("Allocate multiple ext blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 3) == 0);
        validate_and_cleanup_ext_list(bms, 3);
    }
}

TEST_CASE("Block session: __block_size_alloc", "[block_session]")
{
    WT_SIZE *sz;

    sz = nullptr;
    REQUIRE(__ut_block_size_alloc(nullptr, &sz) == 0);
    validate_and_cleanup_size_block(sz);
}

TEST_CASE("Block session: __block_size_prealloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Allocate zero size blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 0) == 0);
        validate_and_cleanup_size_list(bms, 0);
    }

    SECTION("Allocate one size blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 1) == 0);
        validate_and_cleanup_size_list(bms, 1);
    }

    SECTION("Allocate multiple size blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 3) == 0);
        validate_and_cleanup_size_list(bms, 3);
    }
}

TEST_CASE("Block session: __wti_block_ext_alloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Allocate with null block manager session and no extent cache")
    {
        std::shared_ptr<MockSession> session_test_bm = MockSession::buildTestMockSession();
        WT_EXT *ext;

        REQUIRE(__wti_block_ext_alloc(session_test_bm->getWtSessionImpl(), &ext) == 0);
        validate_and_cleanup_ext_block(ext);

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
        validate_and_cleanup_ext_block(ext);
    }

    SECTION("Allocate with fake zero cache extent count")
    {
        WT_EXT *ext;
        WT_EXT *cached_ext;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
        // Construct extent cache with one item.
        bms->ext_cache = ext;
        bms->ext_cache_cnt = 0;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &cached_ext) == 0);
        REQUIRE(cached_ext == ext);
        validate_and_cleanup_ext_block(ext);
    }

    SECTION("Allocate with one extent in cache ")
    {
        WT_EXT *ext;
        WT_EXT *cached_ext;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
        // Construct extent cache with one item.
        bms->ext_cache = ext;
        bms->ext_cache_cnt = 1;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &cached_ext) == 0);
        REQUIRE(cached_ext == ext);
        validate_and_cleanup_ext_block(ext);
    }

    SECTION("Allocate with two extents in cache ")
    {
        WT_EXT *ext;
        WT_EXT *ext2;
        WT_EXT *cached_ext;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext2) == 0);

        // Construct extent cache with two items.
        ext->next[0] = ext2;
        bms->ext_cache = ext;
        bms->ext_cache_cnt = 2;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &cached_ext) == 0);
        REQUIRE(ext == cached_ext);
        REQUIRE(ext2 != cached_ext);
        validate_and_cleanup_ext_block(ext);
        validate_and_cleanup_ext_block(ext2);
    }
}

TEST_CASE("Block session: __wti_block_ext_free", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Free with null block manager session -- needs discussion")
    {
        std::shared_ptr<MockSession> session_no_bm = MockSession::buildTestMockSession();
        WT_EXT *ext;

        REQUIRE(__ut_block_ext_alloc(session_no_bm->getWtSessionImpl(), &ext) == 0);
        REQUIRE(ext != nullptr);

        __wti_block_ext_free(session_no_bm->getWtSessionImpl(), ext);
        // REQUIRE(ext == nullptr);
    }

    SECTION("Calling free with cache")
    {
        WT_EXT *ext;
        WT_EXT *ext2;

        REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);

        __wti_block_ext_free(session->getWtSessionImpl(), ext);

        REQUIRE(ext != nullptr);
        REQUIRE(bms->ext_cache == ext);

        REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext2) == 0);
        __wti_block_ext_free(session->getWtSessionImpl(), ext2);

        REQUIRE(ext != nullptr);
        REQUIRE(bms->ext_cache == ext2);
        REQUIRE(bms->ext_cache->next[0] == ext);
        validate_and_cleanup_ext_list(bms, 2);
    }
}

TEST_CASE("Block session: __wti_block_ext_prealloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    SECTION("Pre-alloc with null block manager")
    {
        WT_BLOCK_MGR_SESSION *bms;

        bms = nullptr;
        __wt_random_init(&session->getWtSessionImpl()->rnd);

        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 0) == 0);
        bms = static_cast<WT_BLOCK_MGR_SESSION *>(session->getWtSessionImpl()->block_manager);

        REQUIRE(bms != nullptr);
    }

    SECTION("Pre-alloc with block manager")
    {
        WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 0) == 0);
        REQUIRE(session->getWtSessionImpl()->block_manager == bms);
    }
}

TEST_CASE("Block session: __wti_block_size_alloc", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Allocate with null block manager session and no size cache")
    {
        std::shared_ptr<MockSession> session_no_bm = MockSession::buildTestMockSession();
        WT_SIZE *sz;

        REQUIRE(__wti_block_size_alloc(session_no_bm->getWtSessionImpl(), &sz) == 0);
        validate_and_cleanup_size_block(sz);

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
        validate_and_cleanup_size_block(sz);
    }

    SECTION("Allocate with fake zero cache size count")
    {
        WT_SIZE *sz;
        WT_SIZE *cached_sz;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
        // Construct extent cache with one item.
        bms->sz_cache = sz;
        bms->sz_cache_cnt = 0;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &cached_sz) == 0);
        REQUIRE(cached_sz == sz);
        validate_and_cleanup_size_block(sz);
    }

    SECTION("Allocate with one size in cache ")
    {
        WT_SIZE *sz;
        WT_SIZE *cached_sz;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
        // Construct size cache with one item.
        bms->sz_cache = sz;
        bms->sz_cache_cnt = 1;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &cached_sz) == 0);
        REQUIRE(cached_sz == sz);
        validate_and_cleanup_size_block(sz);
    }

    SECTION("Allocate with two sizes in cache ")
    {
        WT_SIZE *sz;
        WT_SIZE *sz2;
        WT_SIZE *cached_sz;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz2) == 0);

        // Construct extent cache with two items.
        sz->next[0] = sz2;
        bms->sz_cache = sz;
        bms->sz_cache_cnt = 2;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &cached_sz) == 0);
        REQUIRE(sz == cached_sz);
        REQUIRE(sz2 != cached_sz);
        validate_and_cleanup_size_block(sz);
        validate_and_cleanup_size_block(sz2);
    }
}

TEST_CASE("Block session: __wti_block_size_free", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Free with null block manager session -- needs discussion")
    {
        std::shared_ptr<MockSession> session_no_bm = MockSession::buildTestMockSession();
        WT_SIZE *sz;

        REQUIRE(__ut_block_size_alloc(session_no_bm->getWtSessionImpl(), &sz) == 0);
        REQUIRE(sz != nullptr);

        __wti_block_size_free(session_no_bm->getWtSessionImpl(), sz);

        // REQUIRE(sz == nullptr);
    }

    SECTION("Calling free with empty size cache")
    {
        WT_SIZE *sz;
        WT_SIZE *sz2;

        REQUIRE(__ut_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);

        __wti_block_size_free(session->getWtSessionImpl(), sz);

        REQUIRE(sz != nullptr);
        REQUIRE(bms->sz_cache == sz);

        REQUIRE(__ut_block_size_alloc(session->getWtSessionImpl(), &sz2) == 0);
        __wti_block_size_free(session->getWtSessionImpl(), sz2);

        REQUIRE(sz != nullptr);
        REQUIRE(bms->sz_cache == sz2);
        REQUIRE(bms->sz_cache->next[0] == sz);
        validate_and_cleanup_size_list(bms, 2);
    }
}

TEST_CASE("Block session: __block_manager_session_cleanup", "[block_session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    SECTION("Free with null session block manager ")
    {
        REQUIRE(__ut_block_manager_session_cleanup(session->getWtSessionImpl()) == 0);
        REQUIRE(session->getWtSessionImpl()->block_manager == nullptr);
    }

    SECTION("Calling free with session block manager")
    {
        WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
        REQUIRE(bms != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session->getWtSessionImpl()) == 0);

        // REQUIRE(bms == nullptr);
    }
}

TEST_CASE("Block session: __block_ext_discard", "[block_session]")
{
    WT_EXT *ext, *ext2, *ext3;
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);
    REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext2) == 0);
    REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext3) == 0);

    // Construct extent cache with three items.
    ext2->next[0] = ext3;
    ext->next[0] = ext2;
    bms->ext_cache = ext;
    bms->ext_cache_cnt = 3;
    SECTION("Discard every item in extent list")
    {
        REQUIRE(__ut_block_ext_discard(session->getWtSessionImpl(), 0) == 0);
        validate_and_cleanup_ext_list(bms, 0);
    }

    SECTION("Discard until only one item is in extent list")
    {
        REQUIRE(__ut_block_ext_discard(session->getWtSessionImpl(), 1) == 0);

        validate_and_cleanup_ext_list(bms, 1);
    }

    SECTION("Discard nothing in the extent list")
    {
        REQUIRE(__ut_block_ext_discard(session->getWtSessionImpl(), 3) == 0);
        validate_and_cleanup_ext_list(bms, 3);
    }

    SECTION("Fake cache count and discard every item in extent list")
    {
        bms->ext_cache_cnt = 4;
        REQUIRE(__ut_block_ext_discard(session->getWtSessionImpl(), 0) == WT_ERROR);
    }
}

TEST_CASE("Block session: __block_size_discard", "[block_session]")
{
    WT_SIZE *sz, *sz2, *sz3;
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
    REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz2) == 0);
    REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz3) == 0);

    // Construct size cache with two items.
    sz2->next[0] = sz3;
    sz->next[0] = sz2;
    bms->sz_cache = sz;
    bms->sz_cache_cnt = 3;
    SECTION("Discard every item in size list")
    {
        REQUIRE(__ut_block_size_discard(session->getWtSessionImpl(), 0) == 0);
        validate_and_cleanup_size_list(bms, 0);
    }

    SECTION("Discard until only one item is in size list")
    {
        REQUIRE(__ut_block_size_discard(session->getWtSessionImpl(), 1) == 0);
        validate_and_cleanup_size_list(bms, 1);
    }

    SECTION("Discard nothing in the size list")
    {
        REQUIRE(__ut_block_size_discard(session->getWtSessionImpl(), 3) == 0);
        validate_and_cleanup_size_list(bms, 3);
    }

    SECTION("Fake cache count and discard every item in size list")
    {
        bms->sz_cache_cnt = 4;
        REQUIRE(__ut_block_size_discard(session->getWtSessionImpl(), 0) == WT_ERROR);
    }
}
