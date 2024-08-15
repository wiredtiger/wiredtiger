/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include <catch2/catch.hpp>
#include "../wrappers/mock_session.h"

TEST_CASE("Block session: __block_ext_alloc", "[block-session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_EXT *ext;

    ext = nullptr;
    __wt_random_init(&session->getWtSessionImpl()->rnd);

    REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);

    REQUIRE(ext != nullptr);
    REQUIRE(ext->depth != 0);
    __wt_free(nullptr, ext);
}

TEST_CASE("Block session: __block_ext_prealloc", "[block-session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    // Allocate zero extent blocks.
    SECTION("Allocate zero extent blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 0) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->ext_cache == nullptr);
        REQUIRE(bms->ext_cache_cnt == 0);
    }

    SECTION("Allocate one ext blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 1) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->ext_cache != nullptr);
        REQUIRE(bms->ext_cache->next[0] == nullptr);
        REQUIRE(bms->ext_cache_cnt == 1);
        __wt_free(nullptr, bms->ext_cache);
    }

    SECTION("Allocate multiple ext blocks")
    {
        REQUIRE(__ut_block_ext_prealloc(session->getWtSessionImpl(), 3) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->ext_cache != nullptr);
        REQUIRE(bms->ext_cache->next[0] != nullptr);
        REQUIRE(bms->ext_cache->next[0]->next[0] != nullptr);
        REQUIRE(bms->ext_cache->next[0]->next[0]->next[0] == nullptr);
        REQUIRE(bms->ext_cache_cnt == 3);
        __wt_free(nullptr, bms->ext_cache->next[0]->next[0]);
        __wt_free(nullptr, bms->ext_cache->next[0]);
        __wt_free(nullptr, bms->ext_cache);
    }
}

TEST_CASE("Block session: __block_size_alloc", "[block-session]")
{
    WT_SIZE *sz;

    sz = nullptr;
    REQUIRE(__ut_block_size_alloc(nullptr, &sz) == 0);
    REQUIRE(sz != nullptr);
    __wt_free(nullptr, sz);
}

TEST_CASE("Block session: __block_size_prealloc", "[block-session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    // Allocate zero extent blocks.
    SECTION("Allocate zero extent blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 0) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->sz_cache == nullptr);
        REQUIRE(bms->sz_cache_cnt == 0);
    }

    SECTION("Allocate one ext blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 1) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->sz_cache != nullptr);
        REQUIRE(bms->sz_cache->next[0] == nullptr);
        REQUIRE(bms->sz_cache_cnt == 1);
        __wt_free(nullptr, bms->sz_cache);
    }

    SECTION("Allocate multiple ext blocks")
    {
        REQUIRE(__ut_block_size_prealloc(session->getWtSessionImpl(), 3) == 0);

        REQUIRE(bms != nullptr);
        REQUIRE(bms->sz_cache != nullptr);
        REQUIRE(bms->sz_cache->next[0] != nullptr);
        REQUIRE(bms->sz_cache->next[0]->next[0] != nullptr);
        REQUIRE(bms->sz_cache->next[0]->next[0]->next[0] == nullptr);
        REQUIRE(bms->sz_cache_cnt == 3);
        __wt_free(nullptr, bms->sz_cache->next[0]->next[0]);
        __wt_free(nullptr, bms->sz_cache->next[0]);
        __wt_free(nullptr, bms->sz_cache);
    }
}

TEST_CASE("Block session: __wti_block_ext_alloc", "[block-session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();

    SECTION("Allocate with null block manager session")
    {
        std::shared_ptr<MockSession> session_no_bm = MockSession::buildTestMockSession();
        WT_EXT *ext;

        REQUIRE(__wti_block_ext_alloc(session_no_bm->getWtSessionImpl(), &ext) == 0);

        REQUIRE(ext != nullptr);
        REQUIRE(ext->depth != 0);
        __wt_free(nullptr, ext);
    }

    SECTION("Allocate with no extent cache")
    {
        WT_EXT *ext;

        REQUIRE(__wti_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);

        REQUIRE(ext != nullptr);
        REQUIRE(ext->depth != 0);
        __wt_free(nullptr, ext);
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
        REQUIRE(bms->ext_cache == nullptr);
        REQUIRE(bms->ext_cache_cnt == 0);
        REQUIRE(ext->depth != 0);
        __wt_free(nullptr, ext);
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
        REQUIRE(bms->ext_cache != nullptr);
        REQUIRE(bms->ext_cache_cnt == 1);
        REQUIRE(ext->depth != 0);
        __wt_free(nullptr, ext);
        __wt_free(nullptr, ext2);
    }
}

TEST_CASE("Block session: __wti_block_ext_free", "[block-session]")
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

    SECTION("Calling free with empty cache")
    {
        WT_EXT *ext;
        WT_EXT *ext2;

        REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext) == 0);

        __wti_block_ext_free(session->getWtSessionImpl(), ext);

        REQUIRE(ext != nullptr);
        REQUIRE(bms->ext_cache == ext);
        REQUIRE(bms->ext_cache->next[0] == nullptr);
        REQUIRE(bms->ext_cache_cnt == 1);

        REQUIRE(__ut_block_ext_alloc(session->getWtSessionImpl(), &ext2) == 0);
        __wti_block_ext_free(session->getWtSessionImpl(), ext2);

        REQUIRE(ext != nullptr);
        REQUIRE(bms->ext_cache == ext2);
        REQUIRE(bms->ext_cache->next[0] == ext);
        REQUIRE(bms->ext_cache->next[0]->next[0] == nullptr);
        REQUIRE(bms->ext_cache_cnt == 2);
    }
}

TEST_CASE("Block session: __wti_block_ext_prealloc", "[block-session]")
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

TEST_CASE("Block session: __wti_block_size_alloc", "[block-session]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
    
    SECTION("Allocate with null block manager session")
    {
        std::shared_ptr<MockSession> session_no_bm = MockSession::buildTestMockSession();
        WT_SIZE *sz;

        REQUIRE(__wti_block_size_alloc(session_no_bm->getWtSessionImpl(), &sz) == 0);

        REQUIRE(sz != nullptr);
        __wt_free(nullptr, sz);
    }

    SECTION("Allocate with no size cache")
    {
        WT_SIZE *sz;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);

        REQUIRE(sz != nullptr);
        __wt_free(nullptr, sz);
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
        REQUIRE(bms->sz_cache == nullptr);
        REQUIRE(bms->sz_cache_cnt == 0);
        __wt_free(nullptr, sz);
    }

    SECTION("Allocate with two sizes in cache ")
    {
        WT_SIZE *sz;
        WT_SIZE *sz2;
        WT_SIZE *cached_sz;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz) == 0);
        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &sz2) == 0);

        // Construct extent cache with two items.
        sz->next[0] = sz;
        bms->sz_cache = sz;
        bms->sz_cache_cnt = 2;

        REQUIRE(__wti_block_size_alloc(session->getWtSessionImpl(), &cached_sz) == 0);
        REQUIRE(sz == cached_sz);
        REQUIRE(sz2 != cached_sz);
        REQUIRE(bms->sz_cache != nullptr);
        REQUIRE(bms->sz_cache_cnt == 1);
        __wt_free(nullptr, sz);
        __wt_free(nullptr, sz2);
    }
}

TEST_CASE("Block session: __wti_block_size_free", "[block-session]")
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
        REQUIRE(bms->sz_cache->next[0] == nullptr);
        REQUIRE(bms->sz_cache_cnt == 1);

        REQUIRE(__ut_block_size_alloc(session->getWtSessionImpl(), &sz2) == 0);
        __wti_block_size_free(session->getWtSessionImpl(), sz2);

        REQUIRE(sz != nullptr);
        REQUIRE(bms->sz_cache == sz2);
        REQUIRE(bms->sz_cache->next[0] == sz);
        REQUIRE(bms->sz_cache->next[0]->next[0] == nullptr);
        REQUIRE(bms->sz_cache_cnt == 2);
    }
}