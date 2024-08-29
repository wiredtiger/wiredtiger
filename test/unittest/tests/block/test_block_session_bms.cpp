/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_session_bms]: block_session.c
 * The block manager extent list consists of both extent and size type blocks. This unit test
 * suite tests aims to test all of the allocation and frees of combined extent and size functions.
 *
 * The file aims to test the management of the block manager session.
 */
#include "wt_internal.h"
#include <catch2/catch.hpp>
#include "../wrappers/mock_session.h"
#include "util_block.h"

TEST_CASE("Block session: __wti_block_ext_prealloc", "[block_session_bms]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    SECTION("Prealloc with null block manager")
    {
        WT_BLOCK_MGR_SESSION *bms = nullptr;

        __wt_random_init(&session->getWtSessionImpl()->rnd);

        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 0) == 0);
        bms = static_cast<WT_BLOCK_MGR_SESSION *>(session->getWtSessionImpl()->block_manager);

        /*
         * Check that the session block manager clean up function is set. This is important for
         * cleaning up the blocks that get allocated.
         */
        REQUIRE(session->getWtSessionImpl()->block_manager_cleanup != nullptr);
        REQUIRE(bms != nullptr);
    }

    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
    SECTION("Prealloc with block manager")
    {
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        REQUIRE(session->getWtSessionImpl()->block_manager == bms);
        validate_and_free_ext_list(bms, 2);
        validate_and_free_size_list(bms, 2);
    }

    SECTION("Prealloc with existing cache")
    {
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        REQUIRE(session->getWtSessionImpl()->block_manager == bms);
        validate_ext_list(bms, 2);
        validate_size_list(bms, 2);

        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 5) == 0);
        validate_and_free_ext_list(bms, 5);
        validate_and_free_size_list(bms, 5);
    }
}

TEST_CASE("Block session: __block_manager_session_cleanup", "[block_session_bms]")
{
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();

    SECTION("Free with null session block manager ")
    {
        std::shared_ptr<MockSession> session_no_bms = MockSession::buildTestMockSession();
        REQUIRE(__ut_block_manager_session_cleanup(session_no_bms->getWtSessionImpl()) == 0);
        REQUIRE(session_no_bms->getWtSessionImpl()->block_manager == nullptr);
    }

    SECTION("Calling free with session block manager")
    {
        REQUIRE(session_impl->block_manager != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session_impl) == 0);
        REQUIRE(session_impl->block_manager == nullptr);
    }

    SECTION("Calling free with session block manager and cache")
    {
        REQUIRE(__wti_block_ext_prealloc(session_impl, 2) == 0);
        validate_ext_list(bms, 2);
        validate_size_list(bms, 2);

        REQUIRE(session_impl->block_manager != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session_impl) == 0);
        REQUIRE(session_impl->block_manager == nullptr);
    }

    SECTION("Calling free with session block manager and fake extent cache")
    {
        REQUIRE(__wti_block_ext_prealloc(session_impl, 2) == 0);
        validate_ext_list(bms, 2);
        validate_size_list(bms, 2);

        bms->ext_cache_cnt = 3;

        REQUIRE(session_impl->block_manager != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session_impl) == WT_ERROR);
        REQUIRE(session_impl->block_manager == nullptr);
    }

    SECTION("Calling free with session block manager and fake size cache")
    {
        REQUIRE(__wti_block_ext_prealloc(session_impl, 2) == 0);
        validate_ext_list(bms, 2);
        validate_size_list(bms, 2);

        bms->sz_cache_cnt = 3;

        REQUIRE(session_impl->block_manager != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session_impl) == WT_ERROR);
        REQUIRE(session_impl->block_manager == nullptr);
    }
}
