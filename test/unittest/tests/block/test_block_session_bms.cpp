/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_session]: block_session.c
 * The block manager extent list consists of both extent and size type blocks. This unit test
 * suite tests aims to test all of the allocation and frees of the extent and size block functions.
 *
 * The block session manages an internal caching mechanism for both block and size blocks that are
 * created or discarded.
 */
#include "wt_internal.h"
#include <catch2/catch.hpp>
#include "../wrappers/mock_session.h"
#include "util_block.h"

TEST_CASE("Block session: __wti_block_ext_prealloc", "[block_session]")
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
        // validate_and_free_size_list(bms, 2);
    }

    SECTION("Prealloc with existing cache")
    {
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        REQUIRE(session->getWtSessionImpl()->block_manager == bms);
        validate_ext_list(bms, 2);
        // validate_size_list(bms, 2);

        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 5) == 0);
        validate_and_free_ext_list(bms, 5);
        // validate_and_free_size_list(bms, 5);
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

        REQUIRE(bms == nullptr);
    }

    SECTION("Calling free with session block manager and cache")
    {
        WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        validate_ext_list(bms, 2);
        // validate_size_list(bms, 2);

        REQUIRE(bms != nullptr);
        // REQUIRE(__ut_block_manager_session_cleanup(session->getWtSessionImpl()) == 0);
        // validate_ext_list(bms, 0);
        // validate_size_list(bms, 0);
        // REQUIRE(bms == nullptr);
    }

    SECTION("Calling free with session block manager and fake extent cache")
    {
        WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        validate_ext_list(bms, 2);
        // validate_size_list(bms, 2);

        bms->ext_cache_cnt = 3;

        REQUIRE(bms != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session->getWtSessionImpl()) == WT_ERROR);
        // validate_ext_list(bms, 0);
        // validate_size_list(bms, 0);
        // REQUIRE(bms == nullptr);
    }

    SECTION("Calling free with session block manager and fake size cache")
    {
        WT_BLOCK_MGR_SESSION *bms = session->setupBlockManagerSession();
        REQUIRE(__wti_block_ext_prealloc(session->getWtSessionImpl(), 2) == 0);
        validate_ext_list(bms, 2);
        // validate_size_list(bms, 2);

        bms->sz_cache_cnt = 3;

        REQUIRE(bms != nullptr);
        REQUIRE(__ut_block_manager_session_cleanup(session->getWtSessionImpl()) == WT_ERROR);
        // validate_ext_list(bms, 0);
        // validate_size_list(bms, 0);
        // REQUIRE(bms == nullptr);
    }
}
