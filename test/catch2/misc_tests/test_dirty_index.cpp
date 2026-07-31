/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "../wrappers/mock_session.h"

extern "C" {
#include "wt_internal.h"
#include "../../../src/evict/evict.h"
}

namespace {

struct dirty_index_fixture {
    std::shared_ptr<mock_session> mock;
    WT_SESSION_IMPL *session;
    WT_BTREE *btree;
    WT_EVICT evict;

    dirty_index_fixture() : mock(mock_session::build_test_mock_session()), evict{}
    {
        mock->setup_block_manager_file_operations();
        session = mock->get_wt_session_impl();
        btree = S2BT(session);
        btree->dhandle = session->dhandle;
        S2C(session)->evict = &evict;
        evict.eviction_dirty_index = true;
    }

    ~dirty_index_fixture()
    {
        __wt_dirty_index_destroy(session, btree);
    }
};

} // namespace

TEST_CASE_METHOD(dirty_index_fixture, "Dirty index: eager allocation", "[dirty_index]")
{
    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);

    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(index != nullptr);
    REQUIRE(index->slots != nullptr);
    REQUIRE(index->capacity == WTI_DIRTY_INDEX_MIN_CAPACITY);
    REQUIRE(index->mask == index->capacity - 1);
    REQUIRE(index->head == 0);
    REQUIRE(index->tail == 0);
}
