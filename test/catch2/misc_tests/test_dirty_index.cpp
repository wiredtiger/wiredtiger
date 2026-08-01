/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include <thread>

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

TEST_CASE_METHOD(dirty_index_fixture, "Dirty index: blocked page rejects insertion", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    page.dirty_index_slot = WTI_DIRTY_BP_BLOCKED;
    REQUIRE(!__wt_dirty_index_insert(session, btree, &ref));
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
    __wt_dirty_index_unblock_page(&page);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);

    WT_REF replacement{};
    replacement.page = &page;
    WT_REF_SET_STATE(&replacement, WT_REF_MEM);
    page.dirty_index_slot = WTI_DIRTY_BP_MAKE(0);
    __wt_atomic_store_ptr_release(&index->slots[0].ref, &replacement);
    REQUIRE(__wt_dirty_index_block_page(session, btree, &ref, &page));
    REQUIRE(index->slots[0].ref == &replacement);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: retirement waits for publication and clears old ref", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    page.dirty_index_slot = WTI_DIRTY_BP_MAKE(0);
    __wt_atomic_store_ptr_release(&index->slots[0].ref, nullptr);

    bool blocked = false;
    std::thread retire([&] { blocked = __wt_dirty_index_block_page(session, btree, &ref, &page); });
    __wt_atomic_store_ptr_release(&index->slots[0].ref, &ref);
    retire.join();

    REQUIRE(blocked);
    REQUIRE(index->slots[0].ref == nullptr);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
    __wt_dirty_index_unblock_page(&page);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);
}
