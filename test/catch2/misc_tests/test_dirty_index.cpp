/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include <algorithm>
#include <array>
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

TEST_CASE_METHOD(dirty_index_fixture, "Dirty index: descriptor allocation", "[dirty_index]")
{
    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);

    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(index != nullptr);
    REQUIRE(index->slots == nullptr);
    REQUIRE(index->capacity == WTI_DIRTY_INDEX_MIN_CAPACITY);
    REQUIRE(index->mask == index->capacity - 1);
    REQUIRE(index->head == 0);
    REQUIRE(index->tail == 0);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: first qualifying insert allocates slots", "[dirty_index]")
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
    REQUIRE(index->slots == nullptr);
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    REQUIRE(index->slots != nullptr);
    REQUIRE(index->slots[0].ref == &ref);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: blocked page rejects insertion", "[dirty_index]")
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
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    __wt_dirty_index_clear_page(session, btree, &ref, &page);
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
  dirty_index_fixture, "Dirty index: blocking an absent page succeeds", "[dirty_index]")
{
    WT_REF ref{};

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(__wt_dirty_index_block_page(session, btree, &ref, nullptr));
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: blocking an absent page clears its ring entry", "[dirty_index]")
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
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    ref.page = nullptr;
    REQUIRE(__wt_dirty_index_block_page(session, btree, &ref, nullptr));
    REQUIRE(index->slots[0].ref == nullptr);
}

TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: retirement waits for publication and clears old ref", "[dirty_index]")
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
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    __wt_dirty_index_clear_page(session, btree, &ref, &page);
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

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: retained split page can be indexed", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF old_ref{};
    WT_REF replacement{};
    page.modify = &modify;
    old_ref.page = &page;
    replacement.page = &page;
    F_SET(&old_ref, WT_REF_FLAG_LEAF);
    F_SET(&replacement, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&old_ref, WT_REF_MEM);
    WT_REF_SET_STATE(&replacement, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(__wt_dirty_index_insert(session, btree, &old_ref));

    REQUIRE(__wt_dirty_index_block_page(session, btree, &old_ref, &page));
    REQUIRE(index->slots[0].ref == nullptr);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);

    __wt_dirty_index_unblock_page(&page);

    REQUIRE(__wt_dirty_index_insert(session, btree, &replacement));
    REQUIRE(page.dirty_index_slot != WTI_DIRTY_BP_BLOCKED);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: duplicate insertion is suppressed", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    uint64_t head = index->head;
    REQUIRE(!__wt_dirty_index_insert(session, btree, &ref));
    REQUIRE(index->head == head);
}

TEST_CASE_METHOD(dirty_index_fixture, "Dirty index: runtime disable and re-enable uses fresh pages",
  "[dirty_index]")
{
    WT_PAGE first_page{};
    WT_PAGE_MODIFY first_modify{};
    WT_REF first_ref{};
    first_page.modify = &first_modify;
    first_ref.page = &first_page;
    F_SET(&first_ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&first_ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    S2C(session)->evict->eviction_dirty_index = false;
    REQUIRE(!__wt_dirty_index_insert(session, btree, &first_ref));
    REQUIRE(btree->dirty_index->slots == nullptr);

    S2C(session)->evict->eviction_dirty_index = true;
    REQUIRE(__wt_dirty_index_insert(session, btree, &first_ref));
    REQUIRE(btree->dirty_index->slots != nullptr);

    WT_PAGE second_page{};
    WT_PAGE_MODIFY second_modify{};
    WT_REF second_ref{};
    second_page.modify = &second_modify;
    second_ref.page = &second_page;
    F_SET(&second_ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&second_ref, WT_REF_MEM);
    REQUIRE(__wt_dirty_index_insert(session, btree, &second_ref));
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: concurrent first-use allocation", "[dirty_index]")
{
    constexpr size_t thread_count = 8;
    std::array<WT_PAGE, thread_count> pages{};
    std::array<WT_PAGE_MODIFY, thread_count> modifies{};
    std::array<WT_REF, thread_count> refs{};
    std::array<bool, thread_count> inserted{};
    std::array<std::thread, thread_count> threads;

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    for (size_t i = 0; i < thread_count; ++i) {
        pages[i].modify = &modifies[i];
        refs[i].page = &pages[i];
        F_SET(&refs[i], WT_REF_FLAG_LEAF);
        WT_REF_SET_STATE(&refs[i], WT_REF_MEM);
        threads[i] =
          std::thread([&, i] { inserted[i] = __wt_dirty_index_insert(session, btree, &refs[i]); });
    }
    for (auto &thread : threads)
        thread.join();

    REQUIRE(btree->dirty_index->slots != nullptr);
    REQUIRE(std::count(inserted.begin(), inserted.end(), true) == thread_count);
}
