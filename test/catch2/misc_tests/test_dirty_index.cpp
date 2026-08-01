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
    ref.home = &page;
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
  dirty_index_fixture, "Dirty index: unpublished ref rejects insertion", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(!__wt_dirty_index_insert(session, btree, &ref));
    REQUIRE(btree->dirty_index->slots == nullptr);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: blocked page rejects insertion", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.home = &page;
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
    replacement.home = &page;
    replacement.page = &page;
    WT_REF_SET_STATE(&replacement, WT_REF_MEM);
    page.dirty_index_slot = WTI_DIRTY_BP_MAKE(0);
    __wt_atomic_store_ptr_release(&index->slots[0].ref, &replacement);
    __wt_dirty_index_block_page(session, btree, &ref, &page);
    REQUIRE(index->slots[0].ref == &replacement);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);

    __wt_dirty_index_block_page(session, btree, &replacement, &page);
    REQUIRE(index->slots[0].ref == nullptr);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
    __wt_dirty_index_unblock_page(&page);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: blocking an absent page succeeds", "[dirty_index]")
{
    WT_REF ref{};

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    __wt_dirty_index_block_page(session, btree, &ref, nullptr);
}

TEST_CASE_METHOD(dirty_index_fixture, "Dirty index: blocking an absent page clears its ring entry",
  "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.home = &page;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    ref.page = nullptr;
    __wt_dirty_index_block_page(session, btree, &ref, nullptr);
    REQUIRE(index->slots[0].ref == nullptr);
}

TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: retirement waits for publication and clears old ref", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.home = &page;
    ref.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));
    __wt_dirty_index_clear_page(session, btree, &ref, &page);
    page.dirty_index_slot = WTI_DIRTY_BP_MAKE(0);
    __wt_atomic_store_ptr_release(&index->slots[0].ref, nullptr);

    std::thread retire([&] { __wt_dirty_index_block_page(session, btree, &ref, &page); });
    __wt_atomic_store_ptr_release(&index->slots[0].ref, &ref);
    retire.join();

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
    old_ref.home = &page;
    replacement.home = &page;
    old_ref.page = &page;
    replacement.page = &page;
    F_SET(&old_ref, WT_REF_FLAG_LEAF);
    F_SET(&replacement, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&old_ref, WT_REF_MEM);
    WT_REF_SET_STATE(&replacement, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(__wt_dirty_index_insert(session, btree, &old_ref));

    __wt_dirty_index_block_page(session, btree, &old_ref, &page);
    REQUIRE(index->slots[0].ref == nullptr);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);

    __wt_dirty_index_unblock_page(&page);

    REQUIRE(__wt_dirty_index_insert(session, btree, &replacement));
    REQUIRE(page.dirty_index_slot != WTI_DIRTY_BP_BLOCKED);
}

/*
 * Retirement has to leave no trace of the ref in the ring, whatever the back-pointer says: the
 * caller frees it through the split stash, and a slot that still names it hands the drain a
 * dangling pointer once that generation passes.
 */
TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: retirement clears a ref the page back-pointer does not name", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF retiring{};
    WT_REF newer{};
    page.modify = &modify;
    retiring.home = newer.home = &page;
    retiring.page = newer.page = &page;
    F_SET(&retiring, WT_REF_FLAG_LEAF);
    F_SET(&newer, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&retiring, WT_REF_MEM);
    WT_REF_SET_STATE(&newer, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(__wt_dirty_index_insert(session, btree, &retiring));
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(index->slots[0].ref == &retiring);

    /* Point the back-pointer at a second slot holding a different ref for the same page. */
    __wt_atomic_store_ptr_release(&index->slots[1].ref, &newer);
    page.dirty_index_slot = WTI_DIRTY_BP_MAKE(1);

    __wt_dirty_index_block_page(session, btree, &retiring, &page);

    /* The named slot keeps its newer occupant, but the retiring ref is gone from the ring. */
    REQUIRE(index->slots[1].ref == &newer);
    REQUIRE(index->slots[0].ref == nullptr);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
}

TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: retirement clears a ref left behind by an earlier block", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF retiring{};
    page.modify = &modify;
    retiring.home = &page;
    retiring.page = &page;
    F_SET(&retiring, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&retiring, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(__wt_dirty_index_insert(session, btree, &retiring));
    WTI_DIRTY_INDEX *index = btree->dirty_index;

    /* An earlier retirement already blocked the page, so the back-pointer names nothing. */
    page.dirty_index_slot = WTI_DIRTY_BP_BLOCKED;

    __wt_dirty_index_block_page(session, btree, &retiring, &page);
    REQUIRE(index->slots[0].ref == nullptr);
}

/*
 * The drain releases a block only when the block was taken while the drained slot still owned the
 * page's back-pointer. The two orderings below are what distinguishes a retirement handshake the
 * drain must complete from a retirement that raced the pop and must stay in force.
 */
TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: drain completes the retirement handshake for a replacement ref", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF old_ref{};
    WT_REF replacement{};
    page.modify = &modify;
    old_ref.home = replacement.home = &page;
    old_ref.page = replacement.page = &page;
    F_SET(&old_ref, WT_REF_FLAG_LEAF);
    F_SET(&replacement, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&old_ref, WT_REF_MEM);
    WT_REF_SET_STATE(&replacement, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    WTI_DIRTY_INDEX *index = btree->dirty_index;
    REQUIRE(__wt_dirty_index_insert(session, btree, &replacement));
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_MAKE(0));

    /* Retiring the old ref finds slot 0 holding the replacement, so the page stays blocked. */
    __wt_dirty_index_block_page(session, btree, &old_ref, &page);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
    REQUIRE(index->slots[0].ref == &replacement);

    /* The drain pops the replacement: the block predates the pop, so the drain releases it. */
    bool cleared = __wti_dirty_index_unlink_page(&page, 0);
    REQUIRE(!cleared);
    __wti_dirty_index_release_page(&page, cleared);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);
    REQUIRE(__wt_dirty_index_insert(session, btree, &replacement));
}

TEST_CASE_METHOD(dirty_index_fixture,
  "Dirty index: drain leaves a retirement block that raced the pop", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    WT_REF retiring{};
    page.modify = &modify;
    ref.home = retiring.home = &page;
    ref.page = retiring.page = &page;
    F_SET(&ref, WT_REF_FLAG_LEAF);
    F_SET(&retiring, WT_REF_FLAG_LEAF);
    WT_REF_SET_STATE(&ref, WT_REF_MEM);
    WT_REF_SET_STATE(&retiring, WT_REF_MEM);

    REQUIRE(__wt_dirty_index_alloc(session, btree) == 0);
    REQUIRE(__wt_dirty_index_insert(session, btree, &ref));

    /* The drain pops the entry and gives up its claim on the page. */
    bool cleared = __wti_dirty_index_unlink_page(&page, 0);
    REQUIRE(cleared);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_NONE);

    /* A retirement blocks the page in the window before the drain finishes with it. */
    __wt_dirty_index_block_page(session, btree, &retiring, &page);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);

    /*
     * The drain must not release that block: doing so would let a producer publish the ref the
     * retirement is about to discard.
     */
    __wti_dirty_index_release_page(&page, cleared);
    REQUIRE(page.dirty_index_slot == WTI_DIRTY_BP_BLOCKED);
    REQUIRE(!__wt_dirty_index_insert(session, btree, &retiring));
}

TEST_CASE_METHOD(
  dirty_index_fixture, "Dirty index: duplicate insertion is suppressed", "[dirty_index]")
{
    WT_PAGE page{};
    WT_PAGE_MODIFY modify{};
    WT_REF ref{};
    page.modify = &modify;
    ref.home = &page;
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
    first_ref.home = &first_page;
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
    second_ref.home = &second_page;
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
        refs[i].home = &pages[i];
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
