/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [extent_list]: block_ext.c
 * Test the by-size bucket count kept on an extent list.
 *
 * WT_EXTLIST.size_buckets must always equal the number of entries on the by-size skiplist, because
 * reporting it is what lets the statistics path avoid walking that list. These tests drive the real
 * insert and remove paths rather than hand-building the lists, so they exercise the accounting
 * itself.
 */

#include <catch2/catch.hpp>
#include <memory>

#include "test_util.h"
#include "wt_internal.h"
#include "../utils_extlist.h"
#include "../../utils.h"
#include "../../wrappers/mock_session.h"

using namespace utils;

namespace {
/* Count the by-size skiplist the long way, which is what the counter has to agree with. */
uint32_t
walk_buckets(const WT_EXTLIST &el)
{
    uint32_t n = 0;
    for (WT_SIZE *szp = el.sz[0]; szp != nullptr; szp = szp->next[0])
        ++n;
    return (n);
}

void
insert(WT_SESSION_IMPL *session, WT_EXTLIST &el, wt_off_t off, wt_off_t size)
{
    WT_EXT *ext = alloc_new_ext(session, off, size);
    REQUIRE(ext != nullptr);
    REQUIRE(__ut_block_ext_insert(session, &el, ext) == 0);
}
} // namespace

TEST_CASE("Extent Lists: size_buckets accounting", "[extent_list]")
{
    std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();

    /*
     * The WT_BLOCK is only here because the extent list code reports corruption through it. These
     * tests never corrupt a list, but WT_BLOCK_RET panics the connection rather than returning when
     * "verify" is clear, which would abort the whole test binary instead of failing this test. Set
     * it so that an unexpected failure is reported as a failed REQUIRE.
     */
    WT_BLOCK block = {};
    block.verify = true;

    WT_EXTLIST el = {};
    REQUIRE(__wti_block_extlist_init(session, &el, "test", "avail", true) == 0);

    SECTION("a fresh list has no buckets")
    {
        REQUIRE(el.size_buckets == 0);
        REQUIRE(walk_buckets(el) == 0);
    }

    SECTION("distinct sizes each add a bucket")
    {
        insert(session, el, 4096, 4096);
        insert(session, el, 16384, 8192);
        insert(session, el, 32768, 12288);

        REQUIRE(el.entries == 3);
        REQUIRE(el.size_buckets == 3);
        REQUIRE(walk_buckets(el) == 3);
    }

    SECTION("extents sharing a size share a bucket")
    {
        insert(session, el, 4096, 4096);
        insert(session, el, 16384, 4096);
        insert(session, el, 32768, 4096);

        REQUIRE(el.entries == 3);
        REQUIRE(el.size_buckets == 1);
        REQUIRE(walk_buckets(el) == 1);
    }

    SECTION("a bucket survives until its last extent is removed")
    {
        insert(session, el, 4096, 4096);
        insert(session, el, 16384, 4096);
        REQUIRE(el.size_buckets == 1);

        /* Removing one of two same-sized extents must not drop the bucket. */
        REQUIRE(__ut_block_off_remove(session, &block, &el, 4096, nullptr) == 0);
        REQUIRE(el.entries == 1);
        REQUIRE(el.size_buckets == 1);
        REQUIRE(walk_buckets(el) == 1);

        /* Removing the last one must. */
        REQUIRE(__ut_block_off_remove(session, &block, &el, 16384, nullptr) == 0);
        REQUIRE(el.entries == 0);
        REQUIRE(el.size_buckets == 0);
        REQUIRE(walk_buckets(el) == 0);
    }

    SECTION("removing a middle size leaves the others counted")
    {
        insert(session, el, 4096, 4096);
        insert(session, el, 16384, 8192);
        insert(session, el, 32768, 12288);
        REQUIRE(el.size_buckets == 3);

        REQUIRE(__ut_block_off_remove(session, &block, &el, 16384, nullptr) == 0);
        REQUIRE(el.size_buckets == 2);
        REQUIRE(walk_buckets(el) == 2);
    }

    SECTION("a re-inserted extent of a new size moves the count")
    {
        WT_EXT *ext = nullptr;

        insert(session, el, 4096, 4096);
        REQUIRE(el.size_buckets == 1);

        /*
         * This is what a partial allocation does: take the extent out, shrink it, put it back. The
         * old size's bucket goes away and the new size's bucket appears.
         */
        REQUIRE(__ut_block_off_remove(session, &block, &el, 4096, &ext) == 0);
        REQUIRE(el.size_buckets == 0);
        ext->off += 1024;
        ext->size -= 1024;
        REQUIRE(__ut_block_ext_insert(session, &el, ext) == 0);
        REQUIRE(el.size_buckets == 1);
        REQUIRE(walk_buckets(el) == 1);
    }

    SECTION("coalescing neighbors can reduce the bucket count")
    {
        /*
         * Two extents of different sizes with a gap between them, then the gap is freed: the three
         * coalesce into one extent of a third size, so the bucket count goes down even as extents
         * are returned to the list.
         */
        insert(session, el, 4096, 4096);
        insert(session, el, 12288, 8192);
        REQUIRE(el.size_buckets == 2);
        REQUIRE(el.entries == 2);

        REQUIRE(__ut_block_merge(session, &block, &el, 8192, 4096) == 0);

        REQUIRE(el.entries == 1);
        REQUIRE(el.size_buckets == 1);
        REQUIRE(walk_buckets(el) == 1);
        REQUIRE(el.off[0]->off == 4096);
        REQUIRE(el.off[0]->size == 16384);
    }

    SECTION("lists that do not track sizes never count buckets")
    {
        el.track_size = false;

        insert(session, el, 4096, 4096);
        insert(session, el, 16384, 8192);

        REQUIRE(el.entries == 2);
        REQUIRE(el.size_buckets == 0);
        REQUIRE(walk_buckets(el) == 0);
    }

    __wti_block_extlist_free(session, &el);
}

TEST_CASE("Extent Lists: size_buckets across extlist_merge", "[extent_list]")
{
    std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();

    /* See the note on the WT_BLOCK in the test case above. */
    WT_BLOCK block = {};
    block.verify = true;

    WT_EXTLIST src = {}, dst = {};
    src.track_size = dst.track_size = true;

    SECTION("merging swaps the bucket count with the lists")
    {
        /*
         * The source has more entries than the destination, so __wti_block_extlist_merge swaps the
         * two lists before merging. The bucket count must move with the skiplists it describes.
         */
        insert(session, src, 4096, 4096);
        insert(session, src, 16384, 8192);
        insert(session, src, 40960, 12288);
        insert(session, dst, 81920, 16384);

        REQUIRE(__wti_block_extlist_merge(session, &block, &src, &dst) == 0);

        /* Nothing is adjacent, so all four sizes survive as four buckets. */
        REQUIRE(dst.entries == 4);
        REQUIRE(dst.size_buckets == 4);
        REQUIRE(walk_buckets(dst) == 4);

        /* The swapped-out list must still describe whatever it now holds. */
        REQUIRE(src.size_buckets == walk_buckets(src));
    }

    SECTION("merging without a swap keeps both counts honest")
    {
        insert(session, src, 4096, 4096);
        insert(session, dst, 16384, 8192);
        insert(session, dst, 40960, 12288);
        insert(session, dst, 81920, 16384);

        REQUIRE(__wti_block_extlist_merge(session, &block, &src, &dst) == 0);

        REQUIRE(dst.entries == 4);
        REQUIRE(dst.size_buckets == 4);
        REQUIRE(walk_buckets(dst) == 4);
        REQUIRE(src.size_buckets == walk_buckets(src));
    }

    /*
     * __wti_block_extlist_merge swaps the skiplists but not the cached end-of-list pointer, so
     * after a swap "last" can refer to an extent now owned by the other list. That cache is only
     * used when appending to lists that do not track sizes, and __wti_block_extlist_free never
     * reads it, but the test helper frees it explicitly: clear it so each extent is freed exactly
     * once, through the skiplist that owns it.
     */
    src.last = dst.last = nullptr;
    extlist_free(session, src);
    extlist_free(session, dst);
}
