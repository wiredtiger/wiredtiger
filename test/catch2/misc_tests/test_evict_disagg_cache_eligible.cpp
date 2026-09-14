/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstring>

#include "wt_internal.h"

/*
 * These tests exercise __evict_page_victim_cache_eligible(), the gate that decides whether a page
 * may enter the disaggregated victim cache and, if so, which on-disk-format image matches the
 * page's current block metadata. WT-18626 fixed a bug where a force-cleared reconciled page was
 * admitted because the gate only checked the dirty flag. These cases ensure the gate now follows
 * the same image-resolution logic as the dirty-eviction path.
 */

namespace {

WT_PAGE_HEADER *
make_dsk(uint32_t mem_size)
{
    WT_PAGE_HEADER *dsk;

    dsk = static_cast<WT_PAGE_HEADER *>(calloc(1, sizeof(WT_PAGE_HEADER)));
    REQUIRE(dsk != nullptr);
    dsk->mem_size = mem_size;
    return (dsk);
}

/*
 * Minimal page-log handle: eligibility only requires that caching is available and that the put
 * function pointer is non-NULL.
 */
int
dummy_plh_cache_put(WT_PAGE_LOG_HANDLE *, WT_SESSION *, uint64_t, uint64_t,
  WT_PAGE_LOG_PUT_ARGS *, const WT_ITEM *)
{
    return (0);
}

bool
dummy_plh_cache_available(WT_PAGE_LOG_HANDLE *, WT_SESSION *)
{
    return (true);
}

/*
 * EligibilityFixture --
 *     Build the smallest environment that lets us call __evict_page_victim_cache_eligible(). All
 *     members are zero-allocated; the caller tweaks the fields relevant to each test case.
 */
struct EligibilityFixture {
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    WT_DATA_HANDLE *dhandle;
    WT_BTREE *btree;
    WT_BM *bm;
    WT_BLOCK_DISAGG *block_disagg;
    WT_PAGE_LOG_HANDLE *plhandle;
    WT_REF *ref;
    WT_PAGE *page;
    WT_PAGE_DISAGG_INFO *disagg_info;
    WT_PAGE_MODIFY *modify;
    WT_PAGE_HEADER *dsk;

    EligibilityFixture()
    {
        conn = new WT_CONNECTION_IMPL();
        session = new WT_SESSION_IMPL();
        dhandle = new WT_DATA_HANDLE();
        btree = new WT_BTREE();
        bm = new WT_BM();
        block_disagg = new WT_BLOCK_DISAGG();
        plhandle = new WT_PAGE_LOG_HANDLE();
        ref = new WT_REF();
        page = new WT_PAGE();
        disagg_info = new WT_PAGE_DISAGG_INFO();
        modify = nullptr;
        dsk = make_dsk(4096);

        /*
         * Statistics checks look at the connection. Keep stats disabled so we do not need a full
         * stats array in this minimal fixture.
         */
        conn->stat_flags = 0;
        session->iface.connection = reinterpret_cast<WT_CONNECTION *>(conn);

        /* Wire up the page-log handle. */
        plhandle->plh_cache_put = dummy_plh_cache_put;
        plhandle->plh_cache_available = dummy_plh_cache_available;

        /* Wire up the block manager -> block disagg -> page log chain. */
        block_disagg->plhandle = plhandle;
        bm->block = reinterpret_cast<WT_BLOCK *>(block_disagg);

        /* Default to a valid, eligible disaggregated leaf page. */
        F_SET(btree, WT_BTREE_DISAGGREGATED);
        btree->storage_tier = WT_BTREE_STORAGE_TIER_NONE;
        btree->bm = bm;
        btree->dhandle = dhandle;

        dhandle->handle = btree;
        dhandle->checkpoint = nullptr;
        session->dhandle = dhandle;

        F_SET(ref, WT_REF_FLAG_LEAF);
        ref->home = reinterpret_cast<WT_PAGE *>(0x1); /* Any non-NULL value means not root. */
        ref->page = page;

        disagg_info->block_meta.page_id = 14606;
        page->dsk = dsk;
        page->disagg_info = disagg_info;
    }

    ~EligibilityFixture()
    {
        delete conn;
        delete session;
        delete dhandle;
        delete btree;
        delete bm;
        delete block_disagg;
        delete plhandle;
        delete ref;
        delete page;
        delete disagg_info;
        delete modify;
        free(dsk);
    }

    void
    set_replace_image(void *image)
    {
        delete modify;
        modify = new WT_PAGE_MODIFY();
        memset(modify, 0, sizeof(*modify));
        modify->rec_result = WT_PM_REC_REPLACE;
        modify->mod_disk_image = image;
        page->modify = modify;
    }

    void
    set_rec_result(uint8_t rec_result)
    {
        delete modify;
        modify = new WT_PAGE_MODIFY();
        memset(modify, 0, sizeof(*modify));
        modify->rec_result = rec_result;
        page->modify = modify;
    }
};

} // namespace

TEST_CASE(
  "Victim cache eligibility: clean leaf page with own image is eligible", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    const WT_PAGE_HEADER *disk_image = nullptr;

    REQUIRE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == f.dsk);
}

TEST_CASE("Victim cache eligibility: retained replacement image is used",
  "[evict][disagg_cache]")
{
    EligibilityFixture f;
    WT_PAGE_HEADER *new_dsk = make_dsk(6144);

    f.set_replace_image(new_dsk);

    const WT_PAGE_HEADER *disk_image = nullptr;
    REQUIRE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == reinterpret_cast<const WT_PAGE_HEADER *>(new_dsk));
    REQUIRE(disk_image != f.dsk);

    free(new_dsk);
}

TEST_CASE(
  "Victim cache eligibility: retained replacement image wins when page->dsk is null",
  "[evict][disagg_cache]")
{
    EligibilityFixture f;
    WT_PAGE_HEADER *new_dsk = make_dsk(6144);

    f.set_replace_image(new_dsk);
    f.page->dsk = nullptr;

    const WT_PAGE_HEADER *disk_image = nullptr;
    REQUIRE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == reinterpret_cast<const WT_PAGE_HEADER *>(new_dsk));

    free(new_dsk);
}

TEST_CASE("Victim cache eligibility: non-disaggregated btree is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    F_CLR(f.btree, WT_BTREE_DISAGGREGATED);

    const WT_PAGE_HEADER *disk_image = f.dsk; /* Should be reset to NULL on rejection. */
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: checkpoint-cursor btree is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.dhandle->checkpoint = "test_checkpoint";

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: internal page is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    F_CLR(f.ref, WT_REF_FLAG_LEAF);
    F_SET(f.ref, WT_REF_FLAG_INTERNAL);

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: root page is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.ref->home = nullptr;

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: cold storage tier is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.btree->storage_tier = WT_BTREE_STORAGE_TIER_COLD;

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: missing disagg_info is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.page->disagg_info = nullptr;

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: invalid page id is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.disagg_info->block_meta.page_id = WT_BLOCK_INVALID_PAGE_ID;

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: multiblock result is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.set_rec_result(WT_PM_REC_MULTIBLOCK);

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE("Victim cache eligibility: empty reconciliation is rejected", "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.set_rec_result(WT_PM_REC_EMPTY);

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}

TEST_CASE(
  "Victim cache eligibility: replacement without retained image is rejected",
  "[evict][disagg_cache]")
{
    EligibilityFixture f;
    f.set_replace_image(nullptr);

    const WT_PAGE_HEADER *disk_image = f.dsk;
    REQUIRE_FALSE(__ut_evict_page_victim_cache_eligible(f.session, f.ref, &disk_image));
    REQUIRE(disk_image == nullptr);
}
