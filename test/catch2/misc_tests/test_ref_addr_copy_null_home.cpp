/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Verify that the addr-copy helper returns false safely when the page home pointer is NULL, and
 * that the root-ref check uses a proper acquire load rather than a TSAN-suppressed relaxed one.
 *
 * Race reproduced:
 *   During a deepening parent split, addr is swapped off-page via a sequentially consistent CAS
 *   before home is updated to the new child page via a plain store. On weakly-ordered hardware a
 *   concurrent eviction thread can observe the new child page as home while addr still appears as
 *   the old on-page cell (memory ordering). The dangerous combination is a new child page whose
 *   disk image pointer is NULL: the off-page check would return true for what is actually an
 *   on-page cell, causing a garbage read of the cell as an off-page address struct.
 *
 *   Additionally, under TSAN a data race on home allows the read to return any value including
 *   zero. The NULL guard in the addr-copy helper handles this case: if home is NULL, return false
 *   rather than pass NULL to the off-page check (which dereferences the disk image pointer
 *   unconditionally).
 *
 *   The root-ref check also used a TSAN-suppressed relaxed load of home: on a leaf whose home
 *   appeared NULL under TSAN it returned true, causing reconciliation to invoke the root-write
 *   path on a leaf page and fire an ASSERT_ALWAYS.
 *
 * Fix:
 *   1. Addr-copy helper: upgrade home load to acquire, add NULL guard before off-page check.
 *   2. Root-ref check: replace TSAN-suppressed relaxed load with acquire load.
 *   3. Split code: upgrade the home write to a release store to properly pair with the acquire
 *      loads, eliminating the "new home with stale on-page addr" race on weakly-ordered hardware.
 *
 * How this test demonstrates the problem:
 *   The first test case constructs a WT_REF with home=NULL and a non-NULL addr. This exercises
 *   the NULL guard directly: on unfixed code the off-page check crashes with a NULL dereference;
 *   on fixed code it returns false safely.
 *
 *   The second test case verifies that the root-ref check returns the correct value for a ref
 *   with NULL home (root) and non-NULL home (non-root).
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"

/*
 * [btree][split]: addr-copy returns false for NULL home
 *
 * Constructs the exact WT_REF state visible during the race window: home is NULL (not yet
 * written by the split) while addr already points to an off-page WT_ADDR.
 *
 * Before the fix: the addr-copy helper calls the off-page check with a NULL page, dereferencing
 * a NULL pointer -> SIGSEGV.
 * After the fix: the NULL guard returns false before the off-page check is reached.
 */
TEST_CASE("ref_addr_copy returns false when home is NULL", "[btree][split][wt-17638]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    /*
     * The addr-copy helper requires the caller to hold the split generation so the page-index
     * memory it might access cannot be freed concurrently.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);

    /*
     * Build the synthetic WT_REF that mirrors the split window:
     *   - home = NULL   (not yet written by the split code)
     *   - addr is non-NULL (already swapped to an off-page WT_ADDR)
     *
     * We use a stack-allocated WT_ADDR so the pointer is non-NULL and its block_cookie_size is 0,
     * meaning the copy path would be a zero-length copy which is safe. We never reach the copy path
     * on the fixed code anyway.
     */
    WT_ADDR addr_obj;
    memset(&addr_obj, 0, sizeof(addr_obj));
    addr_obj.type = WT_ADDR_LEAF_NO;

    WT_REF ref;
    memset(&ref, 0, sizeof(ref));
    /* Leave home NULL to reproduce the split window. */
    /* addr is non-NULL: the atomic swap has already run. */
    ref.addr = &addr_obj;
    F_SET(&ref, WT_REF_FLAG_LEAF);

    WT_ADDR_COPY copy;
    memset(&copy, 0, sizeof(copy));

    /*
     * Before the fix this segfaults in the off-page check with a NULL page. After the fix the NULL
     * guard returns false before reaching the off-page check.
     */
    REQUIRE(__wt_ref_addr_copy(session, &ref, &copy) == false);

    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);
}

/*
 * [btree][split]: root-ref check uses acquire load
 *
 * Before the fix the root-ref check used a TSAN-suppressed relaxed load of home. Under TSAN the
 * plain write of home in the split code raced with the relaxed read, which could manifest as NULL,
 * causing the root-ref check to return true for a leaf and trigger an ASSERT_ALWAYS in the
 * root-write path.
 *
 * After the fix it uses an acquire load that no longer requires a TSAN suppression and provides
 * correct acquire semantics.
 *
 * This test verifies the semantics are preserved: a ref with home=NULL is the root, a ref with a
 * non-NULL home is not.
 */
TEST_CASE("ref_is_root returns correct value with acquire load", "[btree][split][wt-17638]")
{
    WT_REF ref;
    memset(&ref, 0, sizeof(ref));

    SECTION("NULL home is the root ref")
    {
        /* home is NULL: this is how the root ref is permanently identified. */
        REQUIRE(__wt_ref_is_root(&ref) == true);
    }

    SECTION("non-NULL home is not the root ref")
    {
        WT_PAGE dummy_page;
        memset(&dummy_page, 0, sizeof(dummy_page));
        ref.home = &dummy_page;
        REQUIRE(__wt_ref_is_root(&ref) == false);
    }
}
