/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Verify that the addr-copy helper returns false safely when the page home pointer is transiently
 * NULL during a deepening parent split, and that the root-ref check uses a proper acquire load
 * rather than a TSAN-suppressed relaxed one.
 *
 * Race reproduced:
 *   During a deepening parent split a new WT_REF is zero-initialized (home=NULL) and its addr
 *   field is swapped off-page before home is written to point at the new parent. An eviction
 *   thread reading ref->home with a relaxed atomic load observed NULL and passed it to the
 *   off-page check helper, which dereferences page->dsk unconditionally, causing a SIGSEGV.
 *
 *   The root-ref check also relaxed-loaded ref->home: on a leaf with a transiently-NULL home it
 *   returned true, causing reconciliation to invoke the root-write path on a leaf page and fire
 *   an ASSERT_ALWAYS.
 *
 * Fix:
 *   1. Addr-copy helper: upgrade home load to acquire, add NULL guard before off-page check.
 *   2. Root-ref check: replace TSAN-suppressed relaxed load with acquire load.
 *   3. Split code: upgrade the home write to a release store to properly pair with the acquire
 *      loads.
 *
 * How this test demonstrates the problem:
 *   The first test case constructs the exact race-window WT_REF state (home=NULL, addr non-NULL).
 *   On unfixed code this segfaults inside the off-page check. On fixed code it returns false.
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
     * __wt_ref_addr_copy requires the caller to hold the split generation so the page-index memory
     * it might access cannot be freed concurrently.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);

    /*
     * Build the synthetic WT_REF that mirrors the split window:
     *   - home = NULL   (not yet written by the split code)
     *   - addr = &addr_obj (already swapped to an off-page WT_ADDR)
     *
     * We use a stack-allocated WT_ADDR so the pointer is non-NULL and its block_cookie_size is 0,
     * meaning the copy path would call memcpy(..., 0) which is safe. We never reach the copy path
     * on the fixed code anyway.
     */
    WT_ADDR addr_obj;
    memset(&addr_obj, 0, sizeof(addr_obj));
    addr_obj.type = WT_ADDR_LEAF_NO;

    WT_REF ref;
    memset(&ref, 0, sizeof(ref));
    /* Leave ref.home = NULL to reproduce the split window. */
    ref.addr = &addr_obj; /* addr is non-NULL: the atomic swap has already run */
    F_SET(&ref, WT_REF_FLAG_LEAF);

    WT_ADDR_COPY copy;
    memset(&copy, 0, sizeof(copy));

    /*
     * Before the fix this segfaults in __wt_off_page(NULL, &addr_obj). After the fix the NULL guard
     * returns false before reaching __wt_off_page.
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
        /* ref.home = NULL: this is how the root ref is permanently identified. */
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
