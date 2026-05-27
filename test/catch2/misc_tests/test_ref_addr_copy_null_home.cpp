/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT-17638: verify that __wt_ref_addr_copy returns false safely when ref->home is
 * transiently NULL during a deepening parent split, and that __wt_ref_is_root uses a
 * proper acquire load rather than a TSAN-suppressed relaxed one.
 *
 * Race reproduced:
 *   During a deepening parent split a new WT_REF is zero-initialized (home=NULL) and
 *   its addr field is CAS'd off-page before home is written to point at the new parent.
 *   An eviction thread reading ref->home with a relaxed atomic load observed NULL and
 *   passed it to __wt_off_page, which dereferences page->dsk unconditionally,
 *   causing a SIGSEGV.
 *
 *   __wt_ref_is_root also relaxed-loads ref->home: on a leaf with a transiently-NULL
 *   home it returned true, causing __reconcile to call __rec_root_write on a leaf page
 *   and fire an ASSERT_ALWAYS.
 *
 * Fix:
 *   1. __wt_ref_addr_copy: relax -> acquire load of home + NULL guard before __wt_off_page.
 *   2. __wt_ref_is_root: replace TSAN-suppressed relaxed load with acquire load.
 *
 * How this test demonstrates the problem:
 *   The first test case calls __wt_ref_addr_copy with home=NULL and a non-NULL addr
 *   (simulating the exact split window).  On unfixed code this segfaults inside
 *   __wt_off_page.  On fixed code it returns false safely.
 *
 *   The second test case verifies that __wt_ref_is_root correctly returns true for
 *   the root ref (permanent NULL home) and false for a non-root ref.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"

/*
 * [btree][split][wt-17638]: __wt_ref_addr_copy returns false for NULL home
 *
 * Constructs the exact WT_REF state visible during the race window: home is NULL
 * (not yet written by the split) while addr already points to an off-page WT_ADDR
 * (already CAS'd in __split_ref_move).
 *
 * Before the fix: __wt_ref_addr_copy calls __wt_off_page(NULL, addr), dereferencing
 * a NULL pointer -> SIGSEGV.
 * After the fix: the NULL guard returns false before __wt_off_page is reached.
 */
TEST_CASE("WT-17638: ref_addr_copy returns false when home is NULL", "[btree][split][wt-17638]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    /*
     * __wt_ref_addr_copy requires the caller to hold the split generation so
     * the page-index memory it might access cannot be freed concurrently.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);

    /*
     * Build the synthetic WT_REF that mirrors the split window:
     *   - home = NULL   (not yet written by the split code)
     *   - addr = &addr_obj (already CAS'd to an off-page WT_ADDR)
     *
     * We use a stack-allocated WT_ADDR so the pointer is non-NULL and its
     * block_cookie_size is 0, meaning the copy path would call memcpy(..., 0)
     * which is safe.  We never reach the copy path on the fixed code anyway.
     */
    WT_ADDR addr_obj;
    memset(&addr_obj, 0, sizeof(addr_obj));
    addr_obj.type = WT_ADDR_LEAF_NO;

    WT_REF ref;
    memset(&ref, 0, sizeof(ref));
    /* Leave ref.home = NULL to reproduce the split window. */
    ref.addr = &addr_obj; /* addr is non-NULL: CAS has already run */
    F_SET(&ref, WT_REF_FLAG_LEAF);

    WT_ADDR_COPY copy;
    memset(&copy, 0, sizeof(copy));

    /*
     * Before the fix this segfaults in __wt_off_page(NULL, &addr_obj).
     * After the fix the NULL guard returns false before reaching __wt_off_page.
     */
    REQUIRE(__wt_ref_addr_copy(session, &ref, &copy) == false);

    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);
}

/*
 * [btree][split][wt-17638]: __wt_ref_is_root uses acquire load
 *
 * Before the fix __wt_ref_is_root used a TSAN-suppressed relaxed load of home.
 * Under TSAN the non-atomic write child_ref->home=child (in __split_ref_prepare)
 * raced with the relaxed read, which could manifest as NULL, causing __wt_ref_is_root
 * to return true for a leaf and trigger an ASSERT_ALWAYS in __rec_root_write.
 *
 * After the fix it uses an acquire load (__wt_atomic_load_ptr_acquire), which no
 * longer requires a TSAN suppression and provides correct acquire semantics.
 *
 * This test verifies the semantics are preserved: a ref with home=NULL is the root,
 * a ref with a non-NULL home is not.
 */
TEST_CASE("WT-17638: ref_is_root returns correct value with acquire load", "[btree][split][wt-17638]")
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
