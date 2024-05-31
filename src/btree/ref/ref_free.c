/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
/*
 * __wt_ref_out --
 *     Discard an in-memory page, freeing all memory associated with it.
 */
void
__wt_ref_out(WT_SESSION_IMPL *session, WT_REF *ref)
{
    /*
     * A version of the page-out function that allows us to make additional diagnostic checks.
     *
     * The WT_REF cannot be the eviction thread's location.
     */
    WT_ASSERT(session, S2BT(session)->evict_ref != ref);

    /*
     * Make sure no other thread has a hazard pointer on the page we are about to discard. This is
     * complicated by the fact that readers publish their hazard pointer before re-checking the page
     * state, so our check can race with readers without indicating a real problem. If we find a
     * hazard pointer, wait for it to be cleared.
     */
    WT_ASSERT_OPTIONAL(session, WT_DIAGNOSTIC_EVICTION_CHECK,
      __wt_hazard_check_assert(session, ref, true),
      "Attempted to free a page with active hazard pointers");

    /* Check we are not evicting an accessible internal page with an active split generation. */
    WT_ASSERT(session,
      !F_ISSET(ref, WT_REF_FLAG_INTERNAL) ||
        F_ISSET(session->dhandle, WT_DHANDLE_DEAD | WT_DHANDLE_EXCLUSIVE) ||
        !__wt_gen_active(session, WT_GEN_SPLIT, ref->page->pg_intl_split_gen));

    __wt_page_out(session, &ref->page);
}

/*
 * __wti_free_ref_index --
 *     Discard a page index and its references.
 */
void
__wti_free_ref_index(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_PAGE_INDEX *pindex, bool free_pages)
{
    WT_REF *ref;
    uint32_t i;

    if (pindex == NULL)
        return;

    WT_ASSERT_ALWAYS(session, !__wt_page_is_reconciling(page),
      "Attempting to discard ref to a page being reconciled");

    for (i = 0; i < pindex->entries; ++i) {
        ref = pindex->index[i];

        /*
         * Used when unrolling splits and other error paths where there should never have been a
         * hazard pointer taken.
         */
        WT_ASSERT_OPTIONAL(session, WT_DIAGNOSTIC_EVICTION_CHECK,
          __wt_hazard_check_assert(session, ref, false),
          "Attempting to discard ref to a page with hazard pointers");

        __wti_free_ref(session, ref, page->type, free_pages);
    }
    __wt_free(session, pindex);
}

/*
 * __wti_free_ref --
 *     Discard the contents of a WT_REF structure (optionally including the pages it references).
 */
void
__wti_free_ref(WT_SESSION_IMPL *session, WT_REF *ref, int page_type, bool free_pages)
{
    WT_IKEY *ikey;

    if (ref == NULL)
        return;

    /*
     * We create WT_REFs in many places, assert a WT_REF has been configured as either an internal
     * page or a leaf page, to catch any we've missed.
     */
    WT_ASSERT(session, F_ISSET(ref, WT_REF_FLAG_INTERNAL) || F_ISSET(ref, WT_REF_FLAG_LEAF));

    /*
     * Optionally free the referenced pages. (The path to free referenced page is used for error
     * cleanup, no instantiated and then discarded page should have WT_REF entries with real pages.
     * The page may have been marked dirty as well; page discard checks for that, so we mark it
     * clean explicitly.)
     */
    if (free_pages && ref->page != NULL) {
        WT_ASSERT_ALWAYS(session, !__wt_page_is_reconciling(ref->page),
          "Attempting to discard ref to a page being reconciled");
        __wt_page_modify_clear(session, ref->page);
        __wt_page_out(session, &ref->page);
    }

    /*
     * Optionally free row-store WT_REF key allocation. Historic versions of this code looked in a
     * passed-in page argument, but that is dangerous, some of our error-path callers create WT_REF
     * structures without ever setting WT_REF.home or having a parent page to which the WT_REF will
     * be linked. Those WT_REF structures invariably have instantiated keys, (they obviously cannot
     * be on-page keys), and we must free the memory.
     */
    switch (page_type) {
    case WT_PAGE_ROW_INT:
    case WT_PAGE_ROW_LEAF:
        if ((ikey = __wt_ref_key_instantiated(ref)) != NULL)
            __wt_free(session, ikey);
        break;
    }

    /* Free any address allocation. */
    __wt_ref_addr_free(session, ref);

    /* Free any backing fast-truncate memory. */
    __wt_free(session, ref->page_del);

    __wt_overwrite_and_free_len(session, ref, WT_REF_CLEAR_SIZE);
}

