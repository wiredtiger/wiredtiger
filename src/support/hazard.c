/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_hazard_set_func --
 *     Set a hazard pointer.
 */
int
__wt_hazard_set_func(WT_SESSION_IMPL *session, WT_REF *ref, bool *busyp
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
)
{
    WT_REF_STATE current_state;

#ifdef HAVE_DIAGNOSTIC
    WT_UNUSED(func);
    WT_UNUSED(line);
#endif

    *busyp = false;

    /* If a file can never be evicted, hazard pointers aren't required. */
    if (F_ISSET(S2BT(session), WT_BTREE_NO_EVICT))
        return (0);

    /*
     * Speculatively increment the ref's hazard pointer count before the barrier. Because WT_REF
     * objects are never freed while the tree is open, this is safe regardless of page state. The
     * barrier then orders this increment against reading the page state, eliminating the transition
     * window that would otherwise require an O(N) fallback scan on eviction.
     */
    (void)__wt_atomic_add_uint32(&ref->hp_count, 1);

    /* Publish and then check whether the page is still in memory. */
    WT_FULL_BARRIER();

    current_state = WT_REF_GET_STATE(ref);
    if (current_state == WT_REF_MEM) {
        ++session->hazards.num_active;

        /*
         * Callers require a barrier here so operations holding the hazard pointer see consistent
         * data.
         */
        WT_ACQUIRE_BARRIER();
        return (0);
    }

    /* Page not available; roll back the speculative increment. */
    (void)__wt_atomic_sub_uint32(&ref->hp_count, 1);
    *busyp = true;
    return (0);
}

/*
 * __wt_hazard_clear --
 *     Clear a hazard pointer.
 */
int
__wt_hazard_clear(WT_SESSION_IMPL *session, WT_REF *ref)
{
    /* If a file can never be evicted, hazard pointers aren't required. */
    if (F_ISSET(S2BT(session), WT_BTREE_NO_EVICT))
        return (0);

    /*
     * Release the hazard pointer. Decrement the per-ref count atomically so eviction sees the
     * change promptly. The session's active count is decremented unconditionally.
     */
    WT_ASSERT(session, ref->hp_count > 0);
    (void)__wt_atomic_sub_uint32(&ref->hp_count, 1);
    --session->hazards.num_active;
    return (0);
}

/*
 * __wt_hazard_close --
 *     Verify that no hazard pointers are set.
 */
void
__wt_hazard_close(WT_SESSION_IMPL *session)
{
    if (session->hazards.num_active == 0)
        return;

    __wt_errx(session, "session %p: close hazard pointer table: table not empty", (void *)session);

#ifdef HAVE_DIAGNOSTIC
    WT_ASSERT(session, session->hazards.num_active == 0);
#endif
}

/*
 * __wt_hazard_check_assert --
 *     Assert there's no hazard pointer to the page.
 */
bool
__wt_hazard_check_assert(WT_SESSION_IMPL *session, void *ref, bool waitfor)
{
    WT_REF *wtref;
    int i;

    wtref = (WT_REF *)ref;
    for (i = 0;;) {
        if (__wt_atomic_load_uint32_relaxed(&wtref->hp_count) == 0)
            return (true);
        if (!waitfor || ++i > 100)
            break;
        __wt_sleep(0, 10 * WT_THOUSAND);
    }
    __wt_errx(session, "hazard pointer reference to discarded object: (%p: hp_count=%" PRIu32 ")",
      ref, __wt_atomic_load_uint32_relaxed(&wtref->hp_count));
    return (false);
}
