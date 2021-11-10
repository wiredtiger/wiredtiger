/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_HAZARD_WEAK_FORALL(s, wha, whp)                     \
    for (wha = (s)->hazard_weak; wha != NULL; wha = wha->next) \
        for (whp = wha->hazard; whp < wha->hazard + wha->hazard_inuse; whp++)

/*
 * __wt_hazard_weak_close --
 *     Verify that no weak hazard pointers are set.
 */
void
__wt_hazard_weak_close(WT_SESSION_IMPL *session)
{
    WT_HAZARD_WEAK *whp;
    WT_HAZARD_WEAK_ARRAY *wha;
    uint32_t nhazard_weak;
    bool found;

    /*
     * Check for a set weak hazard pointer and complain if we find one. We could just check the
     * session's weak hazard pointer count, but this is a useful diagnostic.
     */
    for (found = false, nhazard_weak = 0, wha = session->hazard_weak; wha != NULL;
         nhazard_weak += wha->nhazard, wha = wha->next)
        for (whp = wha->hazard; whp < wha->hazard + wha->hazard_inuse; whp++)
            if (whp->ref != NULL) {
                found = true;
                break;
            }

    if (nhazard_weak == 0 && !found)
        return;

    __wt_errx(
      session, "session %p: close weak hazard pointer table: table not empty", (void *)session);

    WT_HAZARD_WEAK_FORALL(session, wha, whp)
    if (whp->ref != NULL) {
        whp->ref = NULL;
        --wha->nhazard;
        --nhazard_weak;
    }

    if (nhazard_weak != 0)
        __wt_errx(session,
          "session %p: close weak hazard pointer table: count didn't match entries",
          (void *)session);
}

/*
 * hazard_weak_grow --
 *     Grow a weak hazard pointer array.
 */
static int
hazard_weak_grow(WT_SESSION_IMPL *session)
{
    WT_HAZARD_WEAK_ARRAY *wha;
    size_t size;

    /*
     * Allocate a new, larger hazard pointer array and link it into place.
     */
    size = session->hazard_weak->hazard_size;
    WT_RET(__wt_calloc(
      session, sizeof(WT_HAZARD_WEAK_ARRAY) + 2 * size * sizeof(WT_HAZARD_WEAK), 1, &wha));
    wha->next = session->hazard_weak;
    wha->hazard_size = (uint32_t)(size * 2);
    WT_PUBLISH(session->hazard_weak, wha);

    /*
     * Swap the new hazard pointer array into place after initialization is complete (initialization
     * must complete before eviction can see the new hazard pointer array).
     */
    WT_PUBLISH(session->hazard_weak, wha);

    return (0);
}

/*
 * __wt_hazard_weak_destroy --
 *     Free all memory associated with weak hazard pointers
 */
void
__wt_hazard_weak_destroy(WT_SESSION_IMPL *session_safe, WT_SESSION_IMPL *s)
{
    WT_HAZARD_WEAK_ARRAY *wha, *next;

    for (wha = s->hazard_weak; wha != NULL; wha = next) {
        next = wha->next;
        __wt_free(session_safe, wha);
    }
}

/*
 * __wt_hazard_weak_set --
 *     Set a weak hazard pointer. A hazard pointer must be held on the ref.
 */
int
__wt_hazard_weak_set(WT_SESSION_IMPL *session, WT_REF *ref, WT_HAZARD_WEAK **whpp)
{
    WT_HAZARD_WEAK *whp;
    WT_HAZARD_WEAK_ARRAY *wha;

    WT_ASSERT(session, ref != NULL);

    if (whpp != NULL)
        *whpp = NULL;

    /* If we have filled the current hazard pointer array, grow it. */
    for (wha = session->hazard_weak; wha != NULL && wha->nhazard >= wha->hazard_size;
         wha = wha->next)
        WT_ASSERT(
          session, wha->nhazard == wha->hazard_size && wha->hazard_inuse == wha->hazard_size);

    if (wha == NULL) {
        WT_RET(hazard_weak_grow(session));
        wha = session->hazard_weak;
    }

    /*
     * If there are no available hazard pointer slots, make another one visible.
     */
    if (wha->nhazard >= wha->hazard_inuse) {
        WT_ASSERT(
          session, wha->nhazard == wha->hazard_inuse && wha->hazard_inuse < wha->hazard_size);
        whp = &wha->hazard[wha->hazard_inuse++];
    } else {
        WT_ASSERT(
          session, wha->nhazard < wha->hazard_inuse && wha->hazard_inuse <= wha->hazard_size);

        /*
         * There must be an empty slot in the array, find it. Skip most of the active slots by
         * starting after the active count slot; there may be a free slot before there, but checking
         * is expensive. If we reach the end of the array, continue the search from the beginning of
         * the array.
         */
        for (whp = wha->hazard + wha->nhazard;; ++whp) {
            if (whp >= wha->hazard + wha->hazard_inuse)
                whp = wha->hazard;
            if (whp->ref == NULL)
                break;
        }
    }

    ++wha->nhazard;

    WT_ASSERT(session, whp->ref == NULL && whp->count == 0);

    /*
     * We rely on a hazard pointer protecting the ref, so for weak hazard pointers this is much
     * simpler than the regular hazard pointer case.
     */
    whp->ref = ref;
    whp->valid = true;
    whp->count = 1;

    WT_ASSERT(session, whpp != NULL);
    *whpp = whp;
    return (0);
}
