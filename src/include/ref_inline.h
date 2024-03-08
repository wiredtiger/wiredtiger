/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __wt_ref_is_root --
 *     Return if the page reference is for the root page.
 */
static WT_INLINE bool
__wt_ref_is_root(WT_REF *ref)
{
    return (ref->home == NULL);
}

/*
 * __ref_set_state --
 *     Set a ref's state. Accessed from the WT_REF_SET_STATE_MACRO.
 */
static WT_INLINE void
__ref_set_state(WT_REF *ref, uint8_t state)
{
    WT_RELEASE_WRITE_WITH_BARRIER(ref->__state, state);
}

#ifndef HAVE_REF_TRACK
#define WT_REF_SET_STATE(ref, s) __ref_set_state((ref), (s))
#else
/*
 * __ref_track_state --
 *     Save tracking data when REF_TRACK is enabled. This function wraps the WT_REF_SAVE_STATE macro
 *     so we can suppress it in our TSan ignore list.
 */
static inline void
__ref_track_state(
  WT_SESSION_IMPL *session, WT_REF *ref, uint8_t new_state, const char *func, int line)
{
    ref->hist[ref->histoff].session = session;
    ref->hist[ref->histoff].name = session->name;
    __wt_seconds32(session, &ref->hist[ref->histoff].time_sec);
    ref->hist[ref->histoff].func = func;
    ref->hist[ref->histoff].line = (uint16_t)line;
    ref->hist[ref->histoff].state = (uint16_t)(new_state);
    ref->histoff = (ref->histoff + 1) % WT_ELEMENTS(ref->hist);
}
#define WT_REF_SET_STATE(ref, s)                                           \
    do {                                                                   \
        __ref_track_state(session, ref, s, __PRETTY_FUNCTION__, __LINE__); \
        __ref_set_state((ref), (s));                                       \
    } while (0)
#endif

/*
 *  __wt_ref_get_state --
 *     Get a ref's state variable safely.
 */
static WT_INLINE uint8_t
__wt_ref_get_state(WT_REF *ref)
{
    return (__wt_atomic_loadv8(&ref->__state));
}

/*
 * __ref_cas_state --
 *     Try to do a compare and swap, if successful update the ref history in diagnostic mode.
 */
static WT_INLINE bool
__ref_cas_state(WT_SESSION_IMPL *session, WT_REF *ref, uint8_t old_state, uint8_t new_state,
  const char *func, int line)
{
    bool cas_result;

    /* Parameters that are used in a macro for diagnostic builds */
    WT_UNUSED(session);
    WT_UNUSED(func);
    WT_UNUSED(line);

    cas_result = __wt_atomic_casv8(&ref->__state, old_state, new_state);

#ifdef HAVE_REF_TRACK
    /*
     * The history update here has potential to race; if the state gets updated again after the CAS
     * above but before the history has been updated.
     */
    if (cas_result)
        __ref_track_state(session, ref, new_state, func, line);
#endif
    return (cas_result);
}

/* A macro wrapper allowing us to remember the callers code location */
#define WT_REF_CAS_STATE(session, ref, old_state, new_state) \
    __ref_cas_state(session, ref, old_state, new_state, __PRETTY_FUNCTION__, __LINE__)


/* __wt_ref_lock --
 *     Spin until succesfully locking the ref. Return the previous state to the caller.
 */
static WT_INLINE void
__wt_ref_lock(WT_SESSION_IMPL *session, WT_REF *ref, uint8_t *previous_statep) {
    uint8_t previous_state;
    for (;; __wt_yield()) {
        previous_state = __wt_ref_get_state(ref);
        if (previous_state != WT_REF_LOCKED &&
              WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
            break;
    }
    *(previous_statep) = previous_state;
}

#define WT_REF_UNLOCK(ref, state) WT_REF_SET_STATE(ref, state)
