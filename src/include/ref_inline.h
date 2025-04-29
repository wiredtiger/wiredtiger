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
 * # The ref state API. #
 *
 * 5 macros are defined to manipulate the ref state. This is a highly sensitive field and protected
 * via the double underscore keyword. The field should only be accessed via these macros.
 *
 * WT_REF_GET_STATE:
 * Get the state of the ref, wraps a relaxed atomic volatile load. At the time of writing this
 * comment this was done to enable TSan and to enable burying the field behind the
 * aforementioned double underscore.
 *
 * WT_REF_SET_STATE:
 * Set the ref state. If HAVE_REF_TRACK is defined, track where the set call originated from. The
 * ref state tracking is why we use macros here, since the tracking utilizes gcc identifiers to get
 * the function and line number where the macro was called.
 *
 * WT_REF_CAS_STATE:
 * Swap in a new state to the ref, tracking where the call originated from.
 *
 * WT_REF_LOCK:
 * Spin until the state WT_REF_LOCKED is swapped into the ref state field. Once the call to this
 * function completes the caller has exclusive access to the ref.
 *
 * WT_REF_UNLOCK:
 * Effectively wraps WT_REF_SET_STATE, however should only be used when returning the ref to the
 * previous state as returned by WT_REF_LOCK.
 */

/*
 * __ref_set_state --
 *     Set a ref's state. Accessed from the WT_REF_SET_STATE macro.
 */
static WT_INLINE void
__ref_set_state(WT_REF *ref, WT_REF_STATE state)
{
    WT_RELEASE_WRITE_WITH_BARRIER(ref->__state, state);
}

#ifndef HAVE_REF_TRACK
#define WT_REF_SET_STATE(ref, s) __ref_set_state((ref), (s))
#else
/*
 * __ref_track_state --
 *     Save tracking data when REF_TRACK is enabled. This is diagnostic code and ref->state changes
 *     are a hot path. As such we allow some racing in the history tracking code instead of
 *     requiring a lock and slowing down ref state transitions.
 */
static WT_INLINE void
__ref_track_state(
  WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE new_state, const char *func, int line)
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

static WT_INLINE void
__wt_ref_make_visible(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref) {
	printf("session %d BEFORE make_visible for page %p, bucket %p\n", (int)session->id,
		   (void*)ref->page, (void*)ref->page->evict_data.bucket);
	fflush(stdout);
	__wt_evict_touch_page(session, dhandle, ref, false, false);
	printf("session %d AFTER make_visible for page %p, bucket %p\n", (int)session->id,
		   (void*)ref->page, (void*)ref->page->evict_data.bucket);
	fflush(stdout);
	__atomic_store_n(&ref->owner, 0, __ATOMIC_RELEASE);
	WT_REF_SET_STATE(ref, WT_REF_MEM);
}

/*
 * __ref_get_state --
 *     Get a ref's state variable safely.
 */
static WT_INLINE WT_REF_STATE
__ref_get_state(WT_REF *ref)
{
    return (__wt_atomic_loadv8(&ref->__state));
}

/*
 * __ref_get_state_strict --
 *     Get a ref's state variable with sequential consistency ordering.
 */
static WT_INLINE WT_REF_STATE
__ref_get_state_strict(WT_REF *ref)
{
    return (__atomic_load_n(&ref->__state, __ATOMIC_ACQUIRE));
}


#define WT_REF_GET_STATE_STRICT(ref) __ref_get_state_strict((ref))
#define WT_REF_GET_STATE(ref) __ref_get_state((ref))
#define WT_REF_OWNER(ref) (__atomic_load_n(&ref->owner, __ATOMIC_ACQUIRE))

/*
 * __ref_cas_state --
 *     Try to do a compare and swap, if successful update the ref history in diagnostic mode.
 */
static WT_INLINE bool
__ref_cas_state(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE old_state,
  WT_REF_STATE new_state, const char *func, int line)
{
    bool cas_result;

    /* Parameters that are used in a macro for diagnostic builds */
    WT_UNUSED(session);
    WT_UNUSED(func);
    WT_UNUSED(line);

	/* If we have the reference locked and we are about to unlock it, reset the owner first */
	if (old_state == WT_REF_LOCKED && new_state != WT_REF_LOCKED &&
		WT_REF_OWNER(ref) == (uint64_t)session)
		__atomic_store_n(&ref->owner, 0, __ATOMIC_RELEASE);

    cas_result = __wt_atomic_casv8(&ref->__state, old_state, new_state);

#ifdef HAVE_REF_TRACK
    /*
     * The history update here has potential to race; if the state gets updated again after the CAS
     * above but before the history has been updated.
     */
    if (cas_result)
        __ref_track_state(session, ref, new_state, func, line);
#endif
	if (cas_result && new_state == WT_REF_LOCKED)
		__atomic_store_n(&ref->owner, (uint64_t)session, __ATOMIC_RELEASE);

#if 0
	if (cas_result) {
		printf("session %d SUCCESS to CAS STATE from %d to %d on page %p,  func %s, line %d\n",
			   (int)session->id, old_state, new_state, (ref->page == NULL)? 0 : (void*)ref->page,
			   func, line);
	}
	else
		printf("session %d FAIL to CAS STATE from %d to %d on page %p,  func %s, line %d\n", (int)session->id,
			   old_state, new_state, (ref->page == NULL)? 0 : (void*)ref->page, func, line);
	fflush(stdout);
#endif

    return (cas_result);
}

/* A macro wrapper allowing us to remember the callers code location */
#define WT_REF_CAS_STATE(session, ref, old_state, new_state) \
    __ref_cas_state(session, ref, old_state, new_state, __PRETTY_FUNCTION__, __LINE__)

/*
 * __ref_lock --
 *     Spin until successfully locking the ref. Return the previous state to the caller.
 */
static WT_INLINE void
__ref_lock(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE *previous_statep)
{
    WT_REF_STATE previous_state;
    for (;; __wt_yield()) {
        previous_state = WT_REF_GET_STATE_STRICT(ref);
        if (previous_state != WT_REF_LOCKED &&
          WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
            break;
    }
    *(previous_statep) = previous_state;
}

#define WT_REF_LOCK(session, ref, previous_statep) __ref_lock((session), (ref), (previous_statep))

#define WT_REF_UNLOCK(ref, state) \
	do {											\
		__atomic_store_n(&ref->owner, 0, __ATOMIC_SEQ_CST); \
		WT_REF_SET_STATE(ref, state);						\
	}  while(0)

