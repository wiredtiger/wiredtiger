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
    return (__wt_tsan_suppress_load_wt_page_ptr_v(&ref->home) == NULL);
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
 *     Set a ref's state, preserving the reader count in the high bits. Uses a CAS loop so that a
 *     concurrent reader incrementing or decrementing the count does not lose their update.
 *
 *     Callers arrive here from two paths:
 *       1. Eviction unlock after a successful drain (count is already 0; loop executes once).
 *       2. Non-eviction WT_REF_LOCK/UNLOCK (e.g. txn, compaction, splits) where readers may still
 *          hold a count on the page data while the caller manipulates ref metadata. The count must
 *          be preserved or those readers will underflow on release.
 */
static WT_INLINE void
__ref_set_state(WT_REF *ref, WT_REF_STATE state)
{
    uint32_t current;
    do {
        current = __wt_atomic_load_uint32_v_relaxed(&ref->state_and_count);
    } while (!__wt_atomic_cas_uint32_v(
      &ref->state_and_count, current, (current & WT_REF_SC_COUNT_MASK) | (uint32_t)state));
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

/*
 * __ref_get_state --
 *     Get a ref's state variable safely. Extracts the low byte of state_and_count.
 */
static WT_INLINE WT_REF_STATE
__ref_get_state(WT_REF *ref)
{
    return ((WT_REF_STATE)(__wt_atomic_load_uint32_v_relaxed(&ref->state_and_count) &
      WT_REF_SC_STATE_MASK));
}

#define WT_REF_GET_STATE(ref) __ref_get_state((ref))

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

    WT_ASSERT(session, old_state != new_state);

    /*
     * CAS the state bits (low byte) while preserving the reader count in the high bits. This is a
     * loop because the count can change between the load and the CAS. The CAS only fails on a
     * state mismatch (a genuine conflict), not on a count change.
     *
     * Crucially, for the eviction path (MEM->LOCKED) this allows the state to be locked even while
     * readers hold a count. Once LOCKED, new readers see LOCKED and decrement immediately; existing
     * readers drain. The caller (__evict_exclusive) then waits for the count to reach zero.
     */
    {
        uint32_t current;
        cas_result = false;
        for (;;) {
            current = __wt_atomic_load_uint32_v_relaxed(&ref->state_and_count);
            if ((current & WT_REF_SC_STATE_MASK) != (uint32_t)old_state)
                break;
            if (__wt_atomic_cas_uint32_v(&ref->state_and_count, current,
                  (current & WT_REF_SC_COUNT_MASK) | (uint32_t)new_state)) {
                cas_result = true;
                break;
            }
        }
    }

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

/*
 * __ref_cas_state_evict --
 *     Eviction-specific CAS from WT_REF_MEM to WT_REF_LOCKED. The CAS requires the reader count to
 *     be zero in addition to the state match, so eviction never acquires the lock while any reader
 *     is pinning the page; it simply loses the race and retries the page later.
 */
static WT_INLINE bool
__ref_cas_state_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE old_state,
  WT_REF_STATE new_state, const char *func, int line)
{
    bool cas_result;

    WT_UNUSED(session);
    WT_UNUSED(func);
    WT_UNUSED(line);

    WT_ASSERT(session, old_state != new_state);

    {
        uint32_t current;
        cas_result = false;
        for (;;) {
            current = __wt_atomic_load_uint32_v_relaxed(&ref->state_and_count);
            if ((current & WT_REF_SC_STATE_MASK) != (uint32_t)old_state)
                break;
            /* Refuse the CAS if any readers are present. */
            if ((current & WT_REF_SC_COUNT_MASK) != 0)
                break;
            if (__wt_atomic_cas_uint32_v(&ref->state_and_count, current,
                  (current & WT_REF_SC_COUNT_MASK) | (uint32_t)new_state)) {
                cas_result = true;
                break;
            }
        }
    }

#ifdef HAVE_REF_TRACK
    if (cas_result)
        __ref_track_state(session, ref, new_state, func, line);
#endif
    return (cas_result);
}

#define WT_REF_CAS_STATE_EVICT(session, ref, old_state, new_state) \
    __ref_cas_state_evict(session, ref, old_state, new_state, __PRETTY_FUNCTION__, __LINE__)

/*
 * __ref_lock --
 *     Spin until successfully locking the ref. Return the previous state to the caller.
 */
static WT_INLINE void
__ref_lock(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE *previous_statep)
{
    WT_REF_STATE previous_state;
    for (;; __wt_yield()) {
        previous_state = WT_REF_GET_STATE(ref);
        if (previous_state != WT_REF_LOCKED &&
          WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
            break;
    }
    *(previous_statep) = previous_state;
}

#define WT_REF_LOCK(session, ref, previous_statep) __ref_lock((session), (ref), (previous_statep))

#define WT_REF_UNLOCK(ref, state) WT_REF_SET_STATE(ref, state)

/*
 * __wt_ref_count_acquire --
 *     Pin a page in memory by incrementing its reader count. Returns busy=true if the page is not
 *     in WT_REF_MEM state at the moment of the increment, in which case the increment is undone.
 *     Uses seq_cst fetch_add so no separate full barrier is required before the state re-check.
 */
static WT_INLINE int
__wt_ref_count_acquire(WT_SESSION_IMPL *session, WT_REF *ref, bool *busyp)
{
    uint32_t old_word;

    *busyp = false;

    if (F_ISSET(S2BT(session), WT_BTREE_NO_EVICT))
        return (0);

    /*
     * Atomically increment the count and capture the previous value. The seq_cst ordering of
     * fetch_add ensures the increment is visible to eviction before we re-examine the state bits,
     * and that we see the latest state written by any concurrent eviction CAS.
     */
    old_word = __wt_atomic_fetch_add_uint32_v(&ref->state_and_count, WT_REF_SC_COUNT_ONE);
    if ((old_word & WT_REF_SC_STATE_MASK) == WT_REF_MEM)
        return (0);

    /* Page not in MEM state at the moment of increment; undo and report busy. */
    __wt_atomic_sub_uint32_v(&ref->state_and_count, WT_REF_SC_COUNT_ONE);
    *busyp = true;
    return (0);
}

/*
 * __wt_ref_count_release --
 *     Release a previously acquired page pin by decrementing the reader count.
 */
static WT_INLINE void
__wt_ref_count_release(WT_SESSION_IMPL *session, WT_REF *ref)
{
    if (!F_ISSET(S2BT(session), WT_BTREE_NO_EVICT))
        __wt_atomic_sub_uint32_v(&ref->state_and_count, WT_REF_SC_COUNT_ONE);
}

/*
 * __wt_ref_count --
 *     Return the current reader count for a ref.
 */
static WT_INLINE uint32_t
__wt_ref_count(WT_REF *ref)
{
    return (__wt_atomic_load_uint32_v_relaxed(&ref->state_and_count) >> WT_REF_SC_COUNT_SHIFT);
}
