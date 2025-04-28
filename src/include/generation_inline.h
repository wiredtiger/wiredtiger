/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* FIXME: Move all generation functions into one file. */

/*
 * __wt_gen --
 *     Return the resource's generation.
 */
static WT_INLINE uint64_t
__wt_gen(WT_SESSION_IMPL *session, int which)
{
    uint64_t gen;

    WT_ACQUIRE_READ_WITH_BARRIER(gen, S2C(session)->generations[which]);
    return (gen);
}

/*
 * __wt_gen_next --
 *     Switch the resource to its next generation.
 */
static WT_INLINE void
__wt_gen_next(WT_SESSION_IMPL *session, int which, uint64_t *genp)
{
    uint64_t gen;

    gen = __wt_atomic_addv64(&S2C(session)->generations[which], 1);
    if (genp != NULL)
        *genp = gen;
}

/*
 * __wt_session_gen --
 *     Return the thread's resource generation.
 */
static WT_INLINE uint64_t
__wt_session_gen(WT_SESSION_IMPL *session, int which)
{
    /* No need to consider memory ordering for this read as it is always accessed by the same
     * thread. */
    return (__wt_atomic_loadv64(&session->generations[which]));
}
