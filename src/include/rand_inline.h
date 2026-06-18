/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __wt_random_hotpath_1000 --
 *     A shared, low-overhead "fire ~1 in 1000 times" gate for amortized hot-path sampling. The hot
 *     path is a single decrement and compare; only on the rare firing do we reseed using the
 *     session RNG, whose cost is irrelevant at 1/1000 and which (unlike __wt_rdtsc, returning 0
 *     where it isn't implemented) is portable. Reseeding to a random 0..1999 (mean ~1000) keeps the
 *     sampling from lining up with any periodic workload. Any caller wanting a 1/1000 amortization
 *     gate shares this single per-session counter rather than adding its own.
 */
static WT_INLINE bool
__wt_random_hotpath_1000(WT_SESSION_IMPL *session)
{
    if (WT_UNLIKELY(--session->random_hotpath_counter_1000 < 0)) {
        session->random_hotpath_counter_1000 = (int32_t)(__wt_random(&session->rnd_random) % 2000);
        return (true);
    }
    return (false);
}
