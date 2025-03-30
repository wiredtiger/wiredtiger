/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "wt_internal.h"

/*
 * An implementation of George Marsaglia's multiply-with-carry pseudo-random number generator.
 * Computationally fast, with reasonable randomness properties, and a claimed period of > 2^60.
 *
 * Be very careful about races here. Multiple threads can call __wt_random concurrently, and it is
 * okay if those concurrent calls get the same return value. What is *not* okay is if
 * reading/writing the shared state races and uses two different values for m_w or m_z. That can
 * result in a stored value of zero, in which case they will be stuck on zero forever. Take a local
 * copy of the values to avoid that, and read/write in atomic, 8B chunks.
 *
 * Please do not modify the behavior of __wt_random when it is used with the default seed. We have
 * verified that it produces good-quality randomness for our uses within the WiredTiger library, so
 * we would like to preserve its current behavior.
 */
#undef M_V
#define M_V(r) r.v
#undef M_W
#define M_W(r) r.x.w
#undef M_Z
#define M_Z(r) r.x.z

#ifdef ENABLE_ANTITHESIS
#include "instrumentation.h"
#endif

#define DEFAULT_SEED_W 521288629
#define DEFAULT_SEED_Z 362436069

/*
 * __wt_random_init_default --
 *     Initialize a 32-bit pseudo-random number with a default seed.
 */
void
__wt_random_init_default(WT_RAND_STATE *rnd_state) WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    WT_RAND_STATE rnd;

    M_W(rnd) = DEFAULT_SEED_W;
    M_Z(rnd) = DEFAULT_SEED_Z;

    *rnd_state = rnd;
}

/*
 * __wt_random_init_seed --
 *     Initialize the state of a 32-bit pseudo-random number with a seed value.
 */
void
__wt_random_init_seed(WT_RAND_STATE *rnd_state, uint64_t v)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    WT_RAND_STATE rnd;

    /*
     * XOR the provided seed with the initial seed. With high probability, this would provide a
     * random-looking seed which has about 50% of the bits turned on. We don't need to check whether
     * W or Z becomes 0, because we would handle it the first time we use this state to generate a
     * random number.
     */
    M_V(rnd) = v;
    M_W(rnd) ^= DEFAULT_SEED_W;
    M_Z(rnd) ^= DEFAULT_SEED_Z;

    *rnd_state = rnd;
}

/*
 * __wt_session_rng_init_once --
 *     Initialize session's RNGs.
 *
 * This function requires session->id to be already set!
 *
 */
void
__wt_session_rng_init_once(WT_SESSION_IMPL *session) WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    if (!WT_SESSION_FIRST_USE(session))
        return;

    /*
     * Session's skiplist RNG is initialized with the special default seed.
     */
    __wt_random_init_default(&session->rnd_skiplist);

    __wt_random_init_seed(&session->rnd_random,
      ((uint64_t)session->id + 1) * __wt_clock(session) / WT_BILLION + (uint64_t)getpid());
}

/*
 * __wt_random --
 *     Return a 32-bit pseudo-random number.
 */
uint32_t
__wt_random(WT_RAND_STATE *rnd_state) WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
#ifdef ENABLE_ANTITHESIS
    return (uint32_t)(fuzz_get_random());
#else
    WT_RAND_STATE rnd;
    uint32_t w, z;

    /*
     * Generally, every thread should have their own RNG state, but it's not guaranteed. Take a copy
     * of the random state so we can ensure that the calculation operates on the state consistently
     * regardless of concurrent calls with the same random state.
     */
    rnd = *rnd_state;
    WT_ACQUIRE_BARRIER();
    w = M_W(rnd);
    z = M_Z(rnd);

    /*
     * Check if either of the two values goes to 0 (from which we won't recover), and reset it to
     * the default initial state. This would never happen with the default seed, but we need this
     * for the other cases.
     *
     * We do this one component at a time, so that if the random number generator was initialized
     * from an explicitly provided seed, it would not reset the entire state and then effectively
     * result in random number generators from different seeds converging. They would eventually
     * converge if both W and Z become 0 at the same time, but this is very unlikely.
     *
     * This has additional benefits if a caller fails to initialize the state, or initializes with a
     * seed that results in a short period.
     */
    if (w == 0)
        w = DEFAULT_SEED_W;
    if (z == 0)
        z = DEFAULT_SEED_Z;

    M_W(rnd) = w = 18000 * (w & 65535) + (w >> 16);
    M_Z(rnd) = z = 36969 * (z & 65535) + (z >> 16);
    *rnd_state = rnd;

    return ((z << 16) + (w & 65535));
#endif
}

/*
 * __wt_random_init --
 *     Initialize the state of a 32-bit pseudo-random number by a session's rng.
 */
void
__wt_random_init(WT_SESSION_IMPL *session, WT_RAND_STATE *rnd_state)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    if (session != NULL)
        __wt_random_init_seed(rnd_state, __wt_random(&session->rnd_random));
    else {
        uint64_t t = __wt_clock(session);
        __wt_random_init_seed(rnd_state, t / WT_BILLION + (t % WT_BILLION) + (uint64_t)getpid());
    }
}
