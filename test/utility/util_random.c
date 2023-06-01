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
#include "test_util.h"
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
 */
#undef M_W
#define M_W(r) r.x.w
#undef M_Z
#define M_Z(r) r.x.z

#ifdef ENABLE_ANTITHESIS
#include "instrumentation.h"
#endif

/*
 * testutil_random_init_seed --
 *     Initialize the state of a 32-bit pseudo-random number.
 */
void
testutil_random_init_seed(RAND_STATE volatile *rnd_state)
{
    struct timespec ts;
    RAND_STATE rnd;
    uintmax_t threadid;

    __wt_epoch(NULL, &ts);
    __wt_thread_id(&threadid);

    /*
     * Use this, instead of __wt_random_init, to vary the initial state of the RNG. This is
     * (currently) only used by test programs, where, for example, an initial set of test data is
     * created by a single thread, and we want more variability in the initial state of the RNG.
     *
     * Take the seconds and nanoseconds from the clock together with the thread ID to generate a
     * 64-bit seed, then smear that value using algorithm "xor" from Marsaglia, "Xorshift RNGs".
     */
    M_W(rnd) = (uint32_t)ts.tv_sec ^ 521288629;
    M_Z(rnd) = (uint32_t)ts.tv_nsec ^ 362436069;
    rnd.v ^= (uint64_t)threadid;
    rnd.v ^= rnd.v << 13;
    rnd.v ^= rnd.v >> 7;
    rnd.v ^= rnd.v << 17;

    *rnd_state = rnd;
}

/*
 * testutil_random_init_int --
 *     Initialize return of a 32-bit pseudo-random number.
 */
static inline void
testutil_random_init_int(RAND_STATE volatile *rnd_state)
{
    RAND_STATE rnd;

    M_W(rnd) = 521288629;
    M_Z(rnd) = 362436069;
    *rnd_state = rnd;
}

/*
 * testutil_random --
 *     Return a 32-bit pseudo-random number.
 */
uint32_t
testutil_random(RAND_STATE volatile *rnd_state)
{
#ifdef ENABLE_ANTITHESIS
    return (uint32_t)(fuzz_get_random());
#else
    RAND_STATE rnd;
    uint32_t w, z;

    /*
     * Generally, every thread should have their own RNG state, but it's not guaranteed. Take a copy
     * of the random state so we can ensure that the calculation operates on the state consistently
     * regardless of concurrent calls with the same random state.
     */
    if (rnd_state != NULL)
        WT_ORDERED_READ(rnd, *rnd_state);
    else
        testutil_random_init_int(&rnd);

    w = M_W(rnd);
    z = M_Z(rnd);

    /*
     * Check if the value goes to 0 (from which we won't recover), and reset to the initial state.
     * This has additional benefits if a caller fails to initialize the state, or initializes with a
     * seed that results in a short period.
     */
    if (z == 0 || w == 0) {
        testutil_random_init_int(&rnd);
        w = M_W(rnd);
        z = M_Z(rnd);
    }

    M_Z(rnd) = z = 36969 * (z & 65535) + (z >> 16);
    M_W(rnd) = w = 18000 * (w & 65535) + (w >> 16);

    if (rnd_state != NULL)
        *rnd_state = rnd;

    return ((z << 16) + (w & 65535));
#endif
}

/*
 * testutil_random_from_random --
 *     Seed a destination random number generator from a source random number generator. The source
 *     generator's state is advanced.
 */
void
testutil_random_from_random(RAND_STATE *dest, RAND_STATE *src)
{
    testutil_random_from_seed(dest, testutil_random(src));
}

/*
 * testutil_random_from_seed --
 *     Seed a random number generator from a single seed value.
 */
void
testutil_random_from_seed(RAND_STATE *rnd, uint64_t seed)
{
    uint32_t lower, upper;

    /*
     * Our random number generator has two parts that operate independently. We need to seed both
     * with a non-zero value to get the maximum variation. We may be called with a seed that is less
     * than 2^32, so we need to work with zeroes in one half of our 64 bit seed.
     */
    lower = seed & 0xffffffff;
    upper = seed >> 32;

    rnd->x.w = (lower == 0 ? upper : lower);
    rnd->x.z = (upper == 0 ? lower : upper);
}

/*
 * testutil_random_init --
 *     Initialize the Nth random number generator from the seed. If the seed is not yet set, get a
 *     random seed. The random seed is always returned.
 */
void
testutil_random_init(RAND_STATE *rnd, uint64_t *seedp, uint32_t n)
{
    uint32_t shift;

    if (*seedp == 0) {
        /*
         * We'd like to seed our random generator with a 3 byte value. This gives us plenty of
         * variation for testing, but yet makes the seed more convenient for human use. We generate
         * an initial "random" seed that we can then manipulate.
         *
         * However, the initial "random" seed is not random with respect to time, because it is
         * based on the system clock. Successive calls to this function may yield the same clock
         * time on some systems, and that leaves us with the same random seed. So we factor in a "n"
         * value from the caller to get up to 4 different random seeds.
         */
        testutil_random_init_seed(rnd);
        shift = 8 * (n % 4);
        *seedp = ((rnd->v >> shift) & 0xffffff);
    }
    testutil_random_from_seed(rnd, *seedp);
}
