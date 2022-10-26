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

/*
 * time_us --
 *     Return the number of microseconds since the epoch.
 */
static uint64_t
time_us(void)
{
    struct timeval tv;

    testutil_assert_errno(gettimeofday(&tv, NULL) == 0);
    return ((uint64_t)tv.tv_sec * 1000000 + (uint64_t)tv.tv_usec);
}

/*
 * testutil_tiered_begin --
 *     Begin processing for a test program that supports tiered storage.
 */
void
testutil_tiered_begin(TEST_OPTS *opts)
{
    testutil_assert(!opts->tiered_begun);
    testutil_assert(opts->conn != NULL);

    if (opts->tiered_storage && opts->tiered_flush_interval_us != 0)
        /* Initialize the time of the next flush_tier. */
        testutil_tiered_flush_complete(opts, NULL);

    opts->tiered_begun = true;
}

/*
 * testutil_tiered_sleep --
 *     Sleep for a number of seconds, or until it is time to flush_tier, or the process wants to
 *     exit.
 */
void
testutil_tiered_sleep(TEST_OPTS *opts, uint32_t seconds, bool *do_flush_tier)
{
    uint64_t now, wake_time;
    bool do_flush;

    now = time_us();
    wake_time = now + 1000000 * seconds;
    do_flush = false;
    if (do_flush_tier != NULL && opts->tiered_flush_next_us != 0 &&
      opts->tiered_flush_next_us < wake_time) {
        wake_time = opts->tiered_flush_next_us;
        do_flush = true;
    }
    *do_flush_tier = false;

    while (now < wake_time && opts->running) {
        /* Sleep a maximum of one second, so we can make sure we should still be running. */
        if (now + 1000000 < wake_time)
            __wt_sleep(1, 0);
        else
            __wt_sleep(0, wake_time - now);
        now = time_us();
    }
    if (opts->running && do_flush) {
        /* Don't flush again until we know this flush is complete. */
        opts->tiered_flush_next_us = 0;
        *do_flush_tier = true;
    }
}

/*
 * testutil_tiered_flush_complete --
 *     Notification that a flush_tier has completed, with the given argument.
 */
void
testutil_tiered_flush_complete(TEST_OPTS *opts, void *arg)
{
    uint64_t now;

    (void)arg;

    now = time_us();
    opts->tiered_flush_next_us = now + opts->tiered_flush_interval_us;
}
