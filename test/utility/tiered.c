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
 * testutil_tiered_begin --
 *     Begin processing for a test program that supports tiered storage.
 */
void
testutil_tiered_begin(TEST_OPTS *opts)
{
    time_t now;

    testutil_assert(!opts->tiered_begun);
    testutil_assert(opts->conn != NULL);

    if (opts->tiered_storage && opts->tiered_flush_interval != 0) {
        (void)time(&now);
        opts->tiered_flush_next = now + opts->tiered_flush_interval;
    }
    opts->tiered_begun = true;
}

/*
 * testutil_tiered_sleep --
 *     Sleep for a number of seconds, or until it is time to flush_tier, or the process wants to
 * exit.
 */
void
testutil_tiered_sleep(TEST_OPTS *opts, uint32_t seconds, bool *do_flush_tier)
{
    time_t now, wake_time;
    bool do_flush;

    (void)time(&now);
    wake_time = (time_t)(now + seconds);
    do_flush = false;
    if (do_flush_tier != NULL && opts->tiered_flush_next != 0 && opts->tiered_flush_next < wake_time) {
        wake_time = opts->tiered_flush_next;
        do_flush = true;
    }
    *do_flush_tier = false;

    while (now < wake_time && opts->running) {
        sleep(1);
        (void)time(&now);
    }
    if (opts->running && do_flush) {
        *do_flush_tier = true;
        opts->tiered_flush_next = 0; /* Don't flush until we know this flush is complete. */
    }
}

/*
 * testutil_tiered_flush_complete --
 *     Notification that a flush_tier has completed, with the given argument.
 */
void testutil_tiered_flush_complete(TEST_OPTS *opts, void *arg)
{
    time_t now;

    (void)arg;

    (void)time(&now);
    opts->tiered_flush_next = (time_t)(now + opts->tiered_flush_interval);
}
