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

#define N_CALLS (100 * WT_MILLION)

/*
 * do_api --
 *     Do some quick APIs in a tight loop.
 */
static void
do_api(TEST_OPTS *opts, uint64_t *nsec)
{
    struct timespec before, after;
    WT_SESSION *session;
    u_int i;

    session = opts->session;

    __wt_epoch(NULL, &before);
    for (i = 0; i < N_CALLS; ++i) {
        testutil_check(session->reconfigure(session, NULL));
    }
    __wt_epoch(NULL, &after);
    *nsec +=
      (uint64_t)((after.tv_sec - before.tv_sec) * WT_BILLION + (after.tv_nsec - before.tv_nsec));
}

/*
 * main --
 *     The main entry point for a simple test/benchmark for compiling configuration strings.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    uint64_t ncalls, nsecs, per_call;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,statistics=(all),statistics_log=(json,on_close,wait=1)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &opts->session));

    nsecs = 0;

    do_api(opts, &nsecs);

    ncalls = N_CALLS;
    printf("number of API calls: %" PRIu64 "\n", ncalls);
    per_call = nsecs / ncalls;
    printf("total = %" PRIu64
      ", nanoseconds per call = %" PRIu64
      "\n",
      nsecs, per_call);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
