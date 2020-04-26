/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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

#include <unistd.h>

/*
 * JIRA ticket reference: WT-5242 Test case description: This test verifies that WiredTiger does not
 * preserve excessive checkpoints during a hot backup (i.e., while a backup cursor is open).
 */
#define CONN_CONFIG "create,cache_size=100MB,log=(archive=false,enabled=true,file_max=100K)"

#define NOPS 2000
#define CKPT_FREQ 20
#define WORK_ITERS 5
#define KV_SIZE 32

/* Insert a bunch of keys, doing a lot of checkpoints along the way */
static void
do_work(WT_SESSION *session, TEST_OPTS *opts, int base)
{
    WT_CURSOR *cursor;
    int i;
    char k[KV_SIZE], v[KV_SIZE];

    testutil_check(session->open_cursor(session, opts->uri, NULL, NULL, &cursor));

    for (i = 0; i < NOPS; i++) {
        testutil_check(__wt_snprintf(k, sizeof(k), "key.%d.%d", base, i));
        testutil_check(__wt_snprintf(k, sizeof(k), "value.%d.%d", base, i));
        cursor->set_key(cursor, k);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
        if ((i % CKPT_FREQ) == 0)
            testutil_check(session->checkpoint(session, NULL));
    }

    testutil_check(cursor->close(cursor));
}

/* Reach into WT to find out how many checkpoints we have */
static int
count_checkpoints(WT_SESSION *session)
{
    WT_CKPT *ckpt, *ckptbase;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    int count;
    const char *k;

    testutil_check(session->open_cursor(session, WT_METADATA_URI, NULL, NULL, &cursor));

    count = 0;
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &k));

        ret = __wt_metadata_get_ckptlist(session, k, &ckptbase);
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == WT_NOTFOUND)
            continue;

        WT_CKPT_FOREACH (ckptbase, ckpt)
            count++;
    }

    testutil_check(cursor->close(cursor));

    return (count);
}

int
main(int argc, char **argv)
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    int i;
    int initial_count, final_count;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    testutil_check(wiredtiger_open(opts->home, NULL, CONN_CONFIG, &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->create(session, opts->uri, "key_format=S,value_format=S"));

    for (i = 0; i < WORK_ITERS; i++) {
        do_work(session, opts, i);
    }

    /* Open backup cursor */
    testutil_check(session->open_cursor(session, "backup:", NULL, NULL, &cursor));
    initial_count = count_checkpoints(session);

    sleep(2);

    /* Do both updates and inserts */
    for (i = 0; i < WORK_ITERS; i++) {
        do_work(session, opts, i);
        do_work(session, opts, WORK_ITERS + i);
    }
    final_count = count_checkpoints(session);

    /*
     * 3 is just a ballpark figure.  The difference should be much less.
     * Before WT-5242, we would have about 100 times more checkpoints here.
     */
    testutil_assert(final_count < initial_count * 3);

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, NULL));
    testutil_cleanup(opts);

    return (0);
}
