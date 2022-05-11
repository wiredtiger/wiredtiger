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

// threads
void *thread_checkpoint(void *);
void *create_table_and_verify(void *);

bool test_running = true;

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;
    pthread_t create_thread, ckpt_thread;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));

    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    // Open connection
    // TODO - need to enable indexing?
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,cache_size=4G, log=(enabled,file_max=10M,remove=true)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    // Spawn threads
    testutil_check(pthread_create(&ckpt_thread, NULL, thread_checkpoint, opts));
    testutil_check(pthread_create(&create_thread, NULL, create_table_and_verify, opts));
    sleep(1); // hack -> wait for threads to spin up

    // Run for 5 then shutdown
    printf("Running for 5 seconds\n");
    sleep(5);
    test_running = false;

    printf("Stopping\n");
    sleep(1);
    testutil_check(pthread_join(ckpt_thread, NULL));
    testutil_check(pthread_join(create_thread, NULL));

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * create_table_and_verify --
 *     Create new collections and populate(?).
 */
WT_THREAD_RET
create_table_and_verify(void *arg)
{
    printf("Start create_coll\n");

    TEST_OPTS *opts;
    WT_SESSION *session;

    opts = (TEST_OPTS *)arg;
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    uint64_t i = 0;
    while (test_running) {
        // printf("    create_table_and_verify\n");

        // Create name
        char table_uri[32];
        testutil_check(__wt_snprintf(table_uri, 32, "table:T%" PRIu64, i++));

        testutil_check(
          session->create(session, table_uri, "key_format=S,value_format=u,log=(enabled=false)"));

        // TODO - comment from orig file:
        /* Reopen connection for WT_SESSION::verify. It requires exclusive access to the file. */
        testutil_check(session->verify(session, table_uri, NULL));
    }

    return (NULL);
}

/*
 * thread_checkpoint --
 *     Run ckpts and verify in a loop.
 */
WT_THREAD_RET
thread_checkpoint(void *arg)
{
    printf("Start ckpt\n");

    TEST_OPTS *opts;
    WT_SESSION *session;

    opts = (TEST_OPTS *)arg;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    while (test_running) {
        // printf("    ckpting\n");
        testutil_check(session->checkpoint(session, NULL));
    }

    return (NULL);
}
