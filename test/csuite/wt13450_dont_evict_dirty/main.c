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
 * JIRA ticket reference: WT-13450 Don't add dirty pages in a tree to the urgent queue if checkpoint
 * is running on the same tree.
 *
 * Reproducer steps:
 *
 * - Create a clean tree with deleted content across the tree but content that can't be removed due
 * to the oldest timestamp.
 * - To clean the tree reopen the connection.
 * - Open the btree by reading a single record so it is included in checkpoint (this may be wrong,
 * we might need to dirty one page)
 * - Begin walking a cursor next, add a control point which waits once it has seen that many deleted
 * items, for them to appear deleted read after deletion timestamp
 * - The control point will save the btree ID
 * - Begin the checkpoint, trigger the cursor walking next control point so that it gets into the
 * if.
 * - Somehow verify that we did trigger that control point
 */

/* Constants */
#define NUM_WARM_UP_RECORDS 40000

void *thread_do_next(void *);

static const char *const session_open_config = "prefetch=(enabled=true)";

/*
 * set_key --
 *     Wrapper providing the correct typing for the WT_CURSOR::set_key variadic argument.
 */
static void
set_key(WT_CURSOR *cursor, uint64_t value)
{
    cursor->set_key(cursor, value);
}

/*
 * set_value --
 *     Wrapper providing the correct typing for the WT_CURSOR::set_value variadic argument.
 */
static void
set_value(TEST_OPTS *opts, WT_CURSOR *cursor, uint64_t value)
{
    WT_UNUSED(opts);
    cursor->set_value(cursor, value);
}

/*
 * main --
 *     TODO: Add a comment describing this function.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *wt_session, *checkpoint_session;
    pthread_t next_thread_id;
    uint64_t record_idx;
    int ret;
    const char *wiredtiger_open_config =
      "create,cache_size=2G,eviction=(threads_min=1,threads_max=1),"
      "prefetch=(available=true,default=true),"
#if 1 /* Include if needed */
      "verbose=["
      "control_point=5,"
      //   "prefetch=1,"
      "],"
#endif
      "statistics=(all),statistics_log=(json,on_close,wait=1)";

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    opts->nthreads = 1;                   /* Value ignored */
    opts->nrecords = NUM_WARM_UP_RECORDS; /* Use: How many records to insert during warmup. */
    opts->table_type = TABLE_ROW;         /* Value ignored */
    testutil_check(testutil_parse_opts(argc, argv, opts));
    opts->nthreads = 1;           /* Force desired value */
    opts->table_type = TABLE_ROW; /* Force desired value */
    testutil_recreate_dir(opts->home);
    conn = opts->conn;

    testutil_check(wiredtiger_open(opts->home, NULL, wiredtiger_open_config, &conn));

    printf("Running the warm-up loop in the eviction thread.\n");

    /* Create the session for eviction. */
    testutil_check(conn->open_session(conn, NULL, session_open_config, &wt_session));
    testutil_check(
      wt_session->create(wt_session, opts->uri, "key_format=Q,value_format=Q,leaf_page_max=32k"));

    /*
     * Pin the oldest timestamp at 1. This ensures that when we restart the database the tombstones
     * added to the keys by removing them cannot be cleaned up yet as they are not globally visible.
     *
     * This satisfies the condition that there are clean tombstones in the tree when we begin
     * walking with cursor next.
     */
    testutil_check(conn->set_timestamp(conn, "oldest_timestamp=1"));

    /* Warm-up: Insert some documents at timestamp 2. */
    testutil_check((ret = wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor)));
    for (record_idx = 0; record_idx < opts->nrecords; ++record_idx) {
        /* Do one insertion */
        set_key(cursor, record_idx);
        set_value(opts, cursor, record_idx);
        testutil_check(wt_session->begin_transaction(wt_session, "isolation=snapshot"));
        testutil_check(cursor->insert(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, "commit_timestamp=2"));
        /* Print progress. */
        if ((record_idx % WT_THOUSAND) == 0) {
            printf("main thread: Warm-up: insert key=%" PRIu64 ", value=%" PRIu64 "\n", record_idx,
              record_idx);
            fflush(stdout);
        }
    }

    /* Warm-up: Delete most of the records at time 3. */
    for (record_idx = 0; record_idx < opts->nrecords; ++record_idx) {
        /* Do one insertion */
        testutil_check(wt_session->begin_transaction(wt_session, "isolation=snapshot"));
        testutil_check(cursor->next(cursor));
        /*
         * WiredTiger likes to be smart, if we delete every single record in the tree then we never
         * actually walk real records on the page as a resulf of the page skip logic in
         * __wt_tree_walk_custom_skip. Which you can see in bt_curnext.c:959.
         *
         * To get around this cleverness, leave some undeleted records in the tree that must be
         * visited when we walk the cursor. This effectively guarantees that we call
         * __cursor_row_next, which we must in order to increment the deleted record count that we
         * are interested in.
         */
        if (record_idx % 50 != 0)
            testutil_check(cursor->remove(cursor));
        testutil_check(wt_session->commit_transaction(wt_session, "commit_timestamp=3"));
        /* Print progress. */
        if ((record_idx % WT_THOUSAND) == 0) {
            printf("main thread: Warm-up: remove key=%" PRIu64 ", value=%" PRIu64 "\n", record_idx,
              record_idx);
            fflush(stdout);
        }
    }

    /*
     * Move the stable ahead of our modifications so they don't get unceremoniously rolled back by
     * RTS or something... I'm not sure if that actually happens but we should do this anyway.
     */
    testutil_check(conn->set_timestamp(conn, "stable_timestamp=4"));
    testutil_check(cursor->close(cursor));

    /* Close and reopen the connection to force the warm-up documents out of the cache. */
    testutil_check(wt_session->close(wt_session, NULL));
    testutil_check(conn->close(conn, ""));

    testutil_check(wiredtiger_open(opts->home, NULL, wiredtiger_open_config, &conn));
    testutil_check(conn->open_session(conn, NULL, session_open_config, &wt_session));

    /*
     * This line of code makes our tombstones that we left in the tree globally visible. Which is
     * 100% required as if they are not then the incremented value that gets compared against won't
     * be incremented. The relevant global visibility check can be found in bt_curnext:521.
     */
    testutil_check(conn->set_timestamp(conn, "stable_timestamp=4,oldest_timestamp=4"));

    conn->enable_control_point(conn, WT_CONN_CONTROL_POINT_ID_WT_13450_CKPT, NULL);
    conn->enable_control_point(conn, WT_CONN_CONTROL_POINT_ID_WT_13450_TEST, NULL);
    opts->conn = conn;

    /*
     * Create the thread for cursor->next and wait until we see control point TEST trigger. This
     * ensures that we can begin the checkpoint after the cursor walking next is in the correct
     * place in the code.
     */
    testutil_check(pthread_create(&next_thread_id, NULL, thread_do_next, opts));
    {
        bool enabled = false;
        WT_UNUSED(enabled);

        /* Wait for our next thread. */
        CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(
          (WT_SESSION_IMPL *)wt_session, WT_CONN_CONTROL_POINT_ID_WT_13450_TEST, enabled);
    }

    /* Open a session to run checkpoint. */
    testutil_check(conn->open_session(conn, NULL, session_open_config, &checkpoint_session));

    /* Open a cursor with which to dirty the tree otherwise checkpoint won't "really" happen. */
    testutil_check(
      (ret = checkpoint_session->open_cursor(checkpoint_session, opts->uri, NULL, NULL, &cursor)));

    testutil_check(wt_session->begin_transaction(wt_session, "isolation=snapshot"));
    /* Dirty the tree. Insert a record at the end. */
    set_key(cursor, opts->nrecords);
    set_value(opts, cursor, opts->nrecords);
    testutil_check(cursor->insert(cursor));
    testutil_check(wt_session->commit_transaction(wt_session, "commit_timestamp=5"));

    printf("Begin checkpoint\n");
    /*
     * Checkpoint the database this will call into __wt_sync_file which will first mark the btree as
     * syncing, then it will signal the cursor->next thread to continue. There's also a cleverness
     * about making sure the btree ID matches, we could probably have skipped this? But I think it
     * demostrates a nice feature of control points so we choose to have it.
     *
     * The reasoning behind why we could have skipped that is that there is only one b-tree that is
     * relevant and the code paths currently guarantee that it would be the first one up for
     * checkpoint.
     */
    checkpoint_session->checkpoint(checkpoint_session, NULL);

    /* By this stage the program has crashed. */
    testutil_check(pthread_join(next_thread_id, NULL));

    conn->disable_control_point(conn, WT_CONN_CONTROL_POINT_ID_WT_13450_CKPT);
    conn->disable_control_point(conn, WT_CONN_CONTROL_POINT_ID_WT_13450_TEST);

    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}

/*
 * thread_do_next --
 *     Read to trigger pre-fetch.
 */
void *
thread_do_next(void *arg)
{
    TEST_OPTS *opts;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *wt_session;
    int ret;

    opts = (TEST_OPTS *)arg;
    conn = opts->conn;

    printf("Running next thread\n");

    testutil_check(conn->open_session(conn, NULL, session_open_config, &wt_session));
    testutil_check(wt_session->open_cursor(wt_session, opts->uri, NULL, NULL, &cursor));
    wt_session->breakpoint(wt_session);
    /* Wait for the main test thread to get to the control point. */
    __wt_sleep(1, 0);
    printf("walking cursor next\n");
    while ((ret = cursor->next(cursor)) != WT_NOTFOUND) {
        __wt_sleep(0, 1); /* 1 microsecond */
    }

    testutil_check(cursor->close(cursor));
    testutil_check(wt_session->close(wt_session, NULL));
    opts->running = false;
    printf("Next thread exiting\n");
    return (NULL);
}
