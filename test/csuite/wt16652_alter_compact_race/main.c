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

/*
 * [TEST_TAGS]
 * eviction:schema_api
 * [END_TAGS]
 */

#include "test_util.h"

/*
 * Test case description: This test reproduces a race condition between ALTER and COMPACT operations
 * where split generation is global across all btrees, not per-btree. This causes a Compact
 * operation traversing one btree to block ALTER eviction of internal pages in a completely
 * different btree.
 *
 * The bug occurs when:
 * 1. Compact session traverses btree A with split generation
 * 2. An internal page in btree B has a newer split generation
 * 3. Alter calls __wt_evict_file(..., WT_SYNC_DISCARD) on btree B
 * 4. __wt_page_can_evict finds Compact session (operating on btree A) with older generation
 * 5. Returns true (cannot evict) even though Compact is on a different btree
 * 6. Assertion fails: "Page should be evictable during discard"
 *
 * Failure mode: Without the fix, the assertion in __wt_evict_file will fail.
 */

/*
 * Use enough records to create internal pages and make the file large enough for COMPACT to work.
 * With very small page sizes (4KB), 5k records should create a file > 1MB (COMPACT minimum). More
 * records = more internal pages = higher bug reproduction probability.
 */
#define NUM_RECORDS 5000
#define TABLE1_URI "table:table1"
#define TABLE2_URI "table:table2"

static pthread_t thread_compact, thread_alter;
static volatile bool compact_running = false;
static volatile bool stop_threads = false;

/* Thread data structure */
struct thread_data {
    WT_CONNECTION *conn;
    const char *uri;
};

/* Forward declarations */
static void *thread_func_compact(void *);
static void *thread_func_alter(void *);
static void populate_table(WT_SESSION *, const char *);
static void create_internal_pages(WT_SESSION *, const char *);

/*
 * main --
 *     Test entry point.
 */
int
main(int argc, char *argv[])
{
    struct thread_data compact_data, alter_data;
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    /*
     * Use aggressive configuration to maximize the chance of reproducing:
     * - Very small cache (20MB) to force aggressive eviction
     * - Many eviction threads to increase concurrency
     * - compact_slow timing stress to extend the time COMPACT holds split generation
     * - eviction_dirty_target=1 to trigger eviction very aggressively
     */
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,cache_size=20MB,eviction=(threads_min=8,threads_max=16),"
      "eviction_dirty_target=1,eviction_dirty_trigger=5,"
      "timing_stress_for_test=[compact_slow],"
      "statistics=(all),statistics_log=(json,on_close,wait=1)",
      &opts->conn));

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    /*
     * Create tables with very small page sizes to force many internal pages and splits.
     * Aggressive settings:
     * - internal_page_max=4KB (very small to create many internal pages)
     * - leaf_page_max=4KB (very small to create many leaf pages and force splits)
     * - split_pct=50 (split earlier to create more internal pages)
     * - memory_page_max=1MB (small to force more eviction)
     */
    testutil_check(session->create(session, TABLE1_URI,
      "key_format=Q,value_format=S,internal_page_max=4KB,leaf_page_max=4KB,"
      "split_pct=50,memory_page_max=1MB"));
    populate_table(session, TABLE1_URI);
    /* Create internal pages in table1 */
    create_internal_pages(session, TABLE1_URI);

    /*
     * Create fragmentation in table1 by deleting every other record. This will give COMPACT
     * something to do, ensuring it actually walks the tree and enters split generation.
     */
    {
        WT_CURSOR *cursor;
        uint64_t i;
        testutil_check(session->open_cursor(session, TABLE1_URI, NULL, NULL, &cursor));
        for (i = 1; i <= NUM_RECORDS * 2; i += 2) {
            cursor->set_key(cursor, i);
            (void)cursor->remove(cursor);
        }
        testutil_check(cursor->close(cursor));
    }

    /* Create and populate table2 (for alter) */
    testutil_check(session->create(session, TABLE2_URI,
      "key_format=Q,value_format=S,internal_page_max=4KB,leaf_page_max=4KB,"
      "split_pct=50,memory_page_max=1MB"));
    populate_table(session, TABLE2_URI);

    /* Create internal pages in table2 by inserting many records */
    create_internal_pages(session, TABLE2_URI);

    /* Checkpoint to ensure data is on disk */
    testutil_check(session->checkpoint(session, NULL));

    testutil_check(session->close(session, NULL));

    /* Setup thread data */
    compact_data.conn = opts->conn;
    compact_data.uri = TABLE1_URI;
    alter_data.conn = opts->conn;
    alter_data.uri = TABLE2_URI;

    /*
     * Start COMPACT thread on table1. COMPACT will naturally enter split generation when traversing
     * internal pages via WT_WITH_PAGE_INDEX macro. The compact_slow timing stress will extend the
     * time COMPACT holds split generation, increasing the race window.
     */
    testutil_check(pthread_create(&thread_compact, NULL, thread_func_compact, &compact_data));
    /* Wait for compact thread to start */
    while (!compact_running)
        __wt_sleep(0, 100000); /* 100ms */

    /* Start alter thread on table2 */
    testutil_check(pthread_create(&thread_alter, NULL, thread_func_alter, &alter_data));

    /* Wait for alter thread to complete */
    testutil_check(pthread_join(thread_alter, NULL));

    /* Stop compact thread */
    stop_threads = true;
    testutil_check(pthread_join(thread_compact, NULL));
    /* Cleanup */
    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * populate_table --
 *     Populate a table with records.
 */
static void
populate_table(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    uint64_t i;
    char value_buf[1024];

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    memset(value_buf, 'a', sizeof(value_buf) - 1);
    value_buf[sizeof(value_buf) - 1] = '\0';

    for (i = 1; i <= NUM_RECORDS; i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, value_buf);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
}

/*
 * create_internal_pages --
 *     Create internal pages by inserting more records and checkpointing. With 4KB pages, the
 *     existing 50k records should already create internal pages. We just need one more checkpoint
 *     to ensure they're created.
 */
static void
create_internal_pages(WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    uint64_t i;
    char value_buf[1024];

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    memset(value_buf, 'b', sizeof(value_buf) - 1);
    value_buf[sizeof(value_buf) - 1] = '\0';

    /* Insert a few more records to trigger splits */
    for (i = NUM_RECORDS + 1; i <= NUM_RECORDS + 5000; i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, value_buf);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));

    /* Checkpoint to create internal pages */
    testutil_check(session->checkpoint(session, NULL));
}

/*
 * thread_func_compact --
 *     Thread function that runs COMPACT on table1.
 *
 * COMPACT will naturally enter split generation when traversing internal pages via the
 *     WT_WITH_PAGE_INDEX macro in __compact_walk_internal. The compact_slow timing stress extends
 *     the time COMPACT holds split generation, increasing the race window. Any session holding
 *     split generation on table1 btree should NOT block ALTER eviction on table2.
 */
static void *
thread_func_compact(void *arg)
{
    struct thread_data *data;
    WT_SESSION *session;

    data = (struct thread_data *)arg;

    testutil_check(data->conn->open_session(data->conn, NULL, NULL, &session));

    compact_running = true;

    /*
     * Run COMPACT continuously which will naturally enter split generation via WT_WITH_PAGE_INDEX
     * when traversing internal pages. The compact_slow timing stress (10 seconds) in
     * __compact_walk_internal ensures we hold split generation for the entire duration of ALTER
     * operations on other btrees, guaranteeing reliable reproduction of the race condition.
     * Keep running COMPACT until the main thread signals us to stop (after ALTER completes).
     */
    while (!stop_threads) {
        /* COMPACT may return EBUSY if eviction is under pressure, which is fine for this test. */
        (void)session->compact(session, data->uri, NULL);
    }

    testutil_check(session->close(session, NULL));

    return (NULL);
}

/*
 * thread_func_alter --
 *     Thread function that runs ALTER on table2.
 *
 * Simple strategy: 1. Wait for COMPACT thread to start 2. Insert data to trigger page splits on
 *     table2 3. Checkpoint to make btree->modified = false 4. Call ALTER which will trigger
 *     __wt_evict_file(WT_SYNC_DISCARD) 5. WITHOUT FIX: COMPACT session on table1 will block
 *     eviction on table2 6. WITHOUT FIX: Hit assertion "Page should be evictable during discard"
 */
static void *
thread_func_alter(void *arg)
{
    struct thread_data *data;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    uint64_t i;
    char value_buf[1024];

    data = (struct thread_data *)arg;

    testutil_check(data->conn->open_session(data->conn, NULL, NULL, &session));

    /* Wait for COMPACT thread to start running */
    while (!compact_running)
        __wt_sleep(0, 100000); /* 100ms */

    /* Give COMPACT some time to enter split generation */
    __wt_sleep(0, 500000); /* 500ms */

    memset(value_buf, 'c', sizeof(value_buf) - 1);
    value_buf[sizeof(value_buf) - 1] = '\0';

    /*
     * Insert more data to trigger page splits on table2. This will create internal pages with new
     * split generation values.
     */
    testutil_check(session->open_cursor(session, data->uri, NULL, NULL, &cursor));
    for (i = (uint64_t)(NUM_RECORDS * 2 + 1); i <= (uint64_t)(NUM_RECORDS * 4); i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, value_buf);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    /*
     * Checkpoint to:
     * 1. Force page splits and update split generation on internal pages
     * 2. Make btree->modified = false
     */
    testutil_check(session->checkpoint(session, NULL));

    /*
     * Call ALTER which will:
     * 1. Close the dhandle via __wt_conn_dhandle_close_all
     * 2. Call __wt_checkpoint_close
     * 3. Since btree->modified is false, call __wt_evict_file(WT_SYNC_DISCARD)
     * 4. Try to evict all pages including internal pages
     * 5. Check __wt_page_can_evict for each internal page
     * 6. WITHOUT FIX: Find the entering-split-gen session on table1 blocking eviction
     * 7. WITHOUT FIX: Hit assertion "Page should be evictable during discard"
     */
    testutil_check(session->alter(session, data->uri, "access_pattern_hint=random"));

    testutil_check(session->close(session, NULL));

    return (NULL);
}
