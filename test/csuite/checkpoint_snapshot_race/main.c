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

bool test_running = true;
#define CATALOG_URI "table:catalog"

typedef struct {
    TEST_OPTS *opts;
    pthread_mutex_t ckpt_go_cond_mutex;
    pthread_cond_t ckpt_go_cond;
    /* This is a proxy for timestamps as well. */
    volatile uint64_t collection_count;
} BF25011_OPTS;

// threads
void *thread_checkpoint(void *);
void *thread_validate(void *);
void *create_table_and_verify(void *);

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    BF25011_OPTS *bfopts, _bfopts;
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;
    pthread_t create_thread, ckpt_thread, validate_thread;

    bfopts = &_bfopts;
    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    memset(bfopts, 0, sizeof(*bfopts));
    bfopts->opts = opts;

    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    /*
     * Start collection counter at 10, since it's used as a proxy for timestamps as well which
     * makes it better to avoid 0.
     */
    bfopts->collection_count = 10;
    testutil_check(pthread_mutex_init(&bfopts->ckpt_go_cond_mutex, NULL));
    testutil_check(pthread_cond_init(&bfopts->ckpt_go_cond, NULL));

    // Open connection
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,cache_size=4G,log=(enabled,file_max=10M,remove=true),debug_mode=(table_logging)",
      &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    /* Setup globaal transaction IDs. */
    opts->conn->set_timestamp(opts->conn, "oldest_timestamp=1,stable_timestamp=1");

    testutil_check(session->begin_transaction(session, NULL));
    testutil_check(session->create(
        session, CATALOG_URI, "key_format=Q,value_format=SS,log=(enabled=false)"));
    testutil_check(session->commit_transaction(session, NULL));

    // Spawn threads
    testutil_check(pthread_create(&ckpt_thread, NULL, thread_checkpoint, bfopts));
    testutil_check(pthread_create(&create_thread, NULL, create_table_and_verify, bfopts));
    testutil_check(pthread_create(&validate_thread, NULL, thread_validate, bfopts));
    usleep(200); // hack -> wait for threads to spin up

    printf("Running for 2 seconds\n");
    sleep(5);
    test_running = false;

    printf("Stopping\n");
    sleep(2);
    testutil_check(pthread_join(ckpt_thread, NULL));
    testutil_check(pthread_join(create_thread, NULL));
    testutil_check(pthread_join(validate_thread, NULL));

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
    BF25011_OPTS *bfopts;
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_SESSION *ckpt_session, *session;
    uint64_t i;
    char collection_uri[64], index_uri[64], ts_string[64];

    printf("Start create thread\n");

    bfopts = (BF25011_OPTS *)arg;
    opts = bfopts->opts;
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &ckpt_session));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));

    while (test_running) {

        // printf("Creating %lu\n", i);

        // Mongo doesn't use WT's indexing and instead performs their own. Attempt to emulate that
        // here.
        // 1. Create the collection table
        // 2. Create the index table
        // 3. Write a single key into both tables at the same time
        // 4. Check that both tables contain said key.
        //    a. This step requries further checking as to how Mongo performs validation.

        // Create names
        testutil_check(__wt_snprintf(collection_uri, 64, "table:collection_%" PRIu64, i));
        testutil_check(__wt_snprintf(index_uri, 64, "table:index_%" PRIu64, i));

        i = __atomic_fetch_add(&bfopts->collection_count, 1, __ATOMIC_SEQ_CST);
        snprintf(ts_string, 64, "commit_timestamp=%" PRIu64, i);

        // Create collection
        testutil_check(session->create(
          session, collection_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));

        // Create index
        testutil_check(
          session->create(session, index_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));

        /*
         * Wake the checkpoint thread - to encourage the transaction ID associated with the
         * following put being included in the checkpoints snapshot.
         */
        testutil_check(pthread_mutex_lock(&bfopts->ckpt_go_cond_mutex));
        testutil_check(pthread_cond_signal(&bfopts->ckpt_go_cond));
        testutil_check(pthread_mutex_unlock(&bfopts->ckpt_go_cond_mutex));

        /* Add the new tables to the catalog */
        testutil_check(session->begin_transaction(session, NULL));
        catalog_cursor->set_key(catalog_cursor, i - 1);
        catalog_cursor->set_value(catalog_cursor, collection_uri, index_uri);
        catalog_cursor->insert(catalog_cursor);
        catalog_cursor->reset(catalog_cursor);
        testutil_check(session->commit_transaction(session, ts_string));

        // Write to both tables in a single txn as per the printlog
        testutil_check(session->begin_transaction(session, NULL));

        testutil_check(session->open_cursor(session,
                    collection_uri, NULL, NULL, &collection_cursor));
        collection_cursor->set_key(collection_cursor, i);
        collection_cursor->set_value(collection_cursor, 2 * i);
        testutil_check(collection_cursor->insert(collection_cursor));
        testutil_check(collection_cursor->reset(collection_cursor));

        usleep(10);
        testutil_check(session->open_cursor(session, index_uri, NULL, NULL, &index_cursor));
        index_cursor->set_key(index_cursor, i);
        index_cursor->set_value(index_cursor, 2 * i);
        testutil_check(index_cursor->insert(index_cursor));
        testutil_check(index_cursor->reset(index_cursor));

        testutil_check(session->commit_transaction(session, NULL));

        /*
         * For the purpose of this test just check that both tables are populated.
         * The error we're seeing is one table is empty when Mongo validates.
         * TODO - do we need to look on disk for mongo verify?
        testutil_check(session->begin_transaction(session, NULL));
        collection_cursor->set_key(collection_cursor, i);
        testutil_assert(collection_cursor->search(collection_cursor) == 0);

        index_cursor->set_key(index_cursor, i);
        testutil_assert(index_cursor->search(index_cursor) == 0);
        testutil_check(session->commit_transaction(session, NULL));
         */

        testutil_check(collection_cursor->close(collection_cursor));
        testutil_check(index_cursor->close(index_cursor));
    }

    testutil_check(catalog_cursor->close(catalog_cursor));
    printf("END create thread\n");

    return (NULL);
}

/*
 * thread_validate --
 *     Run ckpts in a loop.
 */
WT_THREAD_RET
thread_validate(void *arg)
{

    BF25011_OPTS *bfopts;
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_SESSION *session;
    uint64_t collection_value, index_value, validated_values, validation_passes;
    char *collection_uri, *index_uri;
    int ret;

    bfopts = (BF25011_OPTS *)arg;
    opts = bfopts->opts;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));
    validated_values = validation_passes = 0;
    sleep(1);

    while (test_running) {
        usleep(1000000);
        testutil_check(session->begin_transaction(session, NULL));
        /*
         * Iterate through the set of tables in reverse (so we inspect newer tables first to
         * encourage races).
        */
        while ((ret = catalog_cursor->prev(catalog_cursor)) == 0) {
            catalog_cursor->get_value(catalog_cursor, &collection_uri, &index_uri);
            testutil_check(session->open_cursor(session,
                        collection_uri, NULL, NULL, &collection_cursor));
            testutil_check(session->open_cursor(session, index_uri, NULL, NULL, &index_cursor));

            while ((ret = collection_cursor->next(collection_cursor)) == 0) {
                testutil_assert(index_cursor->next(index_cursor) == 0);
                collection_cursor->get_value(collection_cursor, &collection_value);
                index_cursor->get_value(index_cursor, &index_value);
                testutil_assert(collection_value == index_value);
                ++validated_values;
            }
            testutil_check(collection_cursor->close(collection_cursor));
            testutil_check(index_cursor->close(index_cursor));
        }
        testutil_assert(ret == WT_NOTFOUND);
        testutil_check(session->commit_transaction(session, NULL));
        testutil_check(catalog_cursor->reset(catalog_cursor));
        ++validation_passes;
    }

    testutil_check(catalog_cursor->close(catalog_cursor));
    printf("END validate thread, validation_passes: %" PRIu64 ", validated_values: %" PRIu64 "\n",
            validation_passes, validated_values);

    return (NULL);
}

/*
 * thread_checkpoint --
 *     Run ckpts in a loop.
 */
WT_THREAD_RET
thread_checkpoint(void *arg)
{
    BF25011_OPTS *bfopts;
    TEST_OPTS *opts;
    WT_SESSION *session;
    struct timespec ts;
    uint64_t collection_count;
    char ts_string[64];
    int ret;

    bfopts = (BF25011_OPTS *)arg;
    opts = bfopts->opts;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    while (test_running) {
        /* Update global timestamp state */
        collection_count = __atomic_load_n(&bfopts->collection_count, __ATOMIC_SEQ_CST);
        testutil_check(__wt_snprintf(ts_string, 64,
            "stable_timestamp=%" PRIu64 ",oldest_timestamp=%" PRIu64,
            collection_count - 2, collection_count - 3));
        /* Hack to ensure global timestamps don't go backward at startup */
        if (collection_count > 12)
            opts->conn->set_timestamp(opts->conn, ts_string);
        printf("    Start ckpt, timestamps: %s\n", ts_string);

        /* Checkpoint once per second or when woken */
        testutil_check(pthread_mutex_lock(&bfopts->ckpt_go_cond_mutex));
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        ret = pthread_cond_timedwait(&bfopts->ckpt_go_cond, &bfopts->ckpt_go_cond_mutex, &ts);
        testutil_assert(ret != EINVAL && ret != EPERM);
        testutil_check(pthread_mutex_unlock(&bfopts->ckpt_go_cond_mutex));

        testutil_check(session->checkpoint(session, NULL));
        printf("    End ckpt\n");
    }

    printf("END ckpt thread\n");

    return (NULL);
}
