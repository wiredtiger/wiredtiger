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
void *thread_validate(void *);
void *create_table_and_verify(void *);

bool test_running = true;
#define CATALOG_URI "table:catalog"

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;
    pthread_t create_thread, ckpt_thread, validate_thread;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));

    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    // Open connection
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,cache_size=4G,log=(enabled,file_max=10M,remove=true),debug_mode=(table_logging)",
      &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    testutil_check(session->begin_transaction(session, NULL));
    testutil_check(
      session->create(session, CATALOG_URI, "key_format=Q,value_format=SS,log=(enabled=false)"));
    testutil_check(session->commit_transaction(session, NULL));

    // Spawn threads
    testutil_check(pthread_create(&ckpt_thread, NULL, thread_checkpoint, opts));
    testutil_check(pthread_create(&create_thread, NULL, create_table_and_verify, opts));
    testutil_check(pthread_create(&validate_thread, NULL, thread_validate, opts));
    sleep(1); // hack -> wait for threads to spin up

    // Run for 15 then shutdown
    printf("Running for 5 seconds\n");
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
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_SESSION *session;
    uint64_t i;
    char collection_uri[32];
    char index_uri[32];

    printf("Start create thread\n");

    opts = (TEST_OPTS *)arg;
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));

    i = 0;
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
        i += 1;

        // Create collection
        testutil_check(session->begin_transaction(session, NULL));
        testutil_check(session->create(
          session, collection_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));
        testutil_check(session->commit_transaction(session, NULL));

        // Create index
        testutil_check(session->begin_transaction(session, NULL));
        testutil_check(
          session->create(session, index_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));
        testutil_check(session->commit_transaction(session, NULL));

        /* Add the new tables to the catalog */
        testutil_check(session->begin_transaction(session, NULL));
        catalog_cursor->set_key(catalog_cursor, i - 1);
        catalog_cursor->set_value(catalog_cursor, collection_uri, index_uri);
        catalog_cursor->insert(catalog_cursor);
        catalog_cursor->reset(catalog_cursor);
        testutil_check(session->commit_transaction(session, NULL));

        // Write to both tables in a single txn as per the printlog
        testutil_check(session->begin_transaction(session, NULL));

        testutil_check(
          session->open_cursor(session, collection_uri, NULL, NULL, &collection_cursor));
        collection_cursor->set_key(collection_cursor, i);
        collection_cursor->set_value(collection_cursor, 2 * i);
        testutil_check(collection_cursor->insert(collection_cursor));

        testutil_check(session->open_cursor(session, index_uri, NULL, NULL, &index_cursor));
        index_cursor->set_key(index_cursor, i);
        index_cursor->set_value(index_cursor, 2 * i);
        testutil_check(index_cursor->insert(index_cursor));

        testutil_check(collection_cursor->reset(collection_cursor));
        testutil_check(index_cursor->reset(index_cursor));

        testutil_check(session->commit_transaction(session, NULL));

        // For the purpose of this test just check that both tables are populated.
        // The error we're seeing is one table is empty when Mongo validates.
        // TODO - do we need to look on disk for mongo verify?
        testutil_check(session->begin_transaction(session, NULL));
        collection_cursor->set_key(collection_cursor, i);
        testutil_assert(collection_cursor->search(collection_cursor) == 0);

        index_cursor->set_key(index_cursor, i);
        testutil_assert(index_cursor->search(index_cursor) == 0);
        testutil_check(session->commit_transaction(session, NULL));

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

    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_SESSION *session;
    uint64_t collection_value, index_value;
    char *collection_uri, *index_uri;
    int ret;

    opts = (TEST_OPTS *)arg;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));

    while (test_running) {
        testutil_check(session->begin_transaction(session, NULL));
        /* Iterate through the set of tables in reverse (so we inspect newer tables first to
         * encourage races */
        while ((ret = catalog_cursor->prev(catalog_cursor)) == 0) {
            catalog_cursor->get_value(catalog_cursor, &collection_uri, &index_uri);
            testutil_check(
              session->open_cursor(session, collection_uri, NULL, NULL, &collection_cursor));
            testutil_check(session->open_cursor(session, index_uri, NULL, NULL, &index_cursor));

            while ((ret = collection_cursor->next(collection_cursor)) == 0) {
                testutil_assert(index_cursor->next(index_cursor) == 0);
                collection_cursor->get_value(collection_cursor, &collection_value);
                index_cursor->get_value(index_cursor, &index_value);
                testutil_assert(collection_value == index_value);
            }
            testutil_check(collection_cursor->close(collection_cursor));
            testutil_check(index_cursor->close(index_cursor));
        }
        testutil_assert(ret == WT_NOTFOUND);
        testutil_check(session->commit_transaction(session, NULL));
        testutil_check(catalog_cursor->reset(catalog_cursor));
    }

    testutil_check(catalog_cursor->close(catalog_cursor));
    printf("END validate thread\n");

    return (NULL);
}

/*
 * thread_checkpoint --
 *     Run ckpts in a loop.
 */
WT_THREAD_RET
thread_checkpoint(void *arg)
{

    TEST_OPTS *opts;
    WT_SESSION *session;

    opts = (TEST_OPTS *)arg;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    while (test_running) {
        printf("    Start ckpt\n");
        testutil_check(session->checkpoint(session, NULL));
        sleep(1);
        printf("    End ckpt\n");
    }

    printf("END ckpt thread\n");

    return (NULL);
}
