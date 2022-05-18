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
#define LOAD_TABLE_URI "table:load_table"

typedef struct {
    const char *name;
    uint64_t sleep_min_us;
    uint64_t sleep_max_us;
} SLEEP_CONFIG;

typedef struct {
    TEST_OPTS *opts;
    pthread_mutex_t ckpt_go_cond_mutex;
    pthread_cond_t ckpt_go_cond;
    /* This is a proxy for timestamps as well. */
    volatile uint64_t collection_count;
    SLEEP_CONFIG mid_insertion;
    SLEEP_CONFIG checkpoint_start;
} CHECKPOINT_RACE_OPTS;

/* Thread start points */
void *thread_checkpoint(void *);
void *thread_validate(void *);
void *thread_create_table_race(void *);
void *thread_add_load(void *);

/* Helpers to seed waits in thread operations */
void parse_sleep_config(const char *name, char *config_str, SLEEP_CONFIG *cfg);
void sleep_for_us(TEST_OPTS *opts, WT_RAND_STATE *rnd, SLEEP_CONFIG cfg);

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    CHECKPOINT_RACE_OPTS *cr_opts, _cr_opts;
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;
    pthread_t create_thread, ckpt_thread, validate_thread;

    cr_opts = &_cr_opts;
    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    memset(cr_opts, 0, sizeof(*cr_opts));
    cr_opts->opts = opts;

    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    parse_sleep_config("mid_insertion", opts->insertion_sleep_str, &cr_opts->mid_insertion);
    parse_sleep_config("checkpoint_start", opts->checkpoint_delay_str, &cr_opts->checkpoint_start);

    /* Default to 15 seconds */
    if (opts->runtime == 0)
        opts->runtime = 15;

    /*
     * Start collection counter at 10, since it's used as a proxy for timestamps as well which
     * makes it better to avoid 0.
     */
    cr_opts->collection_count = 10;
    testutil_check(pthread_mutex_init(&cr_opts->ckpt_go_cond_mutex, NULL));
    testutil_check(pthread_cond_init(&cr_opts->ckpt_go_cond, NULL));

    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,cache_size=100MB,log=(enabled,file_max=10M,remove=true),debug_mode=(table_logging)",
      &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    /* Setup globaal transaction IDs. */
    opts->conn->set_timestamp(opts->conn, "oldest_timestamp=1,stable_timestamp=1");

    /* Create the catalogue table. */
    testutil_check(session->create(
        session, CATALOG_URI, "key_format=Q,value_format=SS,log=(enabled=false)"));
    /* Create a table that is used to add load to the cache/database. */
    testutil_check(session->create(
        session, LOAD_TABLE_URI, "key_format=Q,value_format=SS,log=(enabled=false)"));

    /* Spawn threads */
    testutil_check(pthread_create(&ckpt_thread, NULL, thread_checkpoint, cr_opts));
    testutil_check(pthread_create(&create_thread, NULL, thread_create_table_race, cr_opts));
    testutil_check(pthread_create(&create_thread, NULL, thread_add_load, cr_opts));
    testutil_check(pthread_create(&validate_thread, NULL, thread_validate, cr_opts));
    usleep(200); /* Wait for threads to spin up */

    snprintf(opts->progress_msg, opts->progress_msg_len,
            "Running for %"PRIu64 " seconds\n", opts->runtime);
    testutil_progress(opts, opts->progress_msg);
    sleep(opts->runtime);
    test_running = false;

    testutil_progress(opts, "Stopping\n");
    sleep(1);
    testutil_check(pthread_join(ckpt_thread, NULL));
    testutil_check(pthread_join(create_thread, NULL));
    testutil_check(pthread_join(validate_thread, NULL));

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * thread_create_table_race --
 *     Create new collections and populate(?).
 */
WT_THREAD_RET
thread_create_table_race(void *arg)
{
    CHECKPOINT_RACE_OPTS *cr_opts;
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    uint64_t i, rnd_val;
    char collection_uri[64], index_uri[64], ts_string[64];
    char collection_config_str[64], index_config_str[64];
    int ret;

    cr_opts = (CHECKPOINT_RACE_OPTS *)arg;
    opts = cr_opts->opts;
    i = 0;

    testutil_progress(opts, "Start create thread\n");
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));

    while (test_running) {

        /*
         * Mongo doesn't use WT's indexing and instead performs their own. Attempt to emulate that
         * here.
         * 1. Create the collection table
         * 2. Create the index table
         * 3. Write a single key into both tables at the same time
         * 4. Check that both tables contain said key.
         *    a. This step requries further checking as to how Mongo performs validation.
         */

        testutil_check(__wt_snprintf(collection_uri, 64, "table:collection_%" PRIu64, i));
        testutil_check(__wt_snprintf(index_uri, 64, "table:index_%" PRIu64, i));

        i = __atomic_fetch_add(&cr_opts->collection_count, 1, __ATOMIC_SEQ_CST);
        snprintf(ts_string, 64, "commit_timestamp=%" PRIu64, i);

        /* Create collection */
        testutil_check(session->create(
          session, collection_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));

        /* Create index */
        testutil_check(
          session->create(session, index_uri, "key_format=Q,value_format=Q,log=(enabled=true)"));

        /*
         * Wake the checkpoint thread - to encourage the transaction ID associated with the
         * following put being included in the checkpoints snapshot.
         */
        if (i % 5 == 0) {
            testutil_check(pthread_mutex_lock(&cr_opts->ckpt_go_cond_mutex));
            testutil_check(pthread_cond_signal(&cr_opts->ckpt_go_cond));
            testutil_check(pthread_mutex_unlock(&cr_opts->ckpt_go_cond_mutex));
        }

        /* Add the new tables to the catalog */
        testutil_check(session->begin_transaction(session, NULL));
        catalog_cursor->set_key(catalog_cursor, i - 1);
        catalog_cursor->set_value(catalog_cursor, collection_uri, index_uri);
        catalog_cursor->insert(catalog_cursor);
        catalog_cursor->reset(catalog_cursor);
        testutil_check(session->commit_transaction(session, ts_string));

        /* Write to both tables in a single txn as per the printlog */
        testutil_check(session->begin_transaction(session, NULL));

        /* Occasionally force the newly updated page to be evicted */
        rnd_val = (uint64_t)__wt_random(&rnd);
        snprintf(collection_config_str, sizeof(collection_config_str),
            "%s", rnd_val % 12 == 0 ? "debug=(release_evict)" : "");
        rnd_val = (uint64_t)__wt_random(&rnd);
        snprintf(index_config_str, sizeof(index_config_str),
            "%s", rnd_val % 12 == 0 ? "debug=(release_evict)" : "");

        testutil_check(session->open_cursor(session,
                    collection_uri, NULL, collection_config_str, &collection_cursor));

        snprintf(opts->progress_msg, opts->progress_msg_len,
               "Creating collection/index: %s\n", collection_uri);
        testutil_progress(opts, opts->progress_msg);

        collection_cursor->set_key(collection_cursor, i);
        collection_cursor->set_value(collection_cursor, 2 * i);
        testutil_check(collection_cursor->insert(collection_cursor));
        testutil_check(collection_cursor->reset(collection_cursor));

        /* Add some random sleeps in the middle of insertion to increase the chance of a checkpoint beginning during insertion */
        sleep_for_us(opts, &rnd, cr_opts->mid_insertion);

        while ((ret = session->open_cursor(session,
                        index_uri, NULL, index_config_str, &index_cursor)) != 0) {
            snprintf(opts->progress_msg, opts->progress_msg_len,
                    "Error returned opening index cursor: %s\n", wiredtiger_strerror(ret));
            testutil_progress(opts, opts->progress_msg);
            usleep(10);
        }
        index_cursor->set_key(index_cursor, i);
        index_cursor->set_value(index_cursor, 2 * i);
        testutil_check(index_cursor->insert(index_cursor));
        testutil_check(index_cursor->reset(index_cursor));

        testutil_check(session->commit_transaction(session, NULL));

        /*
         * For the purpose of this test just check that both tables are populated.
         * The error we're seeing is one table is empty when Mongo validates.
         * The following read is necessary to get the pages force evicted, since
         * insert doesn't leave the cursor positioned it won't trigger the eviction.
         */
        rnd_val = (uint64_t)__wt_random(&rnd) % 4;
        if (rnd_val == 0) {
            testutil_check(session->begin_transaction(session, NULL));
            collection_cursor->set_key(collection_cursor, i);
            testutil_assert(collection_cursor->search(collection_cursor) == 0);

            index_cursor->set_key(index_cursor, i);
            testutil_assert(index_cursor->search(index_cursor) == 0);
            testutil_check(session->commit_transaction(session, NULL));
        }

        testutil_check(collection_cursor->close(collection_cursor));
        testutil_check(index_cursor->close(index_cursor));
    }

    testutil_check(catalog_cursor->close(catalog_cursor));
    testutil_progress(opts, "END create thread\n");

    return (NULL);
}

const char *data_string =
    "A man of literary taste and culture, familiar with the classics, a facile writer of Latin "
    "verses' as well as of Ciceronian prose, he was as anxious that the Roman clergy should unite "
    "human science and literature with their theological studies as that the laity should be "
    "educated in the principles of religion; and to this end he established in Rome a kind of "
    "voluntary school board, with members both lay and clerical; and the rivalry of the schools "
    "thus founded ultimately obliged the state to include religious teaching in its curriculum."
    "If we wish to know what Wagner means, we must fight our way through his drama to his music; "
    "and we must not expect to find that each phrase in the mouth of the actor corresponds word "
    "for note with the music. That sort of correspondence Wagner leaves to his imitators; and his "
    "views on Leit-motifhunting, as expressed in his prose writings and conversation, are "
    "contemptuously tolerant.";

/*
 * thread_add_load --
 *     Create a collection and add content to it to generate other database load
 */
WT_THREAD_RET
thread_add_load(void *arg)
{
    CHECKPOINT_RACE_OPTS *cr_opts;
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *load_cursor;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    size_t data_offset, raw_data_str_len;
    uint64_t i, table_timestamp, us_sleep;
    bool transaction_running;
    char collection_uri[64], index_uri[64], ts_string[64];
    char load_data[256];
    int ret;

    cr_opts = (CHECKPOINT_RACE_OPTS *)arg;
    opts = cr_opts->opts;

    raw_data_str_len = strlen(data_string);
    us_sleep = 100;

    if (test_running)
        return (NULL);

    testutil_progress(opts, "Start load generation thread\n");
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    testutil_check(session->open_cursor(session, LOAD_TABLE_URI, NULL, NULL, &load_cursor));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));

    for (i = 0, transaction_running = false; test_running; i++) {
        if (!transaction_running) {
            testutil_check(session->begin_transaction(session, NULL));
            transaction_running = true;
            if ((ret = catalog_cursor->prev(catalog_cursor)) == 0) {
                catalog_cursor->get_key(catalog_cursor, &table_timestamp);
                catalog_cursor->get_value(catalog_cursor, &collection_uri, &index_uri);
            } else {
                /* The catalog is empty at first, so use some dummy values */
                testutil_assert(ret == WT_NOTFOUND);
                table_timestamp = 10;
                snprintf(collection_uri, sizeof(collection_uri), "%s", "startup");
            }
        }
        load_cursor->set_key(load_cursor, i);
        data_offset = (uint64_t)__wt_random(&rnd) % (raw_data_str_len - sizeof(load_data));
        snprintf(load_data, sizeof(load_data), "%s", data_string + data_offset);
        load_cursor->set_value(load_cursor, collection_uri, load_data);
        testutil_check(load_cursor->insert(load_cursor));

        if (i % 20 == 0) {
            /*
             * The logged table count is being used as a mechanism for assigning timestamps in
             * this application as well. It's assumed that once a table is included in a
             * checkpoint the timestamp associated with that is behind stable. It's unlikely
             * that ten tables can be created in the span of a single transaction here, so
             * set the timestamp for this commit that far ahead. Don't add too much buffer, since
             * it's important that the content being written to the database as part of this
             * operation is included in checkpoints.
             */
            snprintf(ts_string, 64, "commit_timestamp=%" PRIu64, table_timestamp + 10);
            testutil_check(session->commit_transaction(session, ts_string));
            transaction_running = false;
            testutil_check(catalog_cursor->reset(catalog_cursor));
            testutil_check(load_cursor->reset(load_cursor));
            /*
             * Slow down inserts as the workload runs longer - we want to generate load, but
             * not so much that it interferes with the rest of the application.
             */
            if (us_sleep < 50000 && i % 10000 == 0)
                us_sleep += us_sleep;
            usleep(us_sleep);
        }

    }
    if (transaction_running)
        testutil_check(session->commit_transaction(session, NULL));

    testutil_check(catalog_cursor->close(catalog_cursor));
    testutil_check(load_cursor->close(load_cursor));
    return (NULL);
}


/*
 * thread_validate --
 *     Periodically validate the content of the database.
 */
WT_THREAD_RET
thread_validate(void *arg)
{

    CHECKPOINT_RACE_OPTS *cr_opts;
    TEST_OPTS *opts;
    WT_CURSOR *catalog_cursor, *collection_cursor, *index_cursor;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    uint64_t collection_value, index_value, validated_values, validation_passes;
    uint64_t i, rnd_val;
    char *collection_uri, *index_uri, *verify_uri;
    int ret;

    cr_opts = (CHECKPOINT_RACE_OPTS *)arg;
    opts = cr_opts->opts;

    validated_values = validation_passes = 0;
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, CATALOG_URI, NULL, NULL, &catalog_cursor));
    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);

    sleep(3);

    while (test_running) {
        usleep(100000);
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
        /* Occasionally run WiredTiger verify as well */
        if (validation_passes % 3 == 0) {
            i = 0;
            rnd_val = (uint64_t)__wt_random(&rnd) % 10;
            rnd_val++; /* Avoid divide by zero in modulo calculation */
            while ((ret = catalog_cursor->prev(catalog_cursor)) == 0) {
                if (i == 0)
                    i = rnd_val;
                else
                    i--;
                /* Only verify some tables. */
                if (i % rnd_val != 0)
                    continue;
                verify_uri = rnd_val % 2 == 0 ? collection_uri : index_uri;
                catalog_cursor->get_value(catalog_cursor, &collection_uri, &index_uri);
                ret = session->verify(session, verify_uri, NULL);
                if (ret == EBUSY)
                    snprintf(opts->progress_msg, opts->progress_msg_len,
                            "Verifying got busy on %s\n", verify_uri);
                else {
                    testutil_assert(ret == 0);
                    snprintf(opts->progress_msg, opts->progress_msg_len,
                            "Verifying complete on %s\n", verify_uri);
                }
                testutil_progress(opts, opts->progress_msg);
            }
        }
    }

    testutil_check(catalog_cursor->close(catalog_cursor));
    snprintf(opts->progress_msg, opts->progress_msg_len,
            "END validate thread, validation_passes: %" PRIu64 ", validated_values: %" PRIu64 "\n",
            validation_passes, validated_values);
    testutil_progress(opts, opts->progress_msg);

    return (NULL);
}

/*
 * thread_checkpoint --
 *     Run ckpts in a loop.
 */
WT_THREAD_RET
thread_checkpoint(void *arg)
{
    CHECKPOINT_RACE_OPTS *cr_opts;
    TEST_OPTS *opts;
    WT_SESSION *session;
    struct timespec ts;
    uint64_t collection_count;
    char ts_string[64];
    int ret;
    WT_RAND_STATE rnd;

    cr_opts = (CHECKPOINT_RACE_OPTS *)arg;
    opts = cr_opts->opts;

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    while (test_running) {
        /* Update global timestamp state */
        collection_count = __atomic_load_n(&cr_opts->collection_count, __ATOMIC_SEQ_CST);
        testutil_check(__wt_snprintf(ts_string, 64,
            "stable_timestamp=%" PRIu64 ",oldest_timestamp=%" PRIu64,
            collection_count - 2, collection_count - 3));
        /* Hack to ensure global timestamps don't go backward at startup */
        if (collection_count > 12)
            opts->conn->set_timestamp(opts->conn, ts_string);
        snprintf(opts->progress_msg, opts->progress_msg_len, "Checkpoint: %s\n", ts_string);
        testutil_progress(opts, opts->progress_msg);

        /* Checkpoint once per second or when woken */
        testutil_check(pthread_mutex_lock(&cr_opts->ckpt_go_cond_mutex));
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        ret = pthread_cond_timedwait(&cr_opts->ckpt_go_cond, &cr_opts->ckpt_go_cond_mutex, &ts);
       
        sleep_for_us(opts, &rnd, cr_opts->checkpoint_start);

        testutil_assert(ret != EINVAL && ret != EPERM);
        testutil_check(pthread_mutex_unlock(&cr_opts->ckpt_go_cond_mutex));

        testutil_check(session->checkpoint(session, "use_timestamp=true"));
    }

    testutil_progress(opts, "END ckpt thread\n");

    return (NULL);
}


/* Parse a config for how long a thread should sleep. */
void parse_sleep_config(const char *name, char *config_str, SLEEP_CONFIG *cfg) {
    if(config_str != NULL) {
        if(sscanf(config_str, "%lu-%lu", &cfg->sleep_min_us, &cfg->sleep_max_us) != 2) {
            printf("Config must be of the format {min_sleep}-{max_sleep}. For example '-I 100-200'\n");
            exit(1);
        } else {
            testutil_assert(cfg->sleep_min_us < cfg->sleep_max_us);
            cfg->name = name;
        }
    }
}

/* Provided a min/max range, sleep for a random number of microseconds */
void sleep_for_us(TEST_OPTS *opts, WT_RAND_STATE *rnd, SLEEP_CONFIG cfg) {
    /* Add a small delay to when the checkpoint begins to test timing. */
    if(cfg.sleep_max_us > 0) {
        uint64_t sleep_us;
        uint64_t rnd_val;

        rnd_val = (uint64_t)__wt_random(rnd) % (cfg.sleep_max_us - cfg.sleep_min_us);
        sleep_us = cfg.sleep_min_us + rnd_val;
        // printf("%s waiting for for: %" PRIu64 " us\n", cfg.name, sleep_us);

        snprintf(opts->progress_msg, opts->progress_msg_len, "%s waiting for: %" PRIu64 " us\n", cfg.name, sleep_us);
        testutil_progress(opts, opts->progress_msg);
        usleep(sleep_us);
    }
}
