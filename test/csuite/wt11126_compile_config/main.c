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
 * JIRA ticket reference: WT-11126 tests for precompiling configuration.
 */

#define IGNORE_PREPARE_VALUE_SIZE 3
static const char *ignore_prepare_value[3] = {"false", "force", "true"};
static const char *boolean_value[2] = {"false", "true"};

static const char *begin_transaction_config_precompile_format =
  "ignore_prepare=%s,roundup_timestamps=(prepared=%d,read=%d),no_timestamp=%d";

static const char *begin_transaction_config_printf_format =
  "ignore_prepare=%s,roundup_timestamps=(prepared=%s,read=%s),no_timestamp=%s";

/*
 * A typical implementation will incur the cost of formatting the configuration
 * string on every call.
 */
static void
begin_transaction_slow(WT_SESSION *session, int ignore_prepare, bool roundup_prepared,
  bool roundup_read, bool no_timestamp)
{
    char config[256];

    testutil_check(__wt_snprintf(config, sizeof(config), begin_transaction_config_printf_format,
      ignore_prepare_value[ignore_prepare], boolean_value[(int)roundup_prepared],
      boolean_value[(int)roundup_read], boolean_value[(int)no_timestamp]));
    testutil_check(session->begin_transaction(session, config));
}

/*
 * A faster implementation will take advantage of the finite number of configurations possible. It
 * requires an initialization step.
 */
static char medium_config[3 * 2 * 2 * 2][256];
#define MEDIUM_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_ts) \
    (((((ignore_prepare * 2) + roundup_prepared) * 2) + roundup_read) * 2 + no_ts)

static void
begin_transaction_medium_init(void)
{
    for (int ignore_prepare = 0; ignore_prepare < IGNORE_PREPARE_VALUE_SIZE; ++ignore_prepare)
        for (int roundup_prepared = 0; roundup_prepared < 2; ++roundup_prepared)
            for (int roundup_read = 0; roundup_read < 2; ++roundup_read)
                for (int no_ts = 0; no_ts < 2; ++no_ts) {
                    int entry = MEDIUM_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_ts);
                    testutil_check(__wt_snprintf(medium_config[entry], sizeof(medium_config[entry]),
                      begin_transaction_config_printf_format, ignore_prepare_value[ignore_prepare],
                      boolean_value[(int)roundup_prepared], boolean_value[(int)roundup_read],
                      boolean_value[(int)no_ts]));
                }
}

static void
begin_transaction_medium(WT_SESSION *session, int ignore_prepare, bool roundup_prepared,
  bool roundup_read, bool no_timestamp)
{
    int entry;

    entry = MEDIUM_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_timestamp);
    testutil_check(session->begin_transaction(session, medium_config[entry]));
}

/*
 * A still faster implementation will require WiredTiger to be involved in the precompilation. It
 * requires an initialization step that needs to be run after wiredtiger_open and creates a
 * precompiled string that is valid for the life of the connection.  To be used, the parameters
 * need to be bound with a separate call.
 */
static void
begin_transaction_fast_init(WT_CONNECTION *conn, const char **compiled_ptr)
{
    testutil_check(conn->compile_configuration(conn, "WT_SESSION.begin_transaction",
      begin_transaction_config_precompile_format, compiled_ptr));
}

static void
begin_transaction_fast(WT_SESSION *session, const char *compiled, int ignore_prepare,
  bool roundup_prepared, bool roundup_read, bool no_timestamp)
{
    testutil_check(session->bind_configuration(session, compiled,
      ignore_prepare_value[ignore_prepare], roundup_prepared, roundup_read, no_timestamp));
    testutil_check(session->begin_transaction(session, compiled));
}

/*
 * Another fast implementation takes advantage of the finite number of configuration strings,
 * and calls the WiredTiger configuration compiler to get a precompiled string for each one.
 */
static void
begin_transaction_fast_alternate_init(WT_CONNECTION *conn, const char ***compiled_array_ptr)
{
    static const char *compiled_config[3 * 2 * 2 * 2];
#define MANY_COMPILED_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_ts) \
    (((((ignore_prepare * 2) + roundup_prepared) * 2) + roundup_read) * 2 + no_ts)

    char config[256];

    for (int ignore_prepare = 0; ignore_prepare < IGNORE_PREPARE_VALUE_SIZE; ++ignore_prepare)
        for (int roundup_prepared = 0; roundup_prepared < 2; ++roundup_prepared)
            for (int roundup_read = 0; roundup_read < 2; ++roundup_read)
                for (int no_ts = 0; no_ts < 2; ++no_ts) {
                    int entry =
                      MANY_COMPILED_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_ts);
                    testutil_check(
                      __wt_snprintf(config, sizeof(config), begin_transaction_config_printf_format,
                        ignore_prepare_value[ignore_prepare], boolean_value[(int)roundup_prepared],
                        boolean_value[(int)roundup_read], boolean_value[(int)no_ts]));
                    testutil_check(conn->compile_configuration(
                      conn, "WT_SESSION.begin_transaction", config, &compiled_config[entry]));
                }
    *compiled_array_ptr = compiled_config;
}

static void
begin_transaction_fast_alternate(WT_SESSION *session, const char **compiled_array,
  int ignore_prepare, bool roundup_prepared, bool roundup_read, bool no_timestamp)
{
    int entry = MANY_COMPILED_ENTRY(ignore_prepare, roundup_prepared, roundup_read, no_timestamp);
    testutil_check(session->begin_transaction(session, compiled_array[entry]));
}

/*
 * do_config_run --
 *     Run the test with or without configuration compilation.
 */
static void
do_config_run(TEST_OPTS *opts, int kind, const char *compiled, const char **compiled_array,
  bool check, uint64_t *nsec)
{
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    WT_TXN *txn;
    struct timespec before, after;
    uint32_t r;
    int i, ignore_prepare;
    bool roundup_prepared, roundup_read, no_timestamp;

    session = opts->session;

    /* Initialize the RNG. */
    __wt_random_init(&rnd);

#define NCALLS (WT_THOUSAND * 10)
    __wt_epoch(NULL, &before);
    for (i = 0; i < NCALLS; ++i) {
        r = __wt_random(&rnd);

        ignore_prepare = r % 3;
        roundup_prepared = ((r & 0x1) != 0);
        roundup_read = ((r & 0x2) != 0);
        no_timestamp = ((r & 0x4) != 0);

        switch (kind) {
        case 0:
            begin_transaction_slow(
              session, ignore_prepare, roundup_prepared, roundup_read, no_timestamp);
            break;
        case 1:
            begin_transaction_medium(
              session, ignore_prepare, roundup_prepared, roundup_read, no_timestamp);
            break;
        case 2:
            begin_transaction_fast(
              session, compiled, ignore_prepare, roundup_prepared, roundup_read, no_timestamp);
            break;
        case 3:
            begin_transaction_fast_alternate(session, compiled_array, ignore_prepare,
              roundup_prepared, roundup_read, no_timestamp);
            break;
        default:
            testutil_check(kind < 4);
            break;
        }

        if (check) {
            /*
             * Normal applications should not peer inside WT internals, but we need an easy way to
             * check that the configuration had the proper effect.
             */
            txn = ((WT_SESSION_IMPL *)session)->txn;
            if (ignore_prepare == 0) /* false */
                testutil_assert(
                  !F_ISSET(txn, WT_TXN_IGNORE_PREPARE) && !F_ISSET(txn, WT_TXN_READONLY));
            else if (ignore_prepare == 1) /* force */
                testutil_assert(
                  F_ISSET(txn, WT_TXN_IGNORE_PREPARE) && !F_ISSET(txn, WT_TXN_READONLY));
            else /* true */
                testutil_assert(
                  F_ISSET(txn, WT_TXN_IGNORE_PREPARE) && F_ISSET(txn, WT_TXN_READONLY));
            testutil_assert(roundup_prepared == F_ISSET(txn, WT_TXN_TS_ROUND_PREPARED));
            testutil_assert(roundup_read == F_ISSET(txn, WT_TXN_TS_ROUND_READ));
            testutil_assert(no_timestamp == F_ISSET(txn, WT_TXN_TS_NOT_SET));
        }

        testutil_check(session->rollback_transaction(session, NULL));
    }
    __wt_epoch(NULL, &after);
    *nsec +=
      (uint64_t)((after.tv_sec - before.tv_sec) * WT_BILLION + (after.tv_nsec - before.tv_nsec));
}

/*
 * main --
 *     TODO: Add a comment describing this function.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    const char *compiled_config, **compiled_config_array;
    uint64_t nsecs[4];
    int kind, runs;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL,
      "create,statistics=(all),statistics_log=(json,on_close,wait=1)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &opts->session));

    begin_transaction_medium_init();
    begin_transaction_fast_init(opts->conn, &compiled_config);
    begin_transaction_fast_alternate_init(opts->conn, &compiled_config_array);

    memset(nsecs, 0, sizeof(nsecs));

    /* Run the test, alternating the kinds of tests. */
#define NRUNS (100)
    for (runs = 0; runs < NRUNS; ++runs)
        for (kind = 0; kind < 4; ++kind) {
            do_config_run(
              opts, kind, compiled_config, compiled_config_array, runs == 0, &nsecs[kind]);
        }

    printf("number of calls: %d\n", NCALLS * NRUNS);
    for (kind = 0; kind < 4; ++kind)
        printf("kind = %d, total = %" PRIu64 "\n", kind, nsecs[kind]);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
