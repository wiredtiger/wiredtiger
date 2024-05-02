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
#include <string.h>

/*
 * Test cursor set_raw_key_value
 */

#define NUM_RECORDS 10
#define BUF_SIZE 512

/* Constants and variables declaration. */
static const char conn_config[] =
  "create,cache_size=2GB,statistics=(all),statistics_log=(json,on_close,wait=1)";
static const char table_config[] = "key_format=u,value_format=u";

static const char uri[] = "table:wt11634_get_set";

/* Forward declarations. */
static void populate(WT_SESSION *);
static void validate(WT_SESSION *);

/*
 * validate --
 *     Validate the content.
 */
static void
validate(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    WT_ITEM got_key, got_value;
    uint64_t expected_key_len, expected_value_len, number_of_records;
    int ret;
    char expected_key[BUF_SIZE], expected_value[BUF_SIZE];

    number_of_records = 0;
    memset(expected_key, 0, sizeof(expected_key));
    memset(expected_value, 0, sizeof(expected_value));

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        testutil_snprintf(expected_key, sizeof(expected_key), "key%" PRIu64, number_of_records);
        testutil_snprintf(
          expected_value, sizeof(expected_value), "value%" PRIu64, number_of_records);

        expected_key_len = strlen(expected_key) + 1;
        expected_value_len = strlen(expected_value) + 1;

        testutil_check(cursor->get_key(cursor, &got_key));
        testutil_check(cursor->get_value(cursor, &got_value));

        testutil_assert(expected_key_len == got_key.size);
        testutil_assert(expected_value_len == got_value.size);

        testutil_assert(memcmp(got_key.data, expected_key, expected_key_len) == 0);
        testutil_assert(memcmp(got_value.data, expected_value, expected_value_len) == 0);

        ++number_of_records;
    }
    testutil_check(cursor->close(cursor));
}

/*
 * main --
 *     Methods implementation.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CONNECTION *conn;
    WT_SESSION *session;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    testutil_recreate_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL, conn_config, &conn));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Create and populate table. */
    testutil_check(session->create(session, uri, table_config));

    populate(session);

    validate(session);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}

/*
 * populate --
 *     Populate the table
 */
static void
populate(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    WT_ITEM key, value;
    uint64_t i;

    char key_buf[BUF_SIZE], value_buf[BUF_SIZE];

    memset(key_buf, 0, sizeof(key_buf));
    memset(value_buf, 0, sizeof(value_buf));

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < NUM_RECORDS; ++i) {
        testutil_snprintf(key_buf, sizeof(key_buf), "key%" PRIu64, i);
        testutil_snprintf(value_buf, sizeof(value_buf), "value%" PRIu64, i);

        key.data = key_buf;
        key.size = strlen(key_buf) + 1;

        value.data = value_buf;
        value.size = strlen(value_buf) + 1;

        cursor->set_raw_key_value(cursor, &key, &value);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    cursor = NULL;
}
