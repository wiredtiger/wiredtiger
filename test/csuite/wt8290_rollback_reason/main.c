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

const char *rollback_error = "oldest pinned transaction ID rolled back for eviction";

/*
 * JIRA ticket reference: WT-4699 Test case description: Use a JSON dump cursor on a projection, and
 * overwrite the projection string. Failure mode: On the first retrieval of a JSON key/value, a
 * configure parse error is returned.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *c;
    WT_DECL_RET;
    WT_SESSION *session;
    const char *rollback_reason, *reason_2;
    WT_UNUSED(c);
    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    testutil_check(wiredtiger_open(opts->home, NULL, "create,cache_size=1M", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->create(session, opts->uri, "key_format=i,value_format=S"));
    testutil_check(session->open_cursor(session, opts->uri, NULL, NULL, &c));
    rollback_reason = session->get_rollback_reason(session);
    testutil_assert(rollback_reason == NULL);

    testutil_check(session->begin_transaction(session, NULL));
    for (int i = 0; i < 20000; i++) {
        c->set_key(c, i);
        c->set_value(c, "abcdefghijklmnopqrstuvwxyz");
        ret = c->insert(c);
        if (ret != 0) {
            if (ret == WT_ROLLBACK) {
                reason_2 = session->get_rollback_reason(session);
                testutil_assert(strcmp(reason_2, rollback_error) == 0);
                break;
            } else
                testutil_die(ret, "Unexpected error occurred while inserting values.");
        }
    }
    testutil_check(session->rollback_transaction(session, NULL));
    rollback_reason = session->get_rollback_reason(session);
    testutil_assert(strcmp(rollback_reason, rollback_error) == 0);

    testutil_check(session->close(session, NULL));
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
