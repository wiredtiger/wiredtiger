#include "test_util.h"

#define TABLE_NAME "table"
#define TABLE_URI ("table:" TABLE_NAME)
#define VAL_A "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
#define VAL_B "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
#define NUM_THINGS 1000000

int
main(int argc, char *argv[])
{
    int ret = 0;
    int removals = 0;
    uint64_t k;
    const char *home = "asdf";
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;

    (void)argc;
    (void)argv;

    testutil_recreate_dir(home);
    testutil_assert_errno(chdir(home) == 0);

    /* Automatically flush after each newline, so that we don't miss any messages if we crash. */
    __wt_stream_set_line_buffer(stderr);
    __wt_stream_set_line_buffer(stdout);

    testutil_check(wiredtiger_open(".", NULL, "create=true", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, TABLE_URI, "key_format=r,value_format=S"));

    testutil_check(session->open_cursor(session, TABLE_URI, NULL, "append=true", &cursor));

    /* Insert things. */
    for (uint64_t i = 0; i < NUM_THINGS; i++) {
        cursor->set_value(cursor, i % 2 ? VAL_A : VAL_B);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(conn->set_timestamp(conn, "stable_timestamp=10"));
    testutil_check(session->checkpoint(session, NULL));

    /* Delete the things. */
    cursor->set_key(cursor, 1);
    testutil_check(cursor->search(cursor));
    while (ret == 0) {
        testutil_check(cursor->remove(cursor));
        ++removals;
        ret = cursor->next(cursor);
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_assert(removals == NUM_THINGS);

    /* RTS */
    testutil_check(conn->rollback_to_stable(conn, NULL));

    cursor->set_value(cursor, VAL_A);
    testutil_check(cursor->insert(cursor));

    testutil_check(cursor->get_key(cursor, &k));
    printf("k=%" PRIu64 "\n", k);
    cursor->set_key(cursor, k);
    testutil_check(cursor->search(cursor));

    return (EXIT_SUCCESS);
}
