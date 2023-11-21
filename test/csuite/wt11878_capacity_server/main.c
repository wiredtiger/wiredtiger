#include <test_util.h>

static const char *home = "WT_TEST";

int main(void)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    int i;
    char buf[1024];

    testutil_recreate_dir(home);

    testutil_check(wiredtiger_open(home, NULL, "create, io_capacity=(total=1M),verbose=[all:1, metadata:0, api:0]", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    testutil_check(session->create(session, "table:access", "key_format=S,value_format=S"));

    testutil_check(session->open_cursor(session, "table:access", NULL, NULL, &cursor));

    for (i = 0; i < 100000; i++) {
        snprintf(buf, sizeof(buf), "key%d", i);
        cursor->set_key(cursor, buf);

        cursor->set_value(cursor, "old value  ###############################################################################################################################################################################################################");
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    testutil_check(conn->close(conn, NULL)); /* Close all handles. */

    return (EXIT_SUCCESS);
}
