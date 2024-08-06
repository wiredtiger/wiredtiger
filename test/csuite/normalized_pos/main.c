/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "test_util.h"
#include "wt_internal.h"

extern int __wt_optind;
extern char *__wt_optarg;

static const char *uri = "table:normalized_pos";
static const int NUM_KEYS = 100000;

/*
 * usage --
 *     Print a usage message.
 */
__attribute__((noreturn)) static void
usage(void)
{
    fprintf(stderr, "usage: %s [-h dir]\n", progname);
    exit(EXIT_FAILURE);
}

/*
 * create_btree --
 *     Setup a btree with one key per page. Soft positions work on the in-memory btree, so use an
 *     in-memory version of WiredTiger to keep things simple when reasoning about the shape of the
 *     Btree.
 */
static void
create_btree(WT_CONNECTION *conn)
{
    WT_CURSOR *cursor;
    WT_SESSION *session;
    /* 1KB string to match the 1KB pages. */
    char val_str[1000];
    /* TODO - with 100,000 keys and 1 key per page this should mean that each key maps to an
     * equivalent npos. e.g. key 50,000 should map to roughly npos 0.5, and key 12,300 to 0.123.
     * This isn't true possibly because some pages have 10 slots and others 91? */

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, uri,
      "key_format=Q,value_format=S,memory_page_max=1KB,leaf_page_max=1KB,allocation_size=1KB"));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    memset(val_str, 'A', 1000);
    val_str[1000 - 1] = '\0';

    for (int i = 0; i < NUM_KEYS; i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, val_str);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
}

/*
 * test_normalized_pos --
 *     Given a key in a tree compute the normalized position (npos) of its page. Then make sure the
 *     soft position restores the same page.
 *
 * NOTE!! This is a white box test. It uses functions and types not available in the WiredTiger API.
 */
static void
test_normalized_pos(WT_CONNECTION *conn)
{
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle;
    WT_REF *page_ref, *restored_page_ref;
    WT_SESSION *session;
    WT_SESSION_IMPL *wt_session;
    double npos, prev_npos;
    size_t path_str_offset;
    char path_str[2][1024];

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    prev_npos = 0;
    path_str[0][0] = path_str[1][0] = 0;

    /*
     * Traverse the whole dataset to stabilize the tree and make sure that we don't cause page
     * splits by looking into pages.
     */
    for (int key = 0; key < NUM_KEYS; key++) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));
    }

    /*
     * Traverse the whole dataset, checking npos.
     */
    for (int key = 0; key < NUM_KEYS; key++) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));

        path_str_offset = 0;
        wt_session = (WT_SESSION_IMPL *)session;
        page_ref = ((WT_CURSOR_BTREE *)cursor)->ref;
        dhandle = ((WT_CURSOR_BTREE *)cursor)->dhandle;

        /* Compute the soft position (npos) of the page */
        npos = __wt_page_npos(wt_session, page_ref, 0.5, path_str[key & 1], &path_str_offset, 1024);
        /* printf("key %lu: npos = %f, path_str = %s\n", key, npos, path_str[key&1]); */

        /* We're walking through all pages in order. Each page should have a larger or equal npos */
        testutil_assertfmt(npos >= prev_npos,
          "Page containing key %" PRIu64 " %s has npos (%f) smaller than the page of key %" PRIu64
          ", (%f) %s",
          key, path_str[key & 1], npos, key - 1, prev_npos, path_str[(key & 1) ^ 1]);
        prev_npos = npos;

        /* Now find which page npos restores to. We haven't modified the Btree so it should be the
         * exact same page */
        WT_WITH_DHANDLE(wt_session, dhandle,
          testutil_check(__wt_page_from_npos_for_read(wt_session, &restored_page_ref, 0, 0, npos)));

        testutil_assertfmt(page_ref == restored_page_ref,
          "page mismatch for key %llu!\n  Expected %p, got %p\n  npos = %f", key, (void *)page_ref,
          (void *)restored_page_ref, npos);

        /* __wt_page_from_npos sets a hazard pointer on the found page. We need to clear it before
         * returning. */
        WT_WITH_DHANDLE(
          wt_session, dhandle, testutil_check(__wt_hazard_clear(wt_session, restored_page_ref)));
    }

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
}

/*
 * run --
 *     Create a btree with one key per page. Soft positions work on the in-memory btree, so use an
 *     in-memory version of WiredTiger to keep things simple when reasoning about the shape of the
 *     btree.
 *
 * Then, test that a computed npos returns to the same page it was derived from. This assumes no
 *     change the underlying btree during the test.
 */
static void
run(const char *working_dir)
{
    WT_CONNECTION *conn;
    char home[1024];

    testutil_work_dir_from_path(home, sizeof(home), working_dir);
    testutil_recreate_dir(home);

    /* Only run in memory. It's easier to reason about the shape of the Btree */
    /* TODO - Add test case for on-disk WiredTiger */
    testutil_check(wiredtiger_open(home, NULL, "create,in_memory=true,cache_size=1GB", &conn));

    create_btree(conn);
    test_normalized_pos(conn);

    testutil_check(conn->close(conn, ""));
    testutil_clean_test_artifacts(home);
    testutil_remove(home);
}

/*
 * main --
 *     Test correctness of normalized position.
 */
int
main(int argc, char *argv[])
{
    int ch;
    const char *working_dir;

    working_dir = "WT_TEST.normalized_pos";

    while ((ch = __wt_getopt(progname, argc, argv, "h:")) != EOF)
        switch (ch) {
        case 'h':
            working_dir = __wt_optarg;
            break;
        default:
            usage();
        }

    argc -= __wt_optind;
    if (argc != 0)
        usage();

    run(working_dir);
    return 0;
}
