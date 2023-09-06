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

#include <wt_internal.h>
#include "test_util.h"

extern int __wt_optind;
extern char *__wt_optarg;

/*
 * cmp_int --
 *     Integer comparison.
 */
static int
cmp_int(const void *a, const void *b)
{
    return (__wt_int_compare_p((int *)a, (int *)b));
}

/*
 * run --
 *     Run the test.
 */
static void
run(WT_SESSION_IMPL *session, size_t num_keys)
{
    WT_INT_NODE *next, *node;
    WT_INT_SKIPLIST skiplist;
    WT_RAND_STATE rnd;
    size_t n, s;
    u_int depth;
    int *all_keys, last, v;

    __wt_random_init_seed(NULL, &rnd);

    memset(&skiplist, 0, sizeof(skiplist));
    testutil_check(__wt_spin_init(session, &skiplist.lock, "Skip list lock"));
    testutil_check(__wt_calloc(session, num_keys, sizeof(*all_keys), &all_keys));

    /* Insert into the skip list and into an array, so that we can check our work. */
    for (n = 0; n < num_keys; n++) {
        v = (int)__wt_random(&rnd); /* This cast is ok - we don't care about sign. */
        all_keys[n] = v;            /* Remember for later. */

        depth = __wt_skip_choose_depth(session);
        s = sizeof(*node) + depth * sizeof(node->next[0]);
        testutil_check(__wt_malloc(session, s, &node));
        memset(node, 0, s);
        node->key = v;

        /* Add to the skip list, ignoring duplicate keys (as they are not currently supported). */
        testutil_check_error_ok(
          __wt_skip_insert__int(session, &skiplist.lock, &skiplist.head, node, depth, false),
          EEXIST);
    }

    /* Check the containment. */
    for (n = 0; n < num_keys; n++)
        testutil_assert(__wt_skip_contains__int(session, &skiplist.head, &all_keys[n]));

    /* Sort the array for checking. */
    __wt_qsort(all_keys, num_keys, sizeof(*all_keys), cmp_int);

    /* Check for keys that are not supposed to exist. */
    for (n = 0; n < num_keys; n++) {
        v = (int)__wt_random(&rnd);
        if (bsearch(&v, all_keys, num_keys, sizeof(*all_keys), cmp_int) == NULL)
            testutil_assert(!__wt_skip_contains__int(session, &skiplist.head, &v));
    }

    /* Compare the skip list to the sorted array, ignoring duplicate elements. */
    last = 0;
    n = 0;
    WT_SKIP_FOREACH (node, &skiplist.head) {
        if (n > 0)
            while (n < num_keys && all_keys[n] == last)
                n++;
        last = v = all_keys[n];
        testutil_assert(n <= num_keys);
        testutil_assert(node->key == v);
        n++;
    }
    testutil_assert(n == num_keys);

    /* Clean up. */
    for (node = WT_SKIP_FIRST(&skiplist.head); node != NULL; node = next) {
        next = WT_SKIP_NEXT(node);
        __wt_free(session, node);
    }
    __wt_spin_destroy(session, &skiplist.lock);
    __wt_free(session, all_keys);
}

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
 * main --
 *     Stress test for out-of-order reads in __wt_search_insert on platforms with weak memory
 *     ordering.
 */
int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    int ch;
    char home[PATH_MAX];
    const char *working_dir;

    (void)testutil_set_progname(argv);
    working_dir = "WT_TEST.skiplist";

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

    /* Create a database, just so that we can get a session. */
    testutil_work_dir_from_path(home, sizeof(home), working_dir);
    testutil_recreate_dir(home);
    testutil_check(wiredtiger_open(home, NULL, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Run the test. */
    run((WT_SESSION_IMPL *)session, WT_MILLION);

    /* Finish. */
    testutil_check(session->close(session, ""));
    testutil_check(conn->close(conn, ""));
    testutil_clean_test_artifacts(home);
    testutil_remove(home);
    return 0;
}
