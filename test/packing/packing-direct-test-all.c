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
#include "log_auto_test.h"

/* bilerplate from test_bloom.c */

#define HOME_SIZE 512

static struct {
    WT_CONNECTION *wt_conn; /* WT_CONNECTION handle */
    WT_SESSION *wt_session; /* WT_SESSION handle */

    char *config_open; /* Command-line configuration */

    uint32_t c_cache; /* Config values */
    uint32_t c_key_max;
    uint32_t c_ops;
    uint32_t c_k;      /* Number of hash iterations */
    uint32_t c_factor; /* Number of bits per item */

    WT_RAND_STATE rand;

    uint8_t **entries;
} g;

/*
 * setup --
 *     TODO: Add a comment describing this function.
 */
static void
setup(void)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    char config[512];
    static char home[HOME_SIZE]; /* Base home directory */

    g.c_cache = 10;
    g.c_ops = 100 * WT_THOUSAND;
    g.c_key_max = 100;
    g.c_k = 8;
    g.c_factor = 16;

    testutil_work_dir_from_path(home, HOME_SIZE, "WT_TEST");

    /* Create the home test directory for the test (delete the previous directory if it exists). */
    testutil_recreate_dir(home);

    /*
     * This test doesn't test public Wired Tiger functionality, it still needs connection and
     * session handles.
     */

    /*
     * Open configuration -- put command line configuration options at the end so they can override
     * "standard" configuration.
     */
    testutil_snprintf(config, sizeof(config),
      "create,statistics=(all),error_prefix=\"%s\",cache_size=%" PRIu32 "MB,%s", progname,
      g.c_cache, g.config_open == NULL ? "" : g.config_open);

    testutil_check(wiredtiger_open(home, NULL, config, &conn));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    g.wt_conn = conn;
    g.wt_session = session;
}

/*
 * cleanup --
 *     TODO: Add a comment describing this function.
 */
static void
cleanup(void)
{
    uint32_t i;

    for (i = 0; i < g.c_ops; i++)
        free(g.entries[i]);
    free(g.entries);
    testutil_check(g.wt_session->close(g.wt_session, NULL));
    testutil_check(g.wt_conn->close(g.wt_conn, NULL));
}

/*
 * run --
 *     Run the test.
 */
static void
run(void)
{
    test_cmp_all((WT_SESSION_IMPL *)g.wt_session);
}

/*
 * main --
 *     Test valid and invalid format strings to pack data.
 */
int
main(int argc, char **argv)
{
    (void)argc;
    (void)testutil_set_progname(argv);
    /*
     * Required on some systems to pull in parts of the library for which we have data references.
     */
    testutil_check(__wt_library_init());

    setup();
    run();
    cleanup();

    return (0);
}
