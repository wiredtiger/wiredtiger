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
 * wt17691_early_load_check
 *     Exercise the wiredtiger_open guardrail that rejects an open when an early_load=true
 *     extension recorded in WiredTiger.basecfg was not also passed in the open configuration.
 *
 * The test creates a database with the lz4 compressor marked early_load=true, then performs two
 * reopens:
 *   1. With no extensions in the open config -- wiredtiger_open must return EINVAL because the
 *      basecfg entry is left unloaded.
 *   2. With the same extensions list -- the open must succeed.
 */

#ifndef LZ4_PATH
#define LZ4_PATH "ext/compressors/lz4/libwiredtiger_lz4.so"
#endif

int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    int ret;
    char buf[1024], config[1024], lz4_full[1024];

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    testutil_build_dir(opts, buf, sizeof(buf));
    testutil_snprintf(lz4_full, sizeof(lz4_full), "%s/%s", buf, LZ4_PATH);
    if (access(lz4_full, R_OK) != 0) {
        printf("Skipped: lz4 extension not built\n");
        testutil_cleanup(opts);
        return (EXIT_SUCCESS);
    }
    testutil_snprintf(
      config, sizeof(config), "create,extensions=[\"%s\"=(early_load=true)]", lz4_full);

    /* Create the database. basecfg records the early-load entry. */
    testutil_check(wiredtiger_open(opts->home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, NULL));

    /* Reopen with no extensions in the open config. The guardrail must reject the open. */
    ret = wiredtiger_open(opts->home, NULL, NULL, &conn);
    testutil_assertfmt(
      ret == EINVAL, "expected EINVAL, got %d (%s)", ret, wiredtiger_strerror(ret));

    /* Reopen with the same extensions list. The guardrail must not fire. */
    testutil_check(wiredtiger_open(opts->home, NULL, config, &conn));
    testutil_check(conn->close(conn, NULL));

    printf("Success\n");
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
