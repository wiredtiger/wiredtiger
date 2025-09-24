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

#include "format.h"

/*
 * follower --
 *     Do follower stuff
 */
WT_THREAD_RET
follower(void *arg)
{
    SAP sap;
    WT_CONNECTION *conn;
    WT_ITEM checkpoint_metadata;
    WT_PAGE_LOG *page_log;
    WT_SESSION *session;
    // const char *ckpt_meta;
    char config[128];
    uint64_t checkpoint_ts;

    (void)(arg);

    conn = g.wts_conn_follower;

    memset(&sap, 0, sizeof(sap));
    memset(&checkpoint_metadata, 0, sizeof(checkpoint_metadata));
    wt_wrap_open_session(conn, &sap, NULL, NULL, &session);

    while (!g.follower_shutdown) {
        testutil_check(conn->get_page_log(conn, "palm", &page_log));
        testutil_check(page_log->pl_get_complete_checkpoint_ext(
          page_log, session, NULL, NULL, &checkpoint_ts, &checkpoint_metadata));
        testutil_snprintf(config, sizeof(config), "disaggregated=(checkpoint_meta=\"%.*s\")",
          (int)checkpoint_metadata.size, (const char *)checkpoint_metadata.data);
        testutil_check(conn->reconfigure(conn, config));
        // printf("checkpoint ts is %" PRIu64 "\n", checkpoint_ts);

        __wt_sleep(0, 300);
    }

    wt_wrap_close_session(session);

    return (WT_THREAD_RET_VALUE);
}

/*
 * follower_setup --
 *     Initialize followers.
 */
void
follower_setup(wt_thread_t *follower_tid)
{
    if (!disagg_is_mode_multi())
        return;

    memset(follower_tid, 0, sizeof(*follower_tid));
    testutil_check(__wt_thread_create(NULL, follower_tid, follower, NULL));
}

/*
 * follower_shutdown --
 *     Shutdown followers.
 */
void
follower_shutdown(wt_thread_t *follower_tid)
{
    if (!disagg_is_mode_multi())
        return;

    g.follower_shutdown = true;
    testutil_check(__wt_thread_join(NULL, follower_tid));
    wts_close(&g.wts_conn_follower);
    g.wts_conn_follower = NULL;
}
