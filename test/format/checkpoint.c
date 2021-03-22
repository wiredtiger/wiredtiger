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
 * wts_checkpoints --
 *     Configure WiredTiger library checkpoints.
 */
void
wts_checkpoints(void)
{
    char config[1024];

    /*
     * Configuring WiredTiger library checkpoints is done separately, rather than as part of the
     * original database open because format tests small caches and you can get into cache stuck
     * trouble during the initial load (where bulk load isn't configured). There's a single thread
     * doing lots of inserts and creating huge leaf pages. Those pages can't be evicted if there's a
     * checkpoint running in the tree, and the cache can get stuck. That workload is unlikely enough
     * we're not going to fix it in the library, so configure it away by delaying checkpoint start.
     */
    if (g.c_checkpoint_flag != CHECKPOINT_WIREDTIGER)
        return;

    testutil_check(
      __wt_snprintf(config, sizeof(config), ",checkpoint=(wait=%" PRIu32 ",log_size=%" PRIu32 ")",
        g.c_checkpoint_wait, MEGABYTE(g.c_checkpoint_log_size)));
    testutil_check(g.wts_conn->reconfigure(g.wts_conn, config));
}

/*
 * checkpoint --
 *     Periodically take a checkpoint in a format thread.
 */
WT_THREAD_RET
checkpoint(void *arg)
{
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_SESSION *session;
    u_int secs;
    char config_buf[64];
    const char *ckpt_config;
    bool backup_locked;

    (void)arg;
    conn = g.wts_conn;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    for (secs = mmrand(NULL, 1, 10); !g.workers_finished;) {
        if (secs > 0) {
            __wt_sleep(1, 0);
            --secs;
            continue;
        }

        /*
         * LSM and data-sources don't support named checkpoints. Also, don't attempt named
         * checkpoints during a hot backup. It's OK to create named checkpoints during a hot backup,
         * but we can't delete them, so repeating an already existing named checkpoint will fail
         * when we can't drop the previous one.
         */
        ckpt_config = NULL;
        backup_locked = false;
        if (!DATASOURCE("lsm"))
            switch (mmrand(NULL, 1, 20)) {
            case 1:
                /*
                 * 5% create a named snapshot. Rotate between a
                 * few names to test multiple named snapshots in
                 * the system.
                 */
                ret = lock_try_writelock(session, &g.backup_lock);
                if (ret == 0) {
                    backup_locked = true;
                    testutil_check(__wt_snprintf(
                      config_buf, sizeof(config_buf), "name=mine.%" PRIu32, mmrand(NULL, 1, 4)));
                    ckpt_config = config_buf;
                } else if (ret != EBUSY)
                    testutil_check(ret);
                break;
            case 2:
                /*
                 * 5% drop all named snapshots.
                 */
                ret = lock_try_writelock(session, &g.backup_lock);
                if (ret == 0) {
                    backup_locked = true;
                    ckpt_config = "drop=(all)";
                } else if (ret != EBUSY)
                    testutil_check(ret);
                break;
            }

        testutil_check(session->checkpoint(session, ckpt_config));

        if (backup_locked)
            lock_writeunlock(session, &g.backup_lock);

        secs = mmrand(NULL, 5, 40);
    }

    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}
