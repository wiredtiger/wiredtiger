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

#include <pthread.h>
#include <signal.h>
#include <unistd.h>

/*
 * Opening a backup cursor forces the log into a new file and then waits for allocation to move past
 * the file it captured. If another thread is already switching slots, that switch can publish its
 * new file into alloc_lsn and consume the backup's force-new-file request without rolling past the
 * captured file. The retry loop then has nothing to do and never returns. The log_newfile_slow
 * timing stress pauses between publishing the new file and consuming the request, which makes that
 * interleaving common enough to reproduce.
 *
 * Each attempt runs a writer for a short burst, stops it while a log file roll may still be in
 * flight, then opens and closes a backup cursor. A healthy build completes every attempt; the test
 * prints the number of attempts when it finishes. If an open has not returned within
 * BACKUP_TIMEOUT_SECS the alarm handler exits with a failure, because the hanging call spins inside
 * the library and there is nothing to unwind. Nothing is copied or compared: the test only asserts
 * that opening the cursor returns.
 */

#define ENV_CONFIG                                                     \
    "create,log=(enabled,file_max=100KB,prealloc=false,remove=false)," \
    "timing_stress_for_test=[log_newfile_slow]"

#define BACKUP_TIMEOUT_SECS 20
#define MAX_ATTEMPTS 50
#define V_SIZE 1024

static TEST_OPTS *opts, _opts;
static const char *const uri = "table:main";

static void on_alarm(int) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * on_alarm --
 *     Fail the test if a backup cursor open does not return.
 */
static void
on_alarm(int signo)
{
    static const char msg[] = "backup cursor open did not return\n";

    (void)signo;
    (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(EXIT_FAILURE);
}

/*
 * writer_main --
 *     Insert records until the main thread clears the running flag.
 */
static void *
writer_main(void *arg)
{
    WT_CURSOR *cursor;
    WT_SESSION *session;
    uint32_t i;
    char k[64], v[V_SIZE];

    (void)arg;
    memset(v, 'a', sizeof(v));
    v[sizeof(v) - 1] = '\0';

    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    for (i = 0; opts->running; ++i) {
        testutil_snprintf(k, sizeof(k), "key%08" PRIu32, i);
        cursor->set_key(cursor, k);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, NULL));
    return (NULL);
}

/*
 * main --
 *     Run the backup cursor reproducer.
 */
int
main(int argc, char *argv[])
{
    WT_CURSOR *backup_cursor;
    pthread_t thread;
    int attempt, ret;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    (void)signal(SIGALRM, on_alarm);

    testutil_check(wiredtiger_open(opts->home, NULL, ENV_CONFIG, &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &opts->session));
    testutil_check(opts->session->create(opts->session, uri, "key_format=S,value_format=S"));

    for (attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt) {
        /*
         * Let the writer fill log files, then stop it just before the backup cursor is opened. The
         * stop lands while the writer is likely inside the slowed new-file window.
         */
        opts->running = true;
        testutil_assert(pthread_create(&thread, NULL, writer_main, NULL) == 0);
        usleep(200 * 1000);
        opts->running = false;

        (void)alarm(BACKUP_TIMEOUT_SECS);
        ret = opts->session->open_cursor(opts->session, "backup:", NULL, NULL, &backup_cursor);
        (void)alarm(0);
        testutil_check(ret);
        testutil_check(backup_cursor->close(backup_cursor));
        testutil_assert(pthread_join(thread, NULL) == 0);
    }

    printf("no stuck backup cursor in %d attempts\n", MAX_ATTEMPTS);
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
