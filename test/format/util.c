/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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

void
track(const char *tag, uint64_t cnt, TINFO *tinfo)
{
    static size_t lastlen = 0;
    size_t len;
    char msg[128];

    if (g.c_quiet || tag == NULL)
        return;

    if (tinfo == NULL && cnt == 0)
        testutil_check(
          __wt_snprintf_len_set(msg, sizeof(msg), &len, "%4" PRIu32 ": %s", g.run_cnt, tag));
    else if (tinfo == NULL)
        testutil_check(__wt_snprintf_len_set(
          msg, sizeof(msg), &len, "%4" PRIu32 ": %s: %" PRIu64, g.run_cnt, tag, cnt));
    else
        testutil_check(__wt_snprintf_len_set(msg, sizeof(msg), &len, "%4" PRIu32 ": %s: "
                                                                     "search %" PRIu64 "%s, "
                                                                     "insert %" PRIu64 "%s, "
                                                                     "update %" PRIu64 "%s, "
                                                                     "remove %" PRIu64 "%s",
          g.run_cnt, tag, tinfo->search > M(9) ? tinfo->search / M(1) : tinfo->search,
          tinfo->search > M(9) ? "M" : "",
          tinfo->insert > M(9) ? tinfo->insert / M(1) : tinfo->insert,
          tinfo->insert > M(9) ? "M" : "",
          tinfo->update > M(9) ? tinfo->update / M(1) : tinfo->update,
          tinfo->update > M(9) ? "M" : "",
          tinfo->remove > M(9) ? tinfo->remove / M(1) : tinfo->remove,
          tinfo->remove > M(9) ? "M" : ""));

    if (lastlen > len) {
        memset(msg + len, ' ', (size_t)(lastlen - len));
        msg[lastlen] = '\0';
    }
    lastlen = len;

    if (printf("%s\r", msg) < 0)
        testutil_die(EIO, "printf");
    if (fflush(stdout) == EOF)
        testutil_die(errno, "fflush");
}

/*
 * path_setup --
 *     Build the standard paths and shell commands we use.
 */
void
path_setup(const char *home)
{
    size_t len;

    /* Home directory. */
    g.home = dstrdup(home == NULL ? "RUNDIR" : home);

    /* Log file. */
    len = strlen(g.home) + strlen("log") + 2;
    g.home_log = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_log, len, "%s/%s", g.home, "log"));

    /* History store dump file. */
    len = strlen(g.home) + strlen("HSdump") + 2;
    g.home_hsdump = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_hsdump, len, "%s/%s", g.home, "HSdump"));

    /* Page dump file. */
    len = strlen(g.home) + strlen("pagedump") + 2;
    g.home_pagedump = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_pagedump, len, "%s/%s", g.home, "pagedump"));

    /* RNG log file. */
    len = strlen(g.home) + strlen("rand") + 2;
    g.home_rand = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_rand, len, "%s/%s", g.home, "rand"));

    /* Run file. */
    len = strlen(g.home) + strlen("CONFIG") + 2;
    g.home_config = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_config, len, "%s/%s", g.home, "CONFIG"));

    /* Statistics file. */
    len = strlen(g.home) + strlen("stats") + 2;
    g.home_stats = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_stats, len, "%s/%s", g.home, "stats"));

/*
 * Home directory initialize command: create the directory if it doesn't exist, else remove
 * everything except the RNG log file.
 *
 * Redirect the "cd" command to /dev/null so chatty cd implementations don't add the new working
 * directory to our output.
 */
#undef CMD
#ifdef _WIN32
#define CMD                                             \
    "del /q rand.copy & "                               \
    "(IF EXIST %s\\rand copy /y %s\\rand rand.copy) & " \
    "(IF EXIST %s rd /s /q %s) & mkdir %s & "           \
    "(IF EXIST rand.copy copy rand.copy %s\\rand)"
    len = strlen(g.home) * 7 + strlen(CMD) + 1;
    g.home_init = dmalloc(len);
    testutil_check(
      __wt_snprintf(g.home_init, len, CMD, g.home, g.home, g.home, g.home, g.home, g.home, g.home));
#else
#define CMD                    \
    "test -e %s || mkdir %s; " \
    "cd %s > /dev/null && rm -rf `ls | sed /rand/d`"
    len = strlen(g.home) * 3 + strlen(CMD) + 1;
    g.home_init = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_init, len, CMD, g.home, g.home, g.home));
#endif

    /* Primary backup directory. */
    len = strlen(g.home) + strlen("BACKUP") + 2;
    g.home_backup = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_backup, len, "%s/%s", g.home, "BACKUP"));

/*
 * Backup directory initialize command, remove and re-create the primary backup directory, plus a
 * copy we maintain for recovery testing.
 */
#undef CMD
#ifdef _WIN32
#define CMD "rd /s /q %s\\%s %s\\%s & mkdir %s\\%s %s\\%s"
#else
#define CMD "rm -rf %s/%s %s/%s && mkdir %s/%s %s/%s"
#endif
    len = strlen(g.home) * 4 + strlen("BACKUP") * 2 + strlen("BACKUP_COPY") * 2 + strlen(CMD) + 1;
    g.home_backup_init = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_backup_init, len, CMD, g.home, "BACKUP", g.home,
      "BACKUP_COPY", g.home, "BACKUP", g.home, "BACKUP_COPY"));

/*
 * Salvage command, save the interesting files so we can replay the salvage command as necessary.
 *
 * Redirect the "cd" command to /dev/null so chatty cd implementations don't add the new working
 * directory to our output.
 */
#undef CMD
#ifdef _WIN32
#define CMD                                    \
    "cd %s && "                                \
    "rd /q /s slvg.copy & mkdir slvg.copy && " \
    "copy WiredTiger* slvg.copy\\ >:nul && copy wt* slvg.copy\\ >:nul"
#else
#define CMD                                   \
    "cd %s > /dev/null && "                   \
    "rm -rf slvg.copy && mkdir slvg.copy && " \
    "cp WiredTiger* wt* slvg.copy/"
#endif
    len = strlen(g.home) + strlen(CMD) + 1;
    g.home_salvage_copy = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_salvage_copy, len, CMD, g.home));
}

/*
 * rng_slow --
 *     Return a random number, doing the real work.
 */
uint32_t
rng_slow(WT_RAND_STATE *rnd)
{
    u_long ulv;
    uint32_t v;
    char *endptr, buf[64];

    /*
     * We can reproduce a single-threaded run based on the random numbers used in the initial run,
     * plus the configuration files.
     */
    if (g.replay) {
        if (fgets(buf, sizeof(buf), g.randfp) == NULL) {
            if (feof(g.randfp)) {
                fprintf(stderr,
                  "\n"
                  "end of random number log reached\n");
                exit(EXIT_SUCCESS);
            }
            testutil_die(errno, "random number log");
        }

        errno = 0;
        ulv = strtoul(buf, &endptr, 10);
        testutil_assert(errno == 0 && endptr[0] == '\n');
        testutil_assert(ulv <= UINT32_MAX);
        return ((uint32_t)ulv);
    }

    v = __wt_random(rnd);

    /* Save and flush the random number so we're up-to-date on error. */
    (void)fprintf(g.randfp, "%" PRIu32 "\n", v);
    (void)fflush(g.randfp);

    return (v);
}

/*
 * handle_init --
 *     Initialize logging/random number handles for a run.
 */
void
handle_init(void)
{
    /* Open/truncate logging/random number handles. */
    if (g.logging && (g.logfp = fopen(g.home_log, "w")) == NULL)
        testutil_die(errno, "fopen: %s", g.home_log);
    if ((g.randfp = fopen(g.home_rand, g.replay ? "r" : "w")) == NULL)
        testutil_die(errno, "%s", g.home_rand);
}

/*
 * handle_teardown --
 *     Shutdown logging/random number handles for a run.
 */
void
handle_teardown(void)
{
    /* Flush/close logging/random number handles. */
    fclose_and_clear(&g.logfp);
    fclose_and_clear(&g.randfp);
}

/*
 * fclose_and_clear --
 *     Close a file and clear the handle so we don't close twice.
 */
void
fclose_and_clear(FILE **fpp)
{
    FILE *fp;

    if ((fp = *fpp) == NULL)
        return;
    *fpp = NULL;
    if (fclose(fp) != 0)
        testutil_die(errno, "fclose");
    return;
}

/*
 * timestamp_once --
 *     Update the timestamp once.
 */
void
timestamp_once(void)
{
    static const char *oldest_timestamp_str = "oldest_timestamp=";
    WT_CONNECTION *conn;
    WT_DECL_RET;
    char buf[WT_TS_HEX_STRING_SIZE + 64];

    conn = g.wts_conn;

    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s", oldest_timestamp_str));

    /*
     * Lock out transaction timestamp operations. The lock acts as a barrier ensuring we've checked
     * if the workers have finished, we don't want that line reordered.
     */
    testutil_check(pthread_rwlock_wrlock(&g.ts_lock));

    ret = conn->query_timestamp(conn, buf + strlen(oldest_timestamp_str), "get=all_durable");
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);
    if (ret == 0)
        testutil_check(conn->set_timestamp(conn, buf));

    testutil_check(pthread_rwlock_unlock(&g.ts_lock));
}

/*
 * timestamp --
 *     Periodically update the oldest timestamp.
 */
WT_THREAD_RET
timestamp(void *arg)
{
    bool done;

    (void)(arg);

    /* Update the oldest timestamp at least once every 15 seconds. */
    done = false;
    do {
        /*
         * Do a final bump of the oldest timestamp as part of shutting down the worker threads,
         * otherwise recent operations can prevent verify from running.
         */
        if (g.workers_finished)
            done = true;
        else
            random_sleep(&g.rnd, 15);

        timestamp_once();

    } while (!done);

    return (WT_THREAD_RET_VALUE);
}

/*
 * alter --
 *     Periodically alter a table's metadata.
 */
WT_THREAD_RET
alter(void *arg)
{
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_SESSION *session;
    u_int period;
    char buf[32];
    bool access_value;

    (void)(arg);
    conn = g.wts_conn;

    /*
     * Only alter the access pattern hint. If we alter the cache resident setting we may end up with
     * a setting that fills cache and doesn't allow it to be evicted.
     */
    access_value = false;

    /* Open a session */
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    while (!g.workers_finished) {
        period = mmrand(NULL, 1, 10);

        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "access_pattern_hint=%s", access_value ? "random" : "none"));
        access_value = !access_value;
        /*
         * Alter can return EBUSY if concurrent with other operations.
         */
        while ((ret = session->alter(session, g.uri, buf)) != 0 && ret != EBUSY)
            testutil_die(ret, "session.alter");
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
    }

    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}
