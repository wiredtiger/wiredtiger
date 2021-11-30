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
 * track_ts_diff --
 *     Return a one character descriptor of relative timestamp values.
 */
static const char *
track_ts_diff(uint64_t left_ts, uint64_t right_ts)
{
    if (left_ts < right_ts)
        return "+";
    else if (left_ts == right_ts)
        return "=";
    else
        return "-";
}

/*
 * track_ts_dots --
 *     Return an entry in the time stamp progress indicator.
 */
static const char *
track_ts_dots(u_int dot_count)
{
    static const char *dots[] = {"   ", ".  ", ".. ", "..."};

    return (dots[dot_count % WT_ELEMENTS(dots)]);
}

/*
 * track_write --
 *     Write out a tracking message.
 */
static void
track_write(char *msg, size_t len)
{
    static size_t last_len; /* callers must be single-threaded */

    if (last_len > len) {
        memset(msg + len, ' ', (size_t)(last_len - len));
        msg[last_len] = '\0';
    }
    last_len = len;

    if (printf("%s\r", msg) < 0)
        testutil_die(EIO, "printf");
    if (fflush(stdout) == EOF)
        testutil_die(errno, "fflush");
}

/*
 * track_ops --
 *     Show a status line of operations and time stamp progress.
 */
void
track_ops(TINFO *tinfo)
{
    static uint64_t last_cur, last_old, last_stable;
    static u_int cur_dot_cnt, old_dot_cnt, stable_dot_cnt;
    size_t len;
    uint64_t cur_ts, old_ts, stable_ts;
    char msg[128], ts_msg[64];

    if (GV(QUIET))
        return;

    ts_msg[0] = '\0';
    if (g.transaction_timestamps_config) {
        /*
         * Don't worry about having a completely consistent set of timestamps.
         */
        old_ts = g.oldest_timestamp;
        stable_ts = g.stable_timestamp;
        cur_ts = g.timestamp;

        if (old_ts != last_old) {
            ++old_dot_cnt;
            last_old = old_ts;
        }
        if (stable_ts != last_stable) {
            ++stable_dot_cnt;
            last_stable = stable_ts;
        }
        if (cur_ts != last_cur) {
            ++cur_dot_cnt;
            last_cur = cur_ts;
        }

        testutil_check(__wt_snprintf(ts_msg, sizeof(ts_msg),
          " old%s"
          "stb%s%s"
          "ts%s%s",
          track_ts_dots(old_dot_cnt), track_ts_diff(old_ts, stable_ts),
          track_ts_dots(stable_dot_cnt), track_ts_diff(stable_ts, cur_ts),
          track_ts_dots(cur_dot_cnt)));
    }
    testutil_check(__wt_snprintf_len_set(msg, sizeof(msg), &len,
      "ops: "
      "S %" PRIu64
      "%s, "
      "I %" PRIu64
      "%s, "
      "U %" PRIu64
      "%s, "
      "R %" PRIu64
      "%s, "
      "M %" PRIu64
      "%s, "
      "T %" PRIu64 "%s%s",
      tinfo->search > M(9) ? tinfo->search / M(1) : tinfo->search, tinfo->search > M(9) ? "M" : "",
      tinfo->insert > M(9) ? tinfo->insert / M(1) : tinfo->insert, tinfo->insert > M(9) ? "M" : "",
      tinfo->update > M(9) ? tinfo->update / M(1) : tinfo->update, tinfo->update > M(9) ? "M" : "",
      tinfo->remove > M(9) ? tinfo->remove / M(1) : tinfo->remove, tinfo->remove > M(9) ? "M" : "",
      tinfo->modify > M(9) ? tinfo->modify / M(1) : tinfo->modify, tinfo->modify > M(9) ? "M" : "",
      tinfo->truncate > M(9) ? tinfo->truncate / M(1) : tinfo->truncate,
      tinfo->truncate > M(9) ? "M" : "", ts_msg));

    track_write(msg, len);
}

/*
 * track --
 *     Show general operation progress.
 */
void
track(const char *tag, uint64_t cnt)
{
    size_t len;
    char msg[128];

    if (GV(QUIET))
        return;

    if (cnt == 0)
        testutil_check(__wt_snprintf_len_set(msg, sizeof(msg), &len, "%s", tag));
    else
        testutil_check(__wt_snprintf_len_set(msg, sizeof(msg), &len, "%s: %" PRIu64, tag, cnt));

    track_write(msg, len);
}

/*
 * path_setup --
 *     Build the standard paths and shell commands we use.
 */
void
path_setup(const char *home)
{
    size_t len;
    const char *name;

    /* Home directory. */
    g.home = dstrdup(home == NULL ? "RUNDIR" : home);

    /* Configuration file. */
    name = "CONFIG";
    len = strlen(g.home) + strlen(name) + 2;
    g.home_config = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_config, len, "%s/%s", g.home, name));

    /* Key length configuration file. */
    name = "CONFIG.keylen";
    len = strlen(g.home) + strlen(name) + 2;
    g.home_key = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_key, len, "%s/%s", g.home, name));

    /* History store dump file. */
    name = "FAIL.HSdump";
    len = strlen(g.home) + strlen(name) + 2;
    g.home_hsdump = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_hsdump, len, "%s/%s", g.home, name));

    /* Page dump file. */
    name = "FAIL.pagedump";
    len = strlen(g.home) + strlen(name) + 2;
    g.home_pagedump = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_pagedump, len, "%s/%s", g.home, name));

    /* Statistics file. */
    name = "OPERATIONS.stats";
    len = strlen(g.home) + strlen(name) + 2;
    g.home_stats = dmalloc(len);
    testutil_check(__wt_snprintf(g.home_stats, len, "%s/%s", g.home, name));
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
}

/*
 * set_oldest_timestamp --
 *     Query the oldest timestamp from wiredtiger and set it as our global oldest timestamp. This
 *     should only be called on runs for pre existing databases.
 */
void
set_oldest_timestamp(void)
{
    static const char *oldest_timestamp_str = "oldest_timestamp=";

    WT_CONNECTION *conn;
    WT_DECL_RET;
    uint64_t oldest_ts;
    char buf[WT_TS_HEX_STRING_SIZE * 2 + 64], tsbuf[WT_TS_HEX_STRING_SIZE];

    conn = g.wts_conn;

    if ((ret = conn->query_timestamp(conn, tsbuf, "get=oldest")) == 0) {
        testutil_timestamp_parse(tsbuf, &oldest_ts);
        g.timestamp = oldest_ts;
        testutil_check(
          __wt_snprintf(buf, sizeof(buf), "%s%" PRIx64, oldest_timestamp_str, g.oldest_timestamp));
    } else if (ret != WT_NOTFOUND)
        /*
         * Its possible there may not be an oldest timestamp as such we could get not found. This
         * should be okay assuming timestamps are not configured if they are, it's still okay as we
         * could have configured timestamps after not running with timestamps. As such only error if
         * we get a non not found error. If we were supposed to fail with not found we'll see an
         * error later on anyway.
         */
        testutil_die(ret, "unable to query oldest timestamp");
}

/*
 * lock_init --
 *     Initialize abstract lock that can use either pthread of wt reader-writer locks.
 */
void
lock_init(WT_SESSION *session, RWLOCK *lock)
{
    testutil_assert(lock->lock_type == LOCK_NONE);

    if (GV(WIREDTIGER_RWLOCK)) {
        testutil_check(__wt_rwlock_init((WT_SESSION_IMPL *)session, &lock->l.wt));
        lock->lock_type = LOCK_WT;
    } else {
        testutil_check(pthread_rwlock_init(&lock->l.pthread, NULL));
        lock->lock_type = LOCK_PTHREAD;
    }
}

/*
 * lock_destroy --
 *     Destroy abstract lock.
 */
void
lock_destroy(WT_SESSION *session, RWLOCK *lock)
{
    switch (lock->lock_type) {
    case LOCK_NONE:
        break;
    case LOCK_PTHREAD:
        testutil_check(pthread_rwlock_destroy(&lock->l.pthread));
        break;
    case LOCK_WT:
        __wt_rwlock_destroy((WT_SESSION_IMPL *)session, &lock->l.wt);
        break;
    }
    lock->lock_type = LOCK_NONE;
}

/*
 * set_core --
 *     Turn core dumps off/on.
 */
void
set_core(bool off)
{
#ifdef HAVE_SETRLIMIT
    static bool saved = false;
    static struct rlimit saved_rlim;
    struct rlimit rlim;

    /*
     * This could race if a lot of threads failed at the same time, but it's unlikely (and
     * unimportant) enough that I'm not fixing it.
     */
    if (!saved) {
        testutil_assert_errno(getrlimit(RLIMIT_CORE, &saved_rlim) == 0);
        saved = true;
    }
    rlim = saved_rlim;
    if (off)
        rlim.rlim_cur = 0;
    testutil_assert_errno(setrlimit(RLIMIT_CORE, &rlim) == 0);
#endif
}

/*
 * atou32 --
 *     String to uint32_t helper function.
 */
uint32_t
atou32(const char *tag, const char *s, int match)
{
    long v;
    char *endptr;

    errno = 0;
    v = strtol(s, &endptr, 10);
    if ((errno == ERANGE && (v == LONG_MAX || v == LONG_MIN)) || (errno != 0 && v == 0) ||
      *endptr != match || v < 0 || v > UINT32_MAX)
        testutil_die(EINVAL, "%s: %s: illegal numeric value or value out of range", progname, tag);
    return ((uint32_t)v);
}
