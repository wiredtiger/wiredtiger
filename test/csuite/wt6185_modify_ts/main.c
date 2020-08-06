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

#include "test_util.h"

extern int __wt_optind;
extern char *__wt_optarg;

#define KEYNO 10
#define MAX_MODIFY_ENTRIES 5
#define MAX_OPS 500
#define RUNS 10
#define VALUE_SIZE 2000

#define URI_MOD_TABLE   "table:mod"
#define URI_FULL_TABLE   "table:full"
#define URI_FULL_TS_TABLE   "table:full_ts"

static WT_RAND_STATE rnd;

static char *tlist[MAX_OPS * 5000]; /* List of traced operations. */
static u_int tnext;

static uint64_t ts; /* Current timestamp. */
static uint64_t ts_old; /* Current oldest timestamp. */

static char key[500], modify_repl[2560], tmp[40 * 1024];
static u_int key_int;

/*
 * trace --
 *     Trace an operation.
 */
#define trace(fmt, ...)                                                    \
    do {                                                                   \
        testutil_assert(tnext < WT_ELEMENTS(tlist));                       \
        testutil_check(__wt_snprintf(tmp, sizeof(tmp), fmt, __VA_ARGS__)); \
        free(tlist[tnext]);                                                \
        tlist[tnext] = dstrdup(tmp);                                       \
        ++tnext;                                                           \
    } while (0)

/*
 * usage --
 *     Print usage message and exit.
 */
static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
usage(void)
{
    fprintf(stderr, "usage: %s [-ce] [-h home] [-S seed]\n", progname);
    exit(EXIT_FAILURE);
}

/*
 * cleanup --
 *     Discard allocated memory in case it's a sanitizer run.
 */
static void
cleanup(void)
{
    u_int i;

    for (i = 0; i < WT_ELEMENTS(tlist); ++i)
        free(tlist[i]);
}

/*
 * mmrand --
 *     Return a random value between a min/max pair, inclusive.
 */
static inline uint32_t
mmrand(u_int min, u_int max)
{
    uint32_t v;
    u_int range;

    /*
     * Test runs with small row counts can easily pass a max of 0 (for example, "g.rows / 20").
     * Avoid the problem.
     */
    if (max <= min)
        return (min);

    v = __wt_random(&rnd);
    range = (max - min) + 1;
    v %= range;
    v += min;
    return (v);
}

/*
 * modify_repl_init --
 *     Initialize the replacement information.
 */
static void
modify_repl_init(void)
{
    size_t i;

    for (i = 0; i < sizeof(modify_repl); ++i)
        modify_repl[i] = "zyxwvutsrqponmlkjihgfedcba"[i % 26];
}

#define SET_VALUE(key, value, init)                                                     \
    do {                                                                                \
        char *__p;                                                                      \
        memset(value, '.', sizeof(value));                                              \
        value[sizeof(value) - 1] = '\0';                                                \
        testutil_check(__wt_snprintf(value, sizeof(value), "%010u.value", (u_int)key)); \
        for (__p = value; *__p != '\0'; ++__p)                                          \
            ;                                                                           \
        *__p = (init);                                                                  \
    } while (0)

/*
 * modify_build --
 *     Generate a set of modify vectors.
 */
static void
modify_build(WT_MODIFY *entries, int *nentriesp, int tag)
{
    int i, nentries;

    /* Randomly select a number of byte changes, offsets and lengths. */
    nentries = (int)mmrand(1, MAX_MODIFY_ENTRIES);
    for (i = 0; i < nentries; ++i) {
        /*
         * Take between 0 and 10 bytes from a random spot in the modify data. Replace between 0 and
         * 10 bytes in a random spot in the value, but start at least 11 bytes into the buffer so we
         * skip leading key information.
         */
        entries[i].data.data = modify_repl + mmrand(1, sizeof(modify_repl) - 10);
        entries[i].data.size = (size_t)mmrand(0, 10);
        entries[i].offset = (size_t)mmrand(15, VALUE_SIZE);
        entries[i].size = (size_t)mmrand(0, 10);
        trace("modify %d: off=%" WT_SIZET_FMT ", size=%" WT_SIZET_FMT ", data=\"%.*s\"", tag,
          entries[i].offset, entries[i].size, (int)entries[i].data.size,
          (char *)entries[i].data.data);
    }

    *nentriesp = (int)nentries;
}

/*
 * modify --
 *     Make two modifications to a record inside a single transaction.
 */
static void
modify(WT_SESSION *session, WT_CURSOR *c_full, WT_CURSOR *c_full_ts, WT_CURSOR *c_mod)
{
    WT_MODIFY entries[MAX_MODIFY_ENTRIES];
    int cnt, loop, nentries;
    char value[VALUE_SIZE];
    const char *v;

    testutil_check(session->begin_transaction(session, "isolation=snapshot"));

    /* Set a read timestamp 90% of the time. */
    if (mmrand(1, 10) != 1) {
        testutil_check(__wt_snprintf(tmp, sizeof(tmp), "read_timestamp=%" PRIx64, ts));
        testutil_check(session->timestamp_transaction(session, tmp));
    }

    /* Generally insert modifies, but occasionally insert a full value */
    if (true || mmrand(1, 15) != 1) {
        /* Up to 4 modify operations, 80% chance for each. */
        for (cnt = loop = 1; loop < 5; ++cnt, ++loop)
            if (mmrand(1, 10) <= 5) {
                modify_build(entries, &nentries, cnt);
                c_mod->set_key(c_mod, key);
                testutil_check(c_mod->modify(c_mod, entries, nentries));
            }
    } else {
        SET_VALUE(key_int, value, 'a' + mmrand(0, 26));
        c_mod->set_key(c_mod, key);
        c_mod->set_value(c_mod, value);
        testutil_check(c_mod->insert(c_mod));
    }

    /* Commit 90% of the time, else rollback. */
    if (mmrand(1, 10) != 1) {
        c_mod->set_key(c_mod, key);
        testutil_check(c_mod->search(c_mod));
        testutil_check(c_mod->get_value(c_mod, &v));

        c_full->set_key(c_full, key);
        c_full->set_value(c_full, v);
        testutil_check(c_full->insert(c_full));

        trace("modify read-ts=%" PRIu64 ", commit-ts=%" PRIu64, ts, ts + 1);
        trace("returned {%s}", v);

        testutil_check(__wt_snprintf(tmp, sizeof(tmp), "commit_timestamp=%" PRIx64, ts + 1));
        testutil_check(session->timestamp_transaction(session, tmp));
        testutil_check(session->commit_transaction(session, NULL));

        /* Insert the record into the table with manual timestamp tracking */
        testutil_check(session->begin_transaction(session, "isolation=snapshot"));
        c_full_ts->set_key(c_full_ts, key, ts + 1);
        c_full_ts->set_value(c_full_ts, v);
        testutil_check(c_full_ts->insert(c_full_ts));
        testutil_check(session->commit_transaction(session, NULL));

    } else
        testutil_check(session->rollback_transaction(session, NULL));

    ++ts;
}

/*
 * repeat --
 *     Reread all previously committed modifications.
 */
static void
repeat(WT_SESSION *session, WT_CURSOR *c_full, WT_CURSOR *c_full_ts, WT_CURSOR *c_mod)
{
    u_int i;
    char *v_full, *v_full_ts, *v_mod;
    const char *found_key, *v;
    int cmp, ret;
    uint64_t new_ts, this_ts;

    new_ts = this_ts = 0;

    for (i = 0;; ++i) {
        /* Start at timestamp 0, and progress using the previous timestamp to search for */
        c_full_ts->set_key(c_full_ts, key, this_ts);
        c_full_ts->search_near(c_full_ts, &cmp);
        c_full_ts->get_key(c_full_ts, &found_key, &new_ts);

        /*
         * Either this is the first search (in which case we want to start at the beginning) 
         * or the search is based off the timestamp checked the previous time, so we want to
         * step forward and retrieve the next timestamp that was used for the key.
         * There is a record for the 0 timestamp which was inserted during the load phase, that
         * record can't be retrieved via a timestamped query, so skip that record.
         */
        testutil_assert(this_ts == 0 || (this_ts == new_ts && cmp == 0));
        while (new_ts == 0 || new_ts == this_ts || new_ts < ts_old || strcmp(found_key, key) != 0) {
            /* We don't expect search near to position us further into the tree */
            testutil_assert(cmp <= 0);
            if ((ret = c_full_ts->next(c_full_ts)) != 0)
                return;
            c_full_ts->get_key(c_full_ts, &found_key, &new_ts);
            if (strcmp(found_key, key) != 0)
                return;
        }
        this_ts = new_ts;
        c_full_ts->get_value(c_full_ts, &v);
        v_full_ts = dstrdup(v);

        testutil_check(session->begin_transaction(session, "isolation=snapshot"));
        testutil_check(__wt_snprintf(tmp, sizeof(tmp), "read_timestamp=%" PRIx64, this_ts));
        testutil_check(session->timestamp_transaction(session, tmp));

        c_mod->set_key(c_mod, key);
        testutil_check(c_mod->search(c_mod));
        testutil_check(c_mod->get_value(c_mod, &v));
        v_mod = dstrdup(v);

        c_full->set_key(c_full, key);
        testutil_check(c_full->search(c_full));
        testutil_check(c_full->get_value(c_full, &v));
        v_full = dstrdup(v);

#if 0
        trace("repeat ts=%" PRIu64, this_ts);
        trace("  expected {%s}", v_full_ts);
        trace(" found mod {%s}", v_mod);
        trace("found full {%s}", v_full);
#endif

        testutil_assert(strcmp(v_mod, v_full) == 0 && strcmp(v_mod, v_full_ts) == 0);

        testutil_check(session->rollback_transaction(session, NULL));
        /*
         * We might not need to copy the values, but I don't want to have to figure
         * out whether a cursor stays positioned across a begin transaction.
         */
        free(v_full);
        free(v_full_ts);
        free(v_mod);
    }
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);
}

/*
 * reset --
 *     Force eviction of the underlying page.
 */
static void
evict(WT_CURSOR *c)
{
    trace("%s", "eviction");

    c->set_key(c, key);
    testutil_check(c->search(c));
    F_SET(c, WT_CURSTD_DEBUG_RESET_EVICT);
    testutil_check(c->reset(c));
    F_CLR(c, WT_CURSTD_DEBUG_RESET_EVICT);
}

/*
 * trace_die --
 *     Dump the trace.
 */
static void
trace_die(void)
{
    u_int i;

    fprintf(stderr, "\n");
    for (i = 0; i < tnext; ++i)
        fprintf(stderr, "%s\n", tlist[i]);
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_CURSOR *c_full, *c_full_ts, *c_mod;
    WT_SESSION *session;
    u_int i, j;
    int ch;
    char path[1024], value[VALUE_SIZE];
    const char *home, *v;
    bool no_checkpoint, no_eviction;

    (void)testutil_set_progname(argv);
    custom_die = trace_die;

    __wt_random_init_seed(NULL, &rnd);
    modify_repl_init();

    no_checkpoint = no_eviction = false;
    home = "WT_TEST.wt6185_modify_ts";
    while ((ch = __wt_getopt(progname, argc, argv, "ceh:S:")) != EOF)
        switch (ch) {
        case 'c':
            no_checkpoint = true;
            break;
        case 'e':
            no_eviction = true;
            break;
        case 'h':
            home = __wt_optarg;
            break;
        case 'S':
            rnd.v = strtoul(__wt_optarg, NULL, 10);
            break;
        default:
            usage();
        }
    argc -= __wt_optind;
    if (argc != 0)
        usage();

    testutil_work_dir_from_path(path, sizeof(path), home);
    testutil_make_work_dir(path);

    /* Load 100 records. */
    testutil_check(wiredtiger_open(path, NULL, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, URI_MOD_TABLE, "key_format=S,value_format=S"));
    testutil_check(session->create(session, URI_FULL_TABLE, "key_format=S,value_format=S"));
    testutil_check(session->create(session, URI_FULL_TS_TABLE, "key_format=Sq,value_format=S"));
    testutil_check(session->open_cursor(session, URI_MOD_TABLE, NULL, NULL, &c_mod));
    testutil_check(session->open_cursor(session, URI_FULL_TABLE, NULL, NULL, &c_full));
    testutil_check(session->open_cursor(session, URI_FULL_TS_TABLE, NULL, NULL, &c_full_ts));
    for (i = 0; i <= 100; ++i) {
        testutil_check(__wt_snprintf(key, sizeof(key), "%010u.key", i));
        SET_VALUE(i, value, '.');
        c_full->set_key(c_full, key);
        c_full_ts->set_key(c_full_ts, key, 0);
        c_mod->set_key(c_mod, key);
        c_full->set_value(c_full, value);
        c_full_ts->set_value(c_full_ts, value);
        c_mod->set_value(c_mod, value);
        testutil_check(c_full->insert(c_full));
        testutil_check(c_full->insert(c_full_ts));
        testutil_check(c_mod->insert(c_mod));
    }

    /* Flush, reopen and verify a record. */
    testutil_check(conn->close(conn, NULL));
    testutil_check(wiredtiger_open(path, NULL, NULL, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, URI_FULL_TABLE, NULL));
    testutil_check(session->create(session, URI_FULL_TS_TABLE, NULL));
    testutil_check(session->create(session, URI_MOD_TABLE, NULL));
    testutil_check(session->open_cursor(session, URI_FULL_TABLE, NULL, NULL, &c_full));
    testutil_check(session->open_cursor(session, URI_FULL_TS_TABLE, NULL, NULL, &c_full_ts));
    testutil_check(session->open_cursor(session, URI_MOD_TABLE, NULL, NULL, &c_mod));
    testutil_check(__wt_snprintf(key, sizeof(key), "%010d.key", KEYNO));
    SET_VALUE(KEYNO, value, '.');
    c_full->set_key(c_full, key);
    c_full->set_key(c_full_ts, key, 0);
    c_mod->set_key(c_mod, key);
    testutil_check(c_full->search(c_full));
    testutil_check(c_full_ts->search(c_full_ts));
    testutil_check(c_mod->search(c_mod));
    testutil_check(c_full->get_value(c_full, &v));
    testutil_assert(strcmp(v, value) == 0);
    testutil_check(c_full_ts->get_value(c_full_ts, &v));
    testutil_assert(strcmp(v, value) == 0);
    testutil_check(c_mod->get_value(c_mod, &v));
    testutil_assert(strcmp(v, value) == 0);

    testutil_check(conn->set_timestamp(conn, "oldest_timestamp=1,stable_timestamp=1"));

    /*
     * Loop doing N operations per loop. Each operation consists of modify operations and re-reading
     * all previous committed transactions, then optional page evictions and checkpoints.
     */
    for (i = 0, ts = 1; i < RUNS; ++i) {
        tnext = 0;
        trace("run %u, seed %" PRIu64, i, rnd.v);

        if (i > 0) {
            ts_old = ts / 2;
            testutil_check(__wt_snprintf(tmp, sizeof(tmp),
                    "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, ts_old, ts_old));
            testutil_check(conn->set_timestamp(conn, tmp));
        }

        for (j = mmrand(10, MAX_OPS); j > 0; --j) {
            key_int = mmrand(0, KEYNO);
            testutil_check(__wt_snprintf(key, sizeof(key), "%010u.key", key_int));
            modify(session, c_full, c_full_ts, c_mod);
            repeat(session, c_full, c_full_ts, c_mod);

            /* Evict the page with modifies on it occasionally. */
            if (!no_eviction && mmrand(1, 10) > 1)
                evict(c_mod);

            /* Checkpoint periodically during the run. */
            if (!no_checkpoint && mmrand(1, MAX_OPS / 5) > 1) {
                trace("%s", "checkpoint");
                testutil_check(session->checkpoint(session, NULL));
            }
        }
        testutil_assert(write(STDOUT_FILENO, ".", 1) == 1);
    }
    testutil_assert(write(STDOUT_FILENO, "\n", 1) == 1);

    testutil_check(conn->close(conn, NULL));

    cleanup();
    return (EXIT_SUCCESS);
}
