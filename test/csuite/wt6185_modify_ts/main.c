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

#define OPS 20
#define ROW 50
#define MAX_MODIFY_ENTRIES 5
#define RUNS 100

static struct { /* List of repeatable operations. */
    uint64_t ts;
    char *v;
} list[OPS];
static u_int lnext;

static char *tlist[1000]; /* List of traced operations. */
static u_int tnext;

static uint64_t ts; /* Current timestamp. */

static char key[100], modify_repl[256], tmp[4 * 1024];

/*
 * usage --
 *     Print usage message and exit.
 */
static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
usage(void)
{
    fprintf(stderr, "usage: %s [-ce] [-h home]\n", progname);
    exit(EXIT_FAILURE);
}

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
 * mmrand --
 *     Return a random value between a min/max pair, inclusive.
 */
static inline uint32_t
mmrand(WT_SESSION *session, u_int min, u_int max)
{
    uint32_t v;
    u_int range;

    /*
     * Test runs with small row counts can easily pass a max of 0 (for example, "g.rows / 20").
     * Avoid the problem.
     */
    if (max <= min)
        return (min);

    v = __wt_random(&((WT_SESSION_IMPL *)session)->rnd);
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

/*
 * modify_build --
 *     Generate a set of modify vectors.
 */
static void
modify_build(WT_SESSION *session, WT_MODIFY *entries, int *nentriesp)
{
    int i, nentries;

    /* Randomly select a number of byte changes, offsets and lengths. */
    nentries = (int)mmrand(session, 1, MAX_MODIFY_ENTRIES);
    for (i = 0; i < nentries; ++i) {
        entries[i].data.data = modify_repl + mmrand(session, 1, sizeof(modify_repl) - 10);
        entries[i].data.size = (size_t)mmrand(session, 0, 10);
        /*
         * Start at least 11 bytes into the buffer so we skip leading key information.
         */
        entries[i].offset = (size_t)mmrand(session, 20, 40);
        entries[i].size = (size_t)mmrand(session, 0, 10);
    }

    *nentriesp = (int)nentries;
}

/*
 * modify --
 *     Make two modifications to a record inside a single transaction.
 */
static void
modify(WT_SESSION *session, WT_CURSOR *c)
{
    WT_MODIFY entries[MAX_MODIFY_ENTRIES];
    int nentries;
    const char *v;

    testutil_check(session->begin_transaction(session, "isolation=snapshot"));
    testutil_check(__wt_snprintf(tmp, sizeof(tmp), "read_timestamp=%" PRIx64, ts));
    testutil_check(session->timestamp_transaction(session, tmp));

    modify_build(session, entries, &nentries);
    c->set_key(c, key);
    testutil_check(c->modify(c, entries, nentries));

    modify_build(session, entries, &nentries);
    c->set_key(c, key);
    testutil_check(c->modify(c, entries, nentries));

    if (mmrand(session, 1, 10) > 1) {
        c->set_key(c, key);
        testutil_check(c->search(c));
        testutil_check(c->get_value(c, &v));
        free(list[lnext].v);
        list[lnext].v = dstrdup(v);

        trace("modify read-ts=%" PRIu64 ", commit-ts=%" PRIu64, ts, ts + 1);
        trace("returned {%s}", v);

        testutil_check(__wt_snprintf(tmp, sizeof(tmp), "commit_timestamp=%" PRIx64, ts + 1));
        testutil_check(session->timestamp_transaction(session, tmp));
        testutil_check(session->commit_transaction(session, NULL));

        list[lnext].ts = ts + 1; /* Reread at commit timestamp */
        ++lnext;
    } else
        testutil_check(session->rollback_transaction(session, NULL));

    ++ts;
}

/*
 * repeat --
 *     Reread all previously committed modifications.
 */
static void
repeat(WT_SESSION *session, WT_CURSOR *c)
{
    u_int i;
    const char *v;

    for (i = 0; i < lnext; ++i) {
        testutil_check(session->begin_transaction(session, "isolation=snapshot"));
        testutil_check(__wt_snprintf(tmp, sizeof(tmp), "read_timestamp=%" PRIx64, list[i].ts));
        testutil_check(session->timestamp_transaction(session, tmp));

        c->set_key(c, key);
        testutil_check(c->search(c));
        testutil_check(c->get_value(c, &v));

        trace("repeat=ts=%" PRIu64, list[i].ts);
        trace("expected {%s}", v);
        trace("   found {%s}", list[i].v);

        testutil_assert(strcmp(v, list[i].v) == 0);

        testutil_check(session->rollback_transaction(session, NULL));
    }
}

/*
 * reset --
 *     Reset the cursor, evicting the underlying page.
 */
static void
evict(WT_CURSOR *c)
{
    WT_CURSOR_BTREE *cbt;

    trace("%s", "eviction");
    c->set_key(c, key);
    testutil_check(c->search(c));

    cbt = (WT_CURSOR_BTREE *)c;
    cbt->xx = 1;
    testutil_check(c->reset(c));
    cbt->xx = 0;
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

#define SET_VALUE(key, value)                                                           \
    do {                                                                                \
        char *__p;                                                                      \
        memset(value, '.', sizeof(value));                                              \
        value[sizeof(value) - 1] = '\0';                                                \
        testutil_check(__wt_snprintf(value, sizeof(value), "%010u.value", (u_int)key)); \
        for (__p = value; *__p != '\0'; ++__p)                                          \
            ;                                                                           \
        *__p = '.';                                                                     \
    } while (0)

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_CURSOR *c;
    WT_SESSION *session;
    u_int i, j;
    int ch;
    char path[1024], value[60];
    const char *home, *v;
    bool no_checkpoint, no_eviction;

    (void)testutil_set_progname(argv);
    custom_die = trace_die;

    no_checkpoint = no_eviction = false;
    home = "WT_TEST.wt6185_modify_ts";
    while ((ch = __wt_getopt(progname, argc, argv, "ceh:")) != EOF)
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
    testutil_check(session->create(session, "file:xxx", "key_format=S,value_format=S"));
    testutil_check(session->open_cursor(session, "file:xxx", NULL, NULL, &c));
    for (i = 0; i <= 100; ++i) {
        testutil_check(__wt_snprintf(key, sizeof(key), "%010u.key", i));
        c->set_key(c, key);
        SET_VALUE(i, value);
        c->set_value(c, value);
        testutil_check(c->insert(c));
    }

    /* Flush, reopen and verify a record. */
    testutil_check(conn->close(conn, NULL));
    testutil_check(wiredtiger_open(path, NULL, NULL, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, "file:xxx", NULL));
    testutil_check(session->open_cursor(session, "file:xxx", NULL, NULL, &c));
    testutil_check(__wt_snprintf(key, sizeof(key), "%010d.key", ROW));
    c->set_key(c, key);
    testutil_check(c->search(c));
    testutil_check(c->get_value(c, &v));
    SET_VALUE(ROW, value);
    testutil_assert(strcmp(v, value) == 0);

    /* Setup */
    modify_repl_init();
    testutil_check(conn->set_timestamp(conn, "oldest_timestamp=1"));

    /*
     * Loop doing N operations per loop. Each operation consists of two modify operations and then
     * re-reading all previous committed transactions, then an optional eviction and checkpoint.
     */
    for (i = 0, ts = 1; i < RUNS; ++i) {
        lnext = tnext = 0;
        trace("run %u", i);

        for (j = 0; j < OPS; ++j) {
            modify(session, c);
            repeat(session, c);
            if (!no_eviction && mmrand(session, 1, 10) > 8)
                evict(c);
            if (!no_checkpoint && mmrand(session, 1, 10) > 8) {
                trace("%s", "checkpoint");
                testutil_check(session->checkpoint(session, NULL));
            }
        }
        testutil_assert(write(STDIN_FILENO, ".", 1) == 1);
    }
    testutil_assert(write(STDIN_FILENO, "\n", 1) == 1);

    testutil_check(conn->close(conn, NULL));
    return (EXIT_SUCCESS);
}
