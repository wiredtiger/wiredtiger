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

/*
 * This program tests skip list ordering under concurrent workloads. It copies some of the skip list
 * code from the btree, but links against the WiredTiger library for all of the support functions.
 *
 * This is a quick and dirty test for WT-10461. If we ever decide to make this a standard part of
 * the csuite, we'll need to refactor things so it uses the same code as WT, rather than a copy of
 * the code.
 */

#include <math.h>
#include "test_util.h"

extern int __wt_optind;
extern char *__wt_optarg;

static uint64_t seed = 0;
static uint32_t key_count = 100000;

/* Test parameters. Eventually these should become command line args */

#define INSERT_THREADS 8 /* Number of threads doing inserts */
#define VERIFY_THREADS 2  /* Number of threads doing verify */
#define NTHREADS (INSERT_THREADS + VERIFY_THREADS)

typedef enum { KEYS_NOT_CONFIG, KEYS_ADJACENT, KEYS_PARETO, KEYS_UNIFORM } test_type;

typedef struct {
    WT_CONNECTION *conn;
    WT_INSERT_HEAD *ins_head;
    uint32_t id;
    char **keys;
    uint32_t nkeys;
} THREAD_DATA;

static volatile enum { WAITING, RUNNING, DONE } test_state;

static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * usage --
 *     Print a usage message.
 */
static void
usage(void)
{
    fprintf(stderr, "usage: %s [-adr] [-h dir] [-k key_count] [-S seed]\n", progname);
    fprintf(stderr, "    -a Adjacent keys\n");
    fprintf(stderr, "    -d Pareto distributed random keys\n");
    fprintf(stderr, "    -r Uniform random keys\n");
    fprintf(stderr, "Only one of the -adr options may be used\n");
    exit(EXIT_FAILURE);
}

/*
 * We don't care about the values we store in our mock insert list. So all entries will point to the
 * dummy update. Likewise, the insert code uses the WT page lock when it needs to exclusive access.
 * We're don't have that, so we just set up a single global spinlock that all threads use since
 * they're all operating on the same skiplist.
 */
static WT_UPDATE dummy_update;

static WT_SPINLOCK page_lock;

/*
 * search_insert --
 *     Find the location for an insert into the skip list. Based o __wt_search_insert()
 */
static int
search_insert(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_INSERT_HEAD *ins_head, WT_ITEM *srch_key)
{
    WT_INSERT *ins, **insp, *last_ins;
    WT_ITEM key;
    size_t match, skiphigh, skiplow;
    int cmp, i;

    cmp = 0; /* -Wuninitialized */

    /*
     * The insert list is a skip list: start at the highest skip level, then go as far as possible
     * at each level before stepping down to the next.
     */
    match = skiphigh = skiplow = 0;
    ins = last_ins = NULL;
    for (i = WT_SKIP_MAXDEPTH - 1, insp = &ins_head->head[i]; i >= 0;) {
        if ((ins = *insp) == NULL) {
            cbt->next_stack[i] = NULL;
            cbt->ins_stack[i--] = insp--;
            continue;
        }

        /*
         * Comparisons may be repeated as we drop down skiplist levels; don't repeat comparisons,
         * they might be expensive.
         */
        if (ins != last_ins) {
            last_ins = ins;
            key.data = WT_INSERT_KEY(ins);
            key.size = WT_INSERT_KEY_SIZE(ins);
            match = WT_MIN(skiplow, skiphigh);
            WT_RET(__wt_compare_skip(session, NULL, srch_key, &key, &cmp, &match));
        }

        if (cmp > 0) { /* Keep going at this level */
            insp = &ins->next[i];
            skiplow = match;
        } else if (cmp < 0) { /* Drop down a level */
            cbt->next_stack[i] = ins;
            cbt->ins_stack[i--] = insp--;
            skiphigh = match;
        } else
            for (; i >= 0; i--) {
                cbt->next_stack[i] = ins->next[i];
                cbt->ins_stack[i] = &ins->next[i];
            }
    }

    /*
     * For every insert element we review, we're getting closer to a better choice; update the
     * compare field to its new value. If we went past the last item in the list, return the last
     * one: that is used to decide whether we are positioned in a skiplist.
     */
    cbt->compare = -cmp;
    cbt->ins = (ins != NULL) ? ins : last_ins;
    cbt->ins_head = ins_head;
    return (0);
}

/*
 * insert_simple_func --
 *     Add a WT_INSERT entry to the middle of a skiplist. Copy of __insert_simple_func().
 */
static inline int
insert_simple_func(
  WT_SESSION_IMPL *session, WT_INSERT ***ins_stack, WT_INSERT *new_ins, u_int skipdepth)
{
    u_int i;

    WT_UNUSED(session);

    /*
     * Update the skiplist elements referencing the new WT_INSERT item. If we fail connecting one of
     * the upper levels in the skiplist, return success: the levels we updated are correct and
     * sufficient. Even though we don't get the benefit of the memory we allocated, we can't roll
     * back.
     *
     * All structure setup must be flushed before the structure is entered into the list. We need a
     * write barrier here, our callers depend on it. Don't pass complex arguments to the macro, some
     * implementations read the old value multiple times.
     */
    for (i = 0; i < skipdepth; i++) {
        WT_INSERT *old_ins = *ins_stack[i];
        if (old_ins != new_ins->next[i] || !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
            return (i == 0 ? WT_RESTART : 0);
    }

    return (0);
}

/*
 * insert_serial_func --
 *     Add a WT_INSERT entry to a skiplist. Copy of __insert_serial_func()
 */
static inline int
insert_serial_func(WT_SESSION_IMPL *session, WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack,
  WT_INSERT *new_ins, u_int skipdepth)
{
    u_int i;

    /* The cursor should be positioned. */
    WT_ASSERT(session, ins_stack[0] != NULL);

    /*
     * Update the skiplist elements referencing the new WT_INSERT item.
     *
     * Confirm we are still in the expected position, and no item has been added where our insert
     * belongs. If we fail connecting one of the upper levels in the skiplist, return success: the
     * levels we updated are correct and sufficient. Even though we don't get the benefit of the
     * memory we allocated, we can't roll back.
     *
     * All structure setup must be flushed before the structure is entered into the list. We need a
     * write barrier here, our callers depend on it. Don't pass complex arguments to the macro, some
     * implementations read the old value multiple times.
     */
    for (i = 0; i < skipdepth; i++) {
        WT_INSERT *old_ins = *ins_stack[i];
        if (old_ins != new_ins->next[i] || !__wt_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
            return (i == 0 ? WT_RESTART : 0);
        if (ins_head->tail[i] == NULL || ins_stack[i] == &ins_head->tail[i]->next[i])
            ins_head->tail[i] = new_ins;
    }

    return (0);
}

/*
 * insert_serial --
 *     Top level function for inserting a WT_INSERT into a skiplist. Based on __wt_insert_serial()
 */
static inline int
insert_serial(WT_SESSION_IMPL *session, WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack,
  WT_INSERT **new_insp, u_int skipdepth)
{
    WT_DECL_RET;
    WT_INSERT *new_ins;
    u_int i;
    bool simple;

    /* Clear references to memory we now own and must free on error. */
    new_ins = *new_insp;
    *new_insp = NULL;

    simple = true;
    for (i = 0; i < skipdepth; i++)
        if (new_ins->next[i] == NULL)
            simple = false;

    if (simple)
        ret = insert_simple_func(session, ins_stack, new_ins, skipdepth);
    else {
        __wt_spin_lock(session, &page_lock);
        ret = insert_serial_func(session, ins_head, ins_stack, new_ins, skipdepth);
        __wt_spin_unlock(session, &page_lock);
    }

    if (ret != 0) {
        /* Free unused memory on error. */
        __wt_free(session, new_ins);
        return (ret);
    }

    return (0);
}

/*
 * row_insert --
 *     Our version of the __wt_row_modify() function, with everything stripped out except for the
 *     relevant insert path.
 */
static int
row_insert(WT_CURSOR_BTREE *cbt, const WT_ITEM *key, WT_INSERT_HEAD *ins_head)
{
    WT_DECL_RET;
    WT_INSERT *ins;
    WT_SESSION_IMPL *session;
    size_t ins_size;
    u_int i, skipdepth;

    ins = NULL;
    session = CUR2S(cbt);

    /*
     * Allocate the insert array as necessary.
     *
     * We allocate an additional insert array slot for insert keys sorting less than any key on the
     * page. The test to select that slot is baroque: if the search returned the first page slot, we
     * didn't end up processing an insert list, and the comparison value indicates the search key
     * was smaller than the returned slot, then we're using the smallest-key insert slot. That's
     * hard, so we set a flag.
     */

    /* Choose a skiplist depth for this insert. */
    skipdepth = __wt_skip_choose_depth(session);

    /*
     * Allocate a WT_INSERT/WT_UPDATE pair and transaction ID, and update the cursor to reference it
     * (the WT_INSERT_HEAD might be allocated, the WT_INSERT was allocated).
     */
    WT_ERR(__wt_row_insert_alloc(session, key, skipdepth, &ins, &ins_size));
    cbt->ins_head = ins_head;
    cbt->ins = ins;

    ins->upd = &dummy_update;
    ins_size += WT_UPDATE_SIZE;

    /*
     * If there was no insert list during the search, the cursor's information cannot be correct,
     * search couldn't have initialized it.
     *
     * Otherwise, point the new WT_INSERT item's skiplist to the next elements in the insert list
     * (which we will check are still valid inside the serialization function).
     *
     * The serial mutex acts as our memory barrier to flush these writes before inserting them into
     * the list.
     */
    if (cbt->ins_stack[0] == NULL)
        for (i = 0; i < skipdepth; i++) {
            cbt->ins_stack[i] = &ins_head->head[i];
            ins->next[i] = cbt->next_stack[i] = NULL;
        }
    else
        for (i = 0; i < skipdepth; i++)
            ins->next[i] = cbt->next_stack[i];

    /* Insert the WT_INSERT structure. */
    WT_ERR(insert_serial(session, cbt->ins_head, cbt->ins_stack, &ins, skipdepth));

err:
    return (ret);
}

/*
 * insert --
 *     Test function that inserts a new entry with the given key string into our skiplist.
 */
static int
insert(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_INSERT_HEAD *ins_head, char *key)
{
    WT_ITEM new;

    new.data = key;
    /* Include the terminal nul character in the key for easier printing. */
    new.size = strlen(key) + 1;
    WT_RET(search_insert(session, cbt, ins_head, &new));
    WT_RET(row_insert(cbt, &new, ins_head));

    return (0);
}

/*
 * verify_list --
 *     Walk the skip list and verify that items are in order.
 */
static void
verify_list(WT_SESSION *session, WT_INSERT_HEAD *ins_head)
{
    WT_INSERT *ins;
    WT_ITEM cur;
    WT_ITEM prev;
    int cmp;

    ins = WT_SKIP_FIRST(ins_head);
    if (ins == NULL)
        return;

    prev.data = WT_INSERT_KEY(ins);
    prev.size = WT_INSERT_KEY_SIZE(ins);

    while ((ins = WT_SKIP_NEXT(ins)) != NULL) {
        cur.data = WT_INSERT_KEY(ins);
        cur.size = WT_INSERT_KEY_SIZE(ins);
        testutil_check(__wt_compare((WT_SESSION_IMPL *)session, NULL, &prev, &cur, &cmp));
        if (cmp >= 0) {
            printf("Out of order keys: %s before %s\n", (char *)prev.data, (char *)cur.data);
            testutil_assert(false);
        }
        prev = cur;
    }
}

/*
 * thread_insert_run --
 *     An insert thread. Iterates through the key list and inserts its set of keys to the skiplist.
 */
static WT_THREAD_RET
thread_insert_run(void *arg)
{
    WT_CONNECTION *conn;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;
    WT_INSERT_HEAD *ins_head;
    WT_SESSION *session;
    THREAD_DATA *td;
    char **key_list;
    uint32_t i;

    td = (THREAD_DATA *)arg;
    conn = td->conn;
    ins_head = td->ins_head;
    key_list = td->keys;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Set up state as if we have a btree that is accessing an insert list. */
    cbt = dcalloc(1, sizeof(WT_CURSOR_BTREE));
    ((WT_CURSOR *)cbt)->session = session;

    /* Wait to start */
    while (test_state == WAITING)
        ;

    /* Insert the keys. */
    for (i = 0; i < td->nkeys; i++) {
        while ((ret = insert((WT_SESSION_IMPL *)session, cbt, ins_head, key_list[i]) == WT_RESTART))
            ;
        testutil_assert (ret == 0);
    }

    free(cbt);

    return (WT_THREAD_RET_VALUE);
}

/*
 * thread_verify_run --
 *     A verify thread sits in a loop checking that the skiplist is in order
 */
static WT_THREAD_RET
thread_verify_run(void *arg)
{
    WT_CONNECTION *conn;
    WT_INSERT_HEAD *ins_head;
    WT_SESSION *session;
    THREAD_DATA *td;

    td = (THREAD_DATA *)arg;
    conn = td->conn;
    ins_head = td->ins_head;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /* Wait to start */
    while (test_state == WAITING)
        ;

    /* Keep verifying the skip list until the insert load has finished */
    while (test_state != DONE)
        verify_list(session, ins_head);

    return (WT_THREAD_RET_VALUE);
}

static uint32_t pareto(uint32_t input_val){
    uint32_t r_val;
    double S1, S2, U;

#define PARETO_SHAPE 1.5
    S1 = (-1 / PARETO_SHAPE);
    S2 = key_count * (10 / 100.0) * (PARETO_SHAPE - 1);
    U = 1 - (double)input_val / (double)UINT32_MAX;
    r_val = (uint32_t)((pow(U, S1) - 1) * S2);
    return r_val;
}

/*
 * main --
 *     Test body
 */
int
main(int argc, char *argv[])
{
    static char **key_list;
    WT_CONNECTION *conn;
    WT_INSERT_HEAD *ins_head;
    WT_RAND_STATE rnd;
    WT_SESSION *session;
    char command[1024], home[1024];
    const char *working_dir;
    test_type config;
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t i, idx, j, r_val, thread_keys;
    int ch, status;

    working_dir = "WT_TEST.skip_list_stress";
    config = KEYS_NOT_CONFIG;

    while ((ch = __wt_getopt(progname, argc, argv, "adh:k:rS:")) != EOF)
        switch (ch) {
        case 'a':
            if (config != KEYS_NOT_CONFIG)
                usage();
            config = KEYS_ADJACENT;
            break;
        case 'd':
            if (config != KEYS_NOT_CONFIG)
                usage();
            config = KEYS_PARETO;
            break;
        case 'h':
            working_dir = __wt_optarg;
            break;
        case 'k':
            key_count = (uint32_t) atoll(__wt_optarg);
            break;
        case 'r':
            if (config != KEYS_NOT_CONFIG)
                usage();
            config = KEYS_UNIFORM;
            break;
        case 'S':
            seed = (uint64_t)atoll(__wt_optarg);
            break;
        default:
            usage();
        }
    argc -= __wt_optind;
    if (argc != 0)
        usage();

    if (seed == 0) {
        __wt_random_init_seed(NULL, &rnd);
        seed = rnd.v;
    } else
        rnd.v = seed;

    thread_keys = key_count / INSERT_THREADS;

    /* By default, test with uniform random keys */
    if (config == KEYS_NOT_CONFIG)
        config = KEYS_UNIFORM;

    testutil_work_dir_from_path(home, sizeof(home), working_dir);
    testutil_check(__wt_snprintf(command, sizeof(command), "rm -rf %s; mkdir %s", home, home));
    if ((status = system(command)) < 0)
        testutil_die(status, "system: %s", command);

    testutil_check(wiredtiger_open(home, NULL, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(__wt_spin_init((WT_SESSION_IMPL *)session, &page_lock, "fake page lock"));
    ins_head = dcalloc(1, sizeof(WT_INSERT_HEAD));

    /*
     * Generate the keys. Each insert thread will operate on a separate part of the key_list array.
     * N.B., the key strings here are stored in the skip list. So we need a separate buffer for
     * each key.
     */
    key_list = dmalloc(key_count * sizeof(char *));
    switch (config) {
        case KEYS_NOT_CONFIG:
            usage();
            /* NOT REACHED */

        case KEYS_ADJACENT:
            /* 
             * Pairs of threads operate in the same region of key space, one inserting keys from
             * low to high while the other insers keys from high to low. The goal is to generate
             * pairs of inserts that are adjacent in the skip list. We should get this behavior as
             * each thread's current insert should be adjacent to its partner thread's current 
             * insert, as we haven't yet added any keys between those two.
             */

            /* Even numbered threads get increasing keys */
            for (i = 0; i < INSERT_THREADS; i += 2)
                for (j = 0, idx = i * thread_keys; j < thread_keys; j++, idx++) {
                    key_list[idx] = dmalloc(20);
                    sprintf(key_list[idx], "Key #%c.%06d", 'A' + i, j);
                }

            /* Odd numbered threads get decreasing keys */
            for (i = 1; i < INSERT_THREADS; i += 2)
                for (j = 0, idx = i * thread_keys; j < thread_keys; j++, idx++) {
                    key_list[idx] = dmalloc(20);
                    sprintf(key_list[idx], "Key %c.%06d", 'A' + i - 1, (2 * thread_keys - j));
                }
            break;

        case KEYS_PARETO:
            /* FALLTHROUGH */
        case KEYS_UNIFORM:
            key_list = dmalloc(key_count * sizeof(char *));
            for (i = 0; i < key_count; i++) {
                key_list[i] = dmalloc(20);
                r_val = __wt_random(&rnd);
                if (config == KEYS_PARETO) {
                    r_val = pareto(r_val);
                    r_val = r_val % key_count;
                }
                sprintf(key_list[i], "%u.%d", r_val, i);
            }
            break;
    }

    /* Set up threads */
    td = dmalloc(NTHREADS * sizeof(THREAD_DATA));
    for (i = 0; i < NTHREADS; i++) {
        td[i].conn = conn;
        td[i].id = i;
        td[i].ins_head = ins_head;
        td[i].keys = key_list + i * thread_keys;
        td[i].nkeys = thread_keys;
    }

    thr = dmalloc(NTHREADS * sizeof(wt_thread_t));

    /* Start threads */
    test_state = WAITING;
    for (i = 0; i < NTHREADS; i++)
        if (i < INSERT_THREADS)
            testutil_check(__wt_thread_create(NULL, &thr[i], thread_insert_run, &td[i]));
        else
            testutil_check(__wt_thread_create(NULL, &thr[i], thread_verify_run, &td[i]));

    test_state = RUNNING;

    /* Wait for insert threads to complete */
    for (i = 0; i < INSERT_THREADS; i++)
        testutil_check(__wt_thread_join(NULL, &thr[i]));

    /* Tell verify threads to stop */
    test_state = DONE;
    for (i = INSERT_THREADS; i < NTHREADS; i++)
        testutil_check(__wt_thread_join(NULL, &thr[i]));

    /* Final verification of skiplist */
    verify_list(session, ins_head);

    printf("Success.\n");
    testutil_clean_test_artifacts(home);
    testutil_clean_work_dir(home);

    return (EXIT_SUCCESS);
}
