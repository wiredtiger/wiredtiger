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

#include "schema_disagg_leader_abort.h"

/*
 * Disaggregated schema epoch crash recovery test.
 *
 * A leader child runs schema worker threads that create and drop layered tables. A checkpoint
 * thread advances stable_disaggregated_schema_epoch and checkpoints periodically. The parent
 * kills the child after at least one checkpoint completes. After recovery, the verifier confirms
 * that tables whose creation was captured in the last checkpoint still exist with correct data,
 * and tables whose drop was captured are absent.
 */

extern int __wt_optind;
extern char *__wt_optarg;

/* Global definitions. */
char home[1024];
char page_log_home[PATH_MAX];

bool aggressive_sweep;
volatile bool stable_set;
uint32_t nth;
uint64_t schema_op_epoch;

pthread_mutex_t schema_publish_lock;

static TEST_OPTS _opts;
TEST_OPTS *opts;

const char *const ready_file = "child_ready";

static void sig_handler(int) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

static void
usage(void)
{
    fprintf(stderr, "usage: %s [-h dir] [-S] [-T threads] [-t time] [-p] [-v]\n", progname);
    fprintf(stderr, "%s",
      "\t-h home directory\n"
      "\t-p preserve directory contents\n"
      "\t-S aggressive sweep\n"
      "\t-T number of schema threads\n"
      "\t-t timeout in seconds\n"
      "\t-v verify only\n");
    exit(EXIT_FAILURE);
}

static void
sig_handler(int sig)
{
    pid_t pid;

    WT_UNUSED(sig);
    pid = wait(NULL);
    testutil_die(EINVAL, "Child process %" PRIu64 " abnormally exited", (uint64_t)pid);
}

/*
 * main --
 *     Parse arguments, fork the leader child, kill it after the timeout, then open the database
 *     for recovery and verify schema and data state.
 */
int
main(int argc, char *argv[])
{
    struct sigaction sa;
    WT_CONNECTION *conn;
    pid_t child_pid;
    uint32_t rand_value, timeout;
    int ch, status;
    char buf[PATH_MAX];
    char cwd_start[PATH_MAX];
    bool fatal, rand_th, rand_time, verify_only;

    (void)testutil_set_progname(argv);

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));

    aggressive_sweep = false;
    nth = MIN_TH;
    rand_th = rand_time = true;
    timeout = MIN_TIME;
    verify_only = false;

    testutil_parse_begin_opt(argc, argv, "h:pT:v", opts);

    while ((ch = __wt_getopt(progname, argc, argv, "h:pST:t:v")) != EOF)
        switch (ch) {
        case 'S':
            aggressive_sweep = true;
            break;
        case 'T':
            rand_th = false;
            nth = (uint32_t)atoi(__wt_optarg);
            break;
        case 't':
            rand_time = false;
            timeout = (uint32_t)atoi(__wt_optarg);
            break;
        case 'v':
            verify_only = true;
            break;
        default:
            if (testutil_parse_single_opt(opts, ch) != 0)
                usage();
        }
    argc -= __wt_optind;
    if (argc != 0)
        usage();
    if (verify_only && rand_th) {
        fprintf(stderr, "Verify requires -T\n");
        exit(EXIT_FAILURE);
    }

    opts->disagg.is_enabled = true;
    testutil_parse_end_opt(opts);
    testutil_work_dir_from_path(home, sizeof(home), opts->home);
    testutil_assert_errno(getcwd(cwd_start, sizeof(cwd_start)) != NULL);

    if (!verify_only) {
        testutil_recreate_dir(home);
        testutil_snprintf(buf, sizeof(buf), "%s/%s", home, RECORDS_DIR);
        testutil_mkdir(buf);
        testutil_snprintf(buf, sizeof(buf), "%s/%s", home, WT_HOME_DIR);
        testutil_mkdir(buf);

        if (rand_time) {
            timeout = __wt_random(&opts->extra_rnd) % MAX_TIME;
            if (timeout < MIN_TIME)
                timeout = MIN_TIME;
        }

        rand_value = __wt_random(&opts->data_rnd);
        if (rand_th) {
            nth = rand_value % MAX_TH;
            if (nth < MIN_TH)
                nth = MIN_TH;
        }

        printf("Parent: Create %" PRIu32 " schema threads; sleep %" PRIu32 " seconds\n",
          nth, timeout);
        printf("CONFIG: %s%s -T %" PRIu32 " -t %" PRIu32 " " TESTUTIL_SEED_FORMAT "\n",
          progname, aggressive_sweep ? " -S" : "", nth, timeout,
          opts->data_seed, opts->extra_seed);

        testutil_snprintf(page_log_home, sizeof(page_log_home), "%s/%s/%s", cwd_start, home,
          WT_HOME_DIR);

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig_handler;
        testutil_assert_errno(sigaction(SIGCHLD, &sa, NULL) == 0);

        testutil_assert_errno((child_pid = fork()) >= 0);
        if (child_pid == 0) {
            run_workload();
            /* NOTREACHED */
        }

        while (!testutil_exists(home, ready_file))
            testutil_sleep_wait(1, child_pid);

        sleep(timeout);

        sa.sa_handler = SIG_DFL;
        testutil_assert_errno(sigaction(SIGCHLD, &sa, NULL) == 0);

        testutil_assert_errno(kill(child_pid, SIGKILL) == 0);
        testutil_assert_errno(waitpid(child_pid, &status, 0) != -1);
    }

    if (chdir(home) != 0)
        testutil_die(errno, "parent chdir: %s", home);

    if (!verify_only)
        testutil_copy_data();

    if (page_log_home[0] == '\0')
        testutil_snprintf(page_log_home, sizeof(page_log_home), "%s/%s/%s", cwd_start, home,
          WT_HOME_DIR);

    printf("Open leader database, run recovery and verify content\n");

    opts->disagg.is_enabled = true;
    opts->disagg.mode = "leader";
    opts->disagg.page_log = "palite";
    opts->disagg.page_log_home = page_log_home;
    opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(opts, WT_HOME_DIR,
      "create,disaggregated=(lose_all_my_data=true)", NULL, &conn, true, false);
    fatal = verify_schema_state(conn);
    testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));

    if (chdir(cwd_start) != 0)
        testutil_die(errno, "root chdir: %s", home);

    if (!fatal && !opts->preserve)
        testutil_remove(home);

    testutil_cleanup(opts);
    return (fatal ? EXIT_FAILURE : EXIT_SUCCESS);
}
