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

#include "schema_disagg_abort.h"

/*
 * Disaggregated schema epoch crash recovery test — multi-node variant.
 *
 * A leader child runs schema worker threads that create and drop layered tables.
 * A separate follower child reads schema events from a shared pipe and periodically
 * picks up the leader's checkpoints via the page log.  The parent kills one or both
 * children at a random point (-k flag chooses the target).  After recovery, both the
 * leader and follower WiredTiger homes are verified: tables whose creation epoch was
 * captured in the last checkpoint must still exist with correct data.
 *
 * Thread layout inside the leader child:
 *   N schema worker threads — produce SCHEMA_OP_CREATE / SCHEMA_OP_DROP events
 *   1 checkpoint thread     — produces SCHEMA_OP_CKPT events
 *   1 timestamp thread      — keeps stable advancing for precise_checkpoint
 *   1 oplog writer thread   — single consumer: writes events to record files and pipe
 */

extern int __wt_optind;
extern char *__wt_optarg;

/* Global definitions (declared extern in schema_disagg_abort.h). */
char home[1024];
char page_log_home[PATH_MAX];
int schema_pipe[2] = {-1, -1};

bool aggressive_sweep;
volatile bool stable_set;
uint32_t nth;
uint64_t schema_op_epoch;

SCHEMA_QUEUE schema_queue;
pthread_mutex_t schema_publish_lock;

KILL_MODE kill_mode = KILL_LEADER;

/* Phase 2 (role-switch): globals set in main, read by workload.c. */
bool role_switch;
uint32_t run_timeout;

static TEST_OPTS _opts;
TEST_OPTS *opts;

const char *const ready_file = "child_ready";
const char *const follower_ready_file = "follower_ready";
const char *const follower_stepped_up_file = "follower_stepped_up";

static void sig_handler(int) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

static void
usage(void)
{
    fprintf(stderr,
      "usage: %s [-h dir] [-k l|f|b] [-S] [-T threads] [-t time] [-p] [-v] [-w]\n", progname);
    fprintf(stderr, "%s",
      "\t-h home directory\n"
      "\t-k kill target: l=leader f=follower b=both (default: l)\n"
      "\t-p preserve directory contents\n"
      "\t-S aggressive sweep\n"
      "\t-T number of schema threads\n"
      "\t-t timeout in seconds\n"
      "\t-v verify only\n"
      "\t-w role-switch mode: leader closes/reopens as follower/leader mid-run\n");
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
 *     Parse arguments, fork leader and follower children, kill them after the
 *     timeout, then open both homes for recovery and verify schema/data state.
 */
int
main(int argc, char *argv[])
{
    struct sigaction sa;
    WT_CONNECTION *conn;
    pid_t follower_pid, leader_pid;
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
    role_switch = false;
    kill_mode = KILL_LEADER;
    follower_pid = leader_pid = 0;

    testutil_parse_begin_opt(argc, argv, "h:pT:v", opts);

    while ((ch = __wt_getopt(progname, argc, argv, "h:k:pP:ST:t:vw")) != EOF)
        switch (ch) {
        case 'k':
            if (strcmp(__wt_optarg, "l") == 0)
                kill_mode = KILL_LEADER;
            else if (strcmp(__wt_optarg, "f") == 0)
                kill_mode = KILL_FOLLOWER;
            else if (strcmp(__wt_optarg, "b") == 0)
                kill_mode = KILL_BOTH;
            else
                usage();
            break;
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
        case 'w':
            /* Phase 2 (role-switch): single-node mode, no follower process. */
            role_switch = true;
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

    /* Always disagg — set before parse_end_opt so build_dir is auto-detected. */
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
        testutil_snprintf(buf, sizeof(buf), "%s/%s", home, FOLLOWER_HOME_DIR);
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

        /* Phase 2 (role-switch): publish timeout globally so workload.c can drive the timer. */
        run_timeout = timeout;

        printf("Parent: Create %" PRIu32 " schema threads; kill=%s%s; sleep %" PRIu32
               " seconds\n",
          nth,
          role_switch ? "leader (role-switch)" :
            kill_mode == KILL_LEADER ? "leader" :
            kill_mode == KILL_FOLLOWER ? "follower" : "both",
          role_switch ? "" : "",
          timeout);
        printf("CONFIG: %s%s%s -T %" PRIu32 " -t %" PRIu32 " " TESTUTIL_SEED_FORMAT "\n", progname,
          aggressive_sweep ? " -S" : "", role_switch ? " -w" : "", nth, timeout,
          opts->data_seed, opts->extra_seed);

        testutil_snprintf(page_log_home, sizeof(page_log_home), "%s/%s/%s", cwd_start, home,
          WT_HOME_DIR);

        testutil_assert_errno(pipe(schema_pipe) == 0);

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig_handler;
        testutil_assert_errno(sigaction(SIGCHLD, &sa, NULL) == 0);

        testutil_assert_errno((leader_pid = fork()) >= 0);
        if (leader_pid == 0) {
            run_workload();
            /* NOTREACHED */
        }

        if (!role_switch) {
            /*
             * Phase 3: close the write end in the parent so that when the leader
             * dies the follower sees EOF on the pipe.
             */
            close(schema_pipe[1]);
            schema_pipe[1] = -1;

            testutil_assert_errno((follower_pid = fork()) >= 0);
            if (follower_pid == 0) {
                run_follower(schema_pipe[0]);
                /* NOTREACHED */
            }

            close(schema_pipe[0]);
            schema_pipe[0] = -1;
        } else {
            /* Phase 2 (role-switch): single-node, no follower process; close unused pipe ends. */
            close(schema_pipe[0]);
            close(schema_pipe[1]);
            schema_pipe[0] = schema_pipe[1] = -1;
        }

        while (!testutil_exists(home, ready_file))
            testutil_sleep_wait(1, leader_pid);

        if (!role_switch) {
            /* Phase 3: wait for follower to pick up its first checkpoint. */
            while (!testutil_exists(home, follower_ready_file))
                testutil_sleep_wait(1, follower_pid);
        }

        sleep(timeout);

        sa.sa_handler = SIG_DFL;
        testutil_assert_errno(sigaction(SIGCHLD, &sa, NULL) == 0);

        if (role_switch) {
            /*
             * Phase 2 (role-switch): only the leader child exists.  Kill it
             * after the full timeout; the role-switch already happened inside
             * the child during phase A/B.
             */
            printf("Kill: leader (role-switch mode)\n");
            testutil_assert_errno(kill(leader_pid, SIGKILL) == 0);
            testutil_assert_errno(waitpid(leader_pid, &status, 0) != -1);
            leader_pid = 0;
        } else {
            /*
             * Phase 3: multi-node kill strategy.
             */
            printf("Kill: %s\n",
              kill_mode == KILL_LEADER ? "leader" :
                kill_mode == KILL_FOLLOWER ? "follower" : "both");

            if (kill_mode == KILL_LEADER) {
                /*
                 * Kill the leader only.  The follower detects pipe EOF, steps up,
                 * and writes follower_stepped_up.  Give it time to do so, then let
                 * it run briefly as the new leader before killing it.
                 */
                testutil_assert_errno(kill(leader_pid, SIGKILL) == 0);
                testutil_assert_errno(waitpid(leader_pid, &status, 0) != -1);
                leader_pid = 0;

                printf("Waiting for follower to step up...\n");
                fflush(stdout);
                {
                    uint32_t waited = 0;
                    while (!testutil_exists(home, follower_stepped_up_file) &&
                      kill(follower_pid, 0) == 0 && waited < (uint32_t)MAX_STARTUP) {
                        __wt_sleep(1, 0);
                        ++waited;
                    }
                }
                if (testutil_exists(home, follower_stepped_up_file)) {
                    uint32_t step_up_run = timeout / 4 + 1;
                    printf(
                      "Follower stepped up; running %" PRIu32 " more seconds\n", step_up_run);
                    fflush(stdout);
                    sleep(step_up_run);
                }
                testutil_assert_errno(kill(follower_pid, SIGKILL) == 0);
            } else {
                if (kill_mode == KILL_FOLLOWER || kill_mode == KILL_BOTH)
                    testutil_assert_errno(kill(follower_pid, SIGKILL) == 0);
                testutil_assert_errno(kill(leader_pid, SIGKILL) == 0);
            }

            if (leader_pid > 0)
                testutil_assert_errno(waitpid(leader_pid, &status, 0) != -1);
            testutil_assert_errno(waitpid(follower_pid, &status, 0) != -1);
        }
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
    fatal = verify_schema_state(conn, SCHEMA_RECORDS_FILE);
    testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));

    if (!role_switch) {
        /*
         * Phase 3: verify the follower home.  Open as leader for recovery so
         * the page log checkpoint is picked up automatically, which populates
         * last_disaggregated_schema_epoch.
         */
        printf("Open follower database, run recovery and verify content\n");

        opts->disagg.mode = "leader";
        opts->disagg.page_log_home = page_log_home;

        testutil_wiredtiger_open(opts, FOLLOWER_HOME_DIR,
          "create,disaggregated=(lose_all_my_data=true)", NULL, &conn, true, false);
        if (verify_schema_state(conn, FOLLOWER_RECORDS_FILE))
            fatal = true;
        testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));
    }

    if (chdir(cwd_start) != 0)
        testutil_die(errno, "root chdir: %s", home);

    if (!fatal && !opts->preserve)
        testutil_remove(home);

    testutil_cleanup(opts);
    return (fatal ? EXIT_FAILURE : EXIT_SUCCESS);
}
