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
 * Parent role: orchestrator and verifier. Spawns the leader (and, in multi-node mode, the
 * follower), monitors their health during the timed run, kills them per the configured mode with
 * strict exit-status checks, then reopens the surviving state and verifies it against the record
 * files. The parent never opens WiredTiger while children are running.
 */

#include "schema_disagg_abort.h"

#include <signal.h>

#include "subproc.h"

static void die_on_child_status(const SUBPROC *proc, SUBPROC_STATUS status, int code)
  WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * create_test_dirs --
 *     Create the directory structure needed for a fresh test run.
 */
static void
create_test_dirs(const TEST_CONFIG *cfg)
{
    char buf[PATH_MAX];

    testutil_recreate_dir(cfg->home);
    testutil_snprintf(buf, sizeof(buf), "%s/%s", cfg->home, RECORDS_DIR);
    testutil_mkdir(buf);
    testutil_snprintf(buf, sizeof(buf), "%s/%s", cfg->home, WT_HOME_DIR);
    testutil_mkdir(buf);
    if (cfg->kill_mode != KILL_NONE) {
        testutil_snprintf(buf, sizeof(buf), "%s/%s", cfg->home, FOLLOWER_HOME_DIR);
        testutil_mkdir(buf);
    }
}

/*
 * kill_mode_option --
 *     Return the -k option value for a kill mode.
 */
static const char *
kill_mode_option(KILL_MODE mode)
{
    switch (mode) {
    case KILL_LEADER:
        return ("l");
    case KILL_FOLLOWER:
        return ("f");
    case KILL_BOTH:
        return ("b");
    case KILL_NONE:
        break;
    }
    return (NULL);
}

/*
 * spawn_role --
 *     Spawn this binary again in a child role, passing the full configuration on the command line.
 *     The role name doubles as the child's diagnostic name.
 */
static void
spawn_role(const TEST_CONFIG *cfg, const char *self_path, const char *role, SUBPROC *proc)
{
    char nth_arg[16], pool_arg[16], rfd_arg[16], wfd_arg[16], seed_arg[80];
    testutil_snprintf(nth_arg, sizeof(nth_arg), "%" PRIu32, cfg->nth);
    testutil_snprintf(pool_arg, sizeof(pool_arg), "%" PRIu32, cfg->pool_size);
    testutil_snprintf(rfd_arg, sizeof(rfd_arg), "%d", cfg->pipe_read_fd);
    testutil_snprintf(wfd_arg, sizeof(wfd_arg), "%d", cfg->pipe_write_fd);
    testutil_snprintf(seed_arg, sizeof(seed_arg), TESTUTIL_SEED_FORMAT, cfg->opts->data_seed,
      cfg->opts->extra_seed);

    const char *argv[24];
    size_t n = 0;
    argv[n++] = self_path;
    argv[n++] = "-r";
    argv[n++] = role;
    argv[n++] = "-h";
    argv[n++] = cfg->opts->home;
    if (cfg->opts->build_dir != NULL) {
        argv[n++] = "-b";
        argv[n++] = cfg->opts->build_dir;
    }
    argv[n++] = "-T";
    argv[n++] = nth_arg;
    argv[n++] = "-s";
    argv[n++] = pool_arg;
    if (cfg->switch_mode)
        argv[n++] = "-m";
    if (cfg->kill_mode != KILL_NONE) {
        argv[n++] = "-k";
        argv[n++] = kill_mode_option(cfg->kill_mode);
        argv[n++] = "-R";
        argv[n++] = rfd_arg;
        argv[n++] = "-W";
        argv[n++] = wfd_arg;
    }
    argv[n++] = seed_arg;
    argv[n] = NULL;

    subproc_spawn(proc, role, self_path, (char *const *)argv);
}

/*
 * die_on_child_status --
 *     Fail the test, reporting how a child terminated.
 */
static void
die_on_child_status(const SUBPROC *proc, SUBPROC_STATUS status, int code)
{
    testutil_die(EINVAL, "%s terminated unexpectedly: %s %d", proc->who,
      status == SUBPROC_EXITED ? "exit code" : "signal", code);
}

/*
 * wait_for_sentinel --
 *     Wait up to the given number of seconds for a child to create a sentinel file, failing if the
 *     child dies or times out first.
 */
static void
wait_for_sentinel(const TEST_CONFIG *cfg, SUBPROC *proc, const char *sentinel, uint32_t max_wait)
{
    for (uint32_t waited = 0; !testutil_exists(cfg->home, sentinel); ++waited) {
        int code;
        const SUBPROC_STATUS status = subproc_poll(proc, &code);
        if (status != SUBPROC_RUNNING)
            die_on_child_status(proc, status, code);
        if (waited >= max_wait)
            testutil_die(ETIMEDOUT, "%s did not create %s within %" PRIu32 " seconds", proc->who,
              sentinel, max_wait);
        sleep(1);
    }
}

/*
 * timed_run --
 *     Let the workload run for the configured timeout, failing fast if a child dies early. Slots
 *     that were never spawned (NULL who) are skipped.
 */
static void
timed_run(SUBPROC children[SUBPROC_SLOTS], uint32_t timeout)
{
    for (uint32_t elapsed = 0; elapsed < timeout; ++elapsed) {
        for (size_t i = 0; i < SUBPROC_SLOTS; ++i) {
            SUBPROC *const proc = &children[i];

            if (proc->who == NULL)
                continue;

            int code;
            const SUBPROC_STATUS status = subproc_poll(proc, &code);
            if (status != SUBPROC_RUNNING)
                die_on_child_status(proc, status, code);
        }
        sleep(1);
    }
}

/*
 * reap_killed --
 *     Reap a killed child and assert it died from our signal, not on its own.
 */
static void
reap_killed(SUBPROC *proc)
{
    int code;
    const SUBPROC_STATUS status = subproc_wait(proc, &code);
    if (status != SUBPROC_KILLED || code != SIGKILL)
        die_on_child_status(proc, status, code);
}

/*
 * kill_and_reap --
 *     Kill a child abruptly and assert it died from our signal.
 */
static void
kill_and_reap(SUBPROC *proc)
{
    subproc_kill(proc);
    reap_killed(proc);
}

/*
 * reap_stepped_up_follower --
 *     Wait for the follower to step up and exit on its own, then assert it exited cleanly and left
 *     the stepped-up sentinel.
 */
static void
reap_stepped_up_follower(const TEST_CONFIG *cfg, SUBPROC *proc)
{
    int code;
    SUBPROC_STATUS status;

    for (uint32_t waited = 0; (status = subproc_poll(proc, &code)) == SUBPROC_RUNNING; ++waited) {
        if (waited >= MAX_STARTUP) {
            subproc_kill(proc);
            (void)subproc_wait(proc, &code);
            testutil_die(ETIMEDOUT, "%s did not step up within %d seconds", proc->who, MAX_STARTUP);
        }
        sleep(1);
    }

    if (status != SUBPROC_EXITED || code != EXIT_SUCCESS)
        die_on_child_status(proc, status, code);
    testutil_assert(testutil_exists(cfg->home, FOLLOWER_STEPPED_UP_FILE));
}

/*
 * run_and_kill_children --
 *     Run the crash scenario: spawn the children, let them work, then kill per the configured mode.
 */
static void
run_and_kill_children(TEST_CONFIG *cfg, const char *self_path)
{
    const bool multi_node = cfg->kill_mode != KILL_NONE;

    SUBPROC children[SUBPROC_SLOTS];
    WT_CLEAR(children);
    SUBPROC *const leader = &children[SUBPROC_LEADER];
    SUBPROC *const follower = &children[SUBPROC_FOLLOWER];

    if (multi_node) {
        int fds[2];
        subproc_pipe(fds);
        cfg->pipe_read_fd = fds[0];
        cfg->pipe_write_fd = fds[1];
    }

    spawn_role(cfg, self_path, "leader", leader);

    if (multi_node) {
        spawn_role(cfg, self_path, "follower", follower);

        /* The children own the pipe now; the follower must see EOF once the leader dies. */
        close(cfg->pipe_read_fd);
        close(cfg->pipe_write_fd);
        cfg->pipe_read_fd = cfg->pipe_write_fd = -1;
    }

    /*
     * Wait until the crash window has opened before starting the timer. In switch mode the crash
     * must land in phase 2, so wait for the switch sentinel. Otherwise wait for the leader's ready
     * sentinel, which follows its first checkpoint. Both can run long under heavy schema churn
     * (many workers, large URI pools): give them a much wider window than the follower, whose first
     * pickup follows promptly once a checkpoint exists.
     */
    wait_for_sentinel(
      cfg, leader, cfg->switch_mode ? SWITCH_DONE_FILE : LEADER_READY_FILE, 4 * MAX_STARTUP);
    if (multi_node)
        wait_for_sentinel(cfg, follower, FOLLOWER_READY_FILE, MAX_STARTUP);

    timed_run(children, cfg->timeout);

    printf("Kill: %s\n", multi_node ? kill_mode_option(cfg->kill_mode) : "leader (single-node)");
    fflush(stdout);

    switch (cfg->kill_mode) {
    case KILL_NONE:
        kill_and_reap(leader);
        break;
    case KILL_LEADER:
        /* The pipe EOF makes the follower step up, checkpoint, and exit on its own. */
        kill_and_reap(leader);
        reap_stepped_up_follower(cfg, follower);
        break;
    case KILL_FOLLOWER:
        kill_and_reap(follower);
        kill_and_reap(leader);
        break;
    case KILL_BOTH:
        /* Kill both before reaping either, so the deaths overlap as much as possible. */
        subproc_kill(follower);
        subproc_kill(leader);
        reap_killed(follower);
        reap_killed(leader);
        break;
    }
}

/*
 * open_for_recovery --
 *     Open the given home as a disaggregated leader to trigger recovery.
 */
static void
open_for_recovery(const TEST_CONFIG *cfg, const char *home_dir, WT_CONNECTION **connp)
{
    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.mode = "leader";
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(cfg->opts, home_dir, "create,disaggregated=(lose_all_my_data=true)",
      NULL, connp, true, false);
}

/*
 * verify_homes --
 *     Reopen and verify the surviving state: the leader home always, the follower home in
 *     multi-node mode.
 */
static void
verify_homes(const TEST_CONFIG *cfg)
{
    printf("Open leader database, run recovery and verify content\n");

    WT_CONNECTION *conn;
    open_for_recovery(cfg, WT_HOME_DIR, &conn);
    verify_schema_state(conn, cfg, SCHEMA_RECORDS_BASE);
    testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));

    if (cfg->kill_mode != KILL_NONE) {
        /*
         * In kill-leader mode the follower's own records are complete up to its stepped-up
         * checkpoint. In the other modes the reopened follower converges to the leader's last
         * complete checkpoint, so the leader's records are the sound reference.
         */
        printf("Open follower database, run recovery and verify content\n");

        open_for_recovery(cfg, FOLLOWER_HOME_DIR, &conn);
        verify_schema_state(
          conn, cfg, cfg->kill_mode == KILL_LEADER ? FOLLOWER_RECORDS_BASE : SCHEMA_RECORDS_BASE);
        testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));
    }
}

/*
 * parent_main --
 *     Parent role entry point: run the crash scenario, then verify the outcome. Any failure aborts
 *     the process, so returning at all means success.
 */
void
parent_main(TEST_CONFIG *cfg, const char *self_path)
{
    char cwd_start[PATH_MAX];
    testutil_assert_errno(getcwd(cwd_start, sizeof(cwd_start)) != NULL);

    if (!cfg->verify_only) {
        create_test_dirs(cfg);
        run_and_kill_children(cfg, self_path);
    }

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "parent chdir: %s", cfg->home);

    if (!cfg->verify_only)
        testutil_copy_data();

    if (cfg->page_log_home[0] == '\0')
        testutil_snprintf(cfg->page_log_home, sizeof(cfg->page_log_home), "%s/%s/%s", cwd_start,
          cfg->home, WT_HOME_DIR);

    verify_homes(cfg);

    if (chdir(cwd_start) != 0)
        testutil_die(errno, "root chdir: %s", cfg->home);

    if (!cfg->opts->preserve)
        testutil_remove(cfg->home);

    testutil_cleanup(cfg->opts);
}
