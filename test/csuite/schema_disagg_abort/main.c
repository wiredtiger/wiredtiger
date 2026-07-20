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
 * Entry point: parse the command line, then dispatch to the parent, leader, or follower role. See
 * schema_disagg_abort.h for the overall test structure.
 */

#include "schema_disagg_abort.h"

#include "subproc.h"

extern int __wt_optind;
extern char *__wt_optarg;

static void usage(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/*
 * usage --
 *     Print the command-line usage and exit. The -r/-R/-W options are internal: the parent uses
 *     them to spawn its child roles.
 */
static void
usage(void)
{
    fprintf(stderr,
      "usage: %s [-b build-dir] [-h dir] [-k l|f|b] [-m] [-p] [-s pool] [-T threads] [-t time] "
      "[-v]\n",
      progname);
    fprintf(stderr, "%s",
      "\t-b build directory (required for PALite extension)\n"
      "\t-h home directory\n"
      "\t-k multi-node kill target: l=leader f=follower b=both (default: single-node)\n"
      "\t-m switch mode (randomly start as leader or follower, then switch roles mid-run)\n"
      "\t-p preserve directory contents\n"
      "\t-s URI pool size per thread\n"
      "\t-T number of schema threads\n"
      "\t-t timeout in seconds\n"
      "\t-v verify only\n");
    exit(EXIT_FAILURE);
}

/*
 * parse_kill_mode --
 *     Translate the -k option value.
 */
static KILL_MODE
parse_kill_mode(const char *arg)
{
    if (strcmp(arg, "l") == 0)
        return (KILL_LEADER);
    if (strcmp(arg, "f") == 0)
        return (KILL_FOLLOWER);
    if (strcmp(arg, "b") == 0)
        return (KILL_BOTH);
    usage();
    /* NOTREACHED */
}

/*
 * parse_role --
 *     Translate the internal -r option value.
 */
static TEST_ROLE
parse_role(const char *arg)
{
    if (strcmp(arg, "leader") == 0)
        return (ROLE_LEADER);
    if (strcmp(arg, "follower") == 0)
        return (ROLE_FOLLOWER);
    usage();
    /* NOTREACHED */
}

/*
 * parse_uint_in_range --
 *     Parse a numeric option value, enforcing an inclusive range.
 */
static uint32_t
parse_uint_in_range(const char *arg, uint32_t min, uint32_t max, const char *what)
{
    const uint32_t value = (uint32_t)atoi(arg);
    if (value < min || value > max) {
        fprintf(stderr, "%s must be between %" PRIu32 " and %" PRIu32 "\n", what, min, max);
        usage();
    }
    return (value);
}

/*
 * parse_args --
 *     Parse the command line into the configuration and derive the path fields. Reports whether the
 *     thread count and timeout were left to be randomized.
 */
static void
parse_args(TEST_CONFIG *cfg, int argc, char *argv[], bool *rand_thp, bool *rand_timep)
{
    bool pool_size_set = false;

    *rand_thp = *rand_timep = true;

    testutil_parse_begin_opt(argc, argv, "b:h:k:mpP:r:R:s:T:t:vW:", cfg->opts);

    int ch;
    while ((ch = __wt_getopt(progname, argc, argv, "b:h:k:mpP:r:R:s:T:t:vW:")) != EOF)
        switch (ch) {
        case 'k':
            cfg->kill_mode = parse_kill_mode(__wt_optarg);
            break;
        case 'm':
            cfg->switch_mode = true;
            break;
        case 'r':
            cfg->role = parse_role(__wt_optarg);
            break;
        case 'R':
            cfg->pipe_read_fd = atoi(__wt_optarg);
            break;
        case 's':
            pool_size_set = true;
            cfg->pool_size =
              parse_uint_in_range(__wt_optarg, MIN_POOL_SIZE, MAX_POOL_SIZE, "Pool size");
            break;
        case 'T':
            *rand_thp = false;
            cfg->nth = parse_uint_in_range(__wt_optarg, 1, MAX_TH, "Thread count");
            break;
        case 't':
            *rand_timep = false;
            cfg->timeout = (uint32_t)atoi(__wt_optarg);
            break;
        case 'v':
            cfg->verify_only = true;
            break;
        case 'W':
            cfg->pipe_write_fd = atoi(__wt_optarg);
            break;
        default:
            if (testutil_parse_single_opt(cfg->opts, ch) != 0)
                usage();
        }
    if (argc - __wt_optind != 0)
        usage();
    if (cfg->switch_mode && cfg->kill_mode != KILL_NONE) {
        /* The pipe relay semantics across a role switch are not defined yet. */
        fprintf(stderr, "Switch mode (-m) cannot be combined with multi-node (-k)\n");
        exit(EXIT_FAILURE);
    }
    if (cfg->verify_only && *rand_thp) {
        fprintf(stderr, "Verify requires -T\n");
        exit(EXIT_FAILURE);
    }
    if (cfg->verify_only && !pool_size_set) {
        fprintf(stderr, "Verify requires -s\n");
        exit(EXIT_FAILURE);
    }

    cfg->opts->disagg.is_enabled = true;
    testutil_parse_end_opt(cfg->opts);
    testutil_work_dir_from_path(cfg->home, sizeof(cfg->home), cfg->opts->home);

    char cwd[PATH_MAX];
    testutil_assert_errno(getcwd(cwd, sizeof(cwd)) != NULL);
    testutil_snprintf(
      cfg->page_log_home, sizeof(cfg->page_log_home), "%s/%s/%s", cwd, cfg->home, WT_HOME_DIR);
}

/*
 * randomize_run_parameters --
 *     Choose random values for the parameters not fixed on the command line. The data random stream
 *     is consumed unconditionally to keep it in sync between runs with and without -T.
 */
static void
randomize_run_parameters(TEST_CONFIG *cfg, bool rand_th, bool rand_time)
{
    if (rand_time) {
        cfg->timeout = __wt_random(&cfg->opts->extra_rnd) % MAX_TIME;
        if (cfg->timeout < MIN_TIME)
            cfg->timeout = MIN_TIME;
    }

    const uint32_t rand_value = __wt_random(&cfg->opts->data_rnd);
    if (rand_th) {
        cfg->nth = rand_value % MAX_TH;
        if (cfg->nth < MIN_TH)
            cfg->nth = MIN_TH;
    }
}

/*
 * kill_mode_desc --
 *     Return a human-readable name for a kill mode.
 */
static const char *
kill_mode_desc(KILL_MODE mode)
{
    switch (mode) {
    case KILL_NONE:
        return ("none (single-node)");
    case KILL_LEADER:
        return ("leader");
    case KILL_FOLLOWER:
        return ("follower");
    case KILL_BOTH:
        return ("both");
    }
    return (NULL); /* NOTREACHED */
}

/*
 * kill_mode_arg --
 *     Return the command-line fragment reproducing a kill mode.
 */
static const char *
kill_mode_arg(KILL_MODE mode)
{
    switch (mode) {
    case KILL_NONE:
        return ("");
    case KILL_LEADER:
        return (" -k l");
    case KILL_FOLLOWER:
        return (" -k f");
    case KILL_BOTH:
        return (" -k b");
    }
    return (NULL); /* NOTREACHED */
}

/*
 * print_run_banner --
 *     Report the effective run parameters, including the CONFIG line that reproduces the run.
 */
static void
print_run_banner(const TEST_CONFIG *cfg)
{
    printf("Parent: Create %" PRIu32 " schema threads; pool %" PRIu32
           " slots; kill %s; switch %s; sleep "
           "%" PRIu32 " seconds\n",
      cfg->nth, cfg->pool_size, kill_mode_desc(cfg->kill_mode), cfg->switch_mode ? "yes" : "no",
      cfg->timeout);
    printf("CONFIG: %s%s%s -s %" PRIu32 " -T %" PRIu32 " -t %" PRIu32 " " TESTUTIL_SEED_FORMAT "\n",
      progname, kill_mode_arg(cfg->kill_mode), cfg->switch_mode ? " -m" : "", cfg->pool_size,
      cfg->nth, cfg->timeout, cfg->opts->data_seed, cfg->opts->extra_seed);
}

/*
 * main --
 *     Parse arguments and run the requested role.
 */
int
main(int argc, char *argv[])
{
    static TEST_OPTS s_opts;

    (void)testutil_set_progname(argv);

    TEST_CONFIG cfg = {0};
    cfg.opts = &s_opts;
    cfg.nth = MIN_TH;
    cfg.pool_size = MAX_POOL_SIZE / 8; /* Default: 8 slots per thread. */
    cfg.timeout = MIN_TIME;
    cfg.pipe_read_fd = cfg.pipe_write_fd = -1;

    bool rand_th, rand_time;
    parse_args(&cfg, argc, argv, &rand_th, &rand_time);

    /* The child roles get their full configuration from the command line; just run them. */
    if (cfg.role == ROLE_LEADER)
        leader_main(&cfg); /* NOTREACHED */
    if (cfg.role == ROLE_FOLLOWER)
        follower_main(&cfg); /* NOTREACHED */

    if (!cfg.verify_only) {
        randomize_run_parameters(&cfg, rand_th, rand_time);
        print_run_banner(&cfg);
    }

    char self_path[PATH_MAX];
    subproc_self_path(argv[0], self_path, sizeof(self_path));

    parent_main(&cfg, self_path);
    return (EXIT_SUCCESS);
}
