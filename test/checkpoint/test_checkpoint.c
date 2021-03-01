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

#include "test_checkpoint.h"

GLOBAL g;

static int handle_error(WT_EVENT_HANDLER *, WT_SESSION *, int, const char *);
static int handle_message(WT_EVENT_HANDLER *, WT_SESSION *, const char *);
static void onint(int) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void cleanup(bool);
static int usage(void);
static int wt_connect(const char *);
static int wt_shutdown(void);

extern int __wt_optind;
extern char *__wt_optarg;

int
main(int argc, char *argv[])
{
    table_type ttype;
    int ch, cnt, ret, runs;
    char *working_dir;
    const char *config_open;

    (void)testutil_set_progname(argv);

    config_open = NULL;
    ret = 0;
    working_dir = NULL;
    ttype = MIX;
    g.checkpoint_name = "WiredTigerCheckpoint";
    g.debug_mode = false;
    g.home = dmalloc(512);
    g.nkeys = 10000;
    g.nops = 100000;
    g.ntables = 3;
    g.nworkers = 1;
    g.sweep_stress = g.use_timestamps = false;
    runs = 1;

    while ((ch = __wt_getopt(progname, argc, argv, "C:c:Dh:k:l:n:pr:sT:t:W:x")) != EOF)
        switch (ch) {
        case 'c':
            g.checkpoint_name = __wt_optarg;
            break;
        case 'C': /* wiredtiger_open config */
            config_open = __wt_optarg;
            break;
        case 'D':
            g.debug_mode = true;
            break;
        case 'h': /* wiredtiger_open config */
            working_dir = __wt_optarg;
            break;
        case 'k': /* rows */
            g.nkeys = (u_int)atoi(__wt_optarg);
            break;
        case 'l': /* log */
            if ((g.logfp = fopen(__wt_optarg, "w")) == NULL) {
                fprintf(stderr, "%s: %s\n", __wt_optarg, strerror(errno));
                return (EXIT_FAILURE);
            }
            break;
        case 'n': /* operations */
            g.nops = (u_int)atoi(__wt_optarg);
            break;
        case 'p': /* prepare */
            g.prepare = true;
            break;
        case 'r': /* runs */
            runs = atoi(__wt_optarg);
            break;
        case 's':
            g.sweep_stress = true;
            break;
        case 't':
            switch (__wt_optarg[0]) {
            case 'c':
                ttype = COL;
                break;
            case 'l':
                ttype = LSM;
                break;
            case 'm':
                ttype = MIX;
                break;
            case 'r':
                ttype = ROW;
                break;
            default:
                return (usage());
            }
            break;
        case 'T':
            g.ntables = atoi(__wt_optarg);
            break;
        case 'W':
            g.nworkers = atoi(__wt_optarg);
            break;
        case 'x':
            g.use_timestamps = true;
            break;
        default:
            return (usage());
        }

    argc -= __wt_optind;
    if (argc != 0)
        return (usage());

    /* Clean up on signal. */
    (void)signal(SIGINT, onint);

    testutil_work_dir_from_path(g.home, 512, working_dir);

    printf("%s: process %" PRIu64 "\n", progname, (uint64_t)getpid());
    for (cnt = 1; (runs == 0 || cnt <= runs) && g.status == 0; ++cnt) {
        cleanup(cnt == 1); /* Clean up previous runs */

        printf("    %d: %d workers, %d tables\n", cnt, g.nworkers, g.ntables);

        /* Setup a fresh set of cookies in the global array. */
        if ((g.cookies = calloc((size_t)(g.ntables), sizeof(COOKIE))) == NULL) {
            (void)log_print_err("No memory", ENOMEM, 1);
            break;
        }

        g.running = 1;

        if ((ret = wt_connect(config_open)) != 0) {
            (void)log_print_err("Connection failed", ret, 1);
            break;
        }

        start_checkpoints();
        if ((ret = start_workers(ttype)) != 0) {
            (void)log_print_err("Start workers failed", ret, 1);
            break;
        }

        g.running = 0;
        end_checkpoints();

        free(g.cookies);
        g.cookies = NULL;
        if ((ret = wt_shutdown()) != 0) {
            (void)log_print_err("Start workers failed", ret, 1);
            break;
        }
    }
    if (g.logfp != NULL)
        (void)fclose(g.logfp);

    /* Ensure that cleanup is done on error. */
    (void)wt_shutdown();
    free(g.cookies);
    return (g.status);
}

#define DEBUG_MODE_CFG ",debug_mode=(eviction=true,table_logging=true)"
/*
 * wt_connect --
 *     Configure the WiredTiger connection.
 */
static int
wt_connect(const char *config_open)
{
    static WT_EVENT_HANDLER event_handler = {
      handle_error, handle_message, NULL, NULL /* Close handler. */
    };
    int ret;
    char config[512];

    /*
     * If we want to stress sweep, we have a lot of additional configuration settings to set.
     */
    if (g.sweep_stress)
        testutil_check(__wt_snprintf(config, sizeof(config),
          "create,cache_cursors=false,statistics=(fast),statistics_log=(json,wait=1),error_prefix="
          "\"%s\",file_manager=(close_handle_minimum=1,close_idle_time=1,close_scan_interval=1),"
          "log=(enabled),cache_size=1GB,timing_stress_for_test=(aggressive_sweep)%s%s%s",
          progname, g.debug_mode ? DEBUG_MODE_CFG : "", config_open == NULL ? "" : ",",
          config_open == NULL ? "" : config_open));
    else
        testutil_check(__wt_snprintf(config, sizeof(config),
          "create,cache_cursors=false,statistics=(fast),statistics_log=(json,wait=1),error_prefix="
          "\"%s\"%s%s%s",
          progname, g.debug_mode ? DEBUG_MODE_CFG : "", config_open == NULL ? "" : ",",
          config_open == NULL ? "" : config_open));

    if ((ret = wiredtiger_open(g.home, &event_handler, config, &g.conn)) != 0)
        return (log_print_err("wiredtiger_open", ret, 1));
    return (0);
}

/*
 * wt_shutdown --
 *     Shut down the WiredTiger connection.
 */
static int
wt_shutdown(void)
{
    int ret;

    if (g.conn == NULL)
        return (0);

    printf("Closing connection\n");
    ret = g.conn->close(g.conn, NULL);
    g.conn = NULL;
    if (ret != 0)
        return (log_print_err("conn.close", ret, 1));
    return (0);
}

/*
 * cleanup --
 *     Clean up from previous runs.
 */
static void
cleanup(bool remove_dir)
{
    g.running = 0;
    g.ntables_created = 0;
    g.ts_oldest = 0;
    g.ts_stable = 0;

    if (remove_dir)
        testutil_make_work_dir(g.home);
}

static int
handle_error(WT_EVENT_HANDLER *handler, WT_SESSION *session, int error, const char *errmsg)
{
    WT_UNUSED(handler);
    WT_UNUSED(session);
    WT_UNUSED(error);

    return (fprintf(stderr, "%s\n", errmsg) < 0 ? -1 : 0);
}

static int
handle_message(WT_EVENT_HANDLER *handler, WT_SESSION *session, const char *message)
{
    WT_UNUSED(handler);
    WT_UNUSED(session);

    if (g.logfp != NULL)
        return (fprintf(g.logfp, "%s\n", message) < 0 ? -1 : 0);

    return (printf("%s\n", message) < 0 ? -1 : 0);
}

/*
 * onint --
 *     Interrupt signal handler.
 */
static void
onint(int signo)
{
    WT_UNUSED(signo);

    cleanup(false);

    fprintf(stderr, "\n");
    exit(EXIT_FAILURE);
}

/*
 * log_print_err --
 *     Report an error and return the error.
 */
int
log_print_err(const char *m, int e, int fatal)
{
    if (fatal) {
        g.running = 0;
        g.status = e;
    }
    fprintf(stderr, "%s: %s: %s\n", progname, m, wiredtiger_strerror(e));
    if (g.logfp != NULL)
        fprintf(g.logfp, "%s: %s: %s\n", progname, m, wiredtiger_strerror(e));
    return (e);
}

/*
 * path_setup --
 *     Build the standard paths and shell commands we use.
 */
const char *
type_to_string(table_type type)
{
    if (type == COL)
        return ("COL");
    if (type == LSM)
        return ("LSM");
    if (type == ROW)
        return ("ROW");
    if (type == MIX)
        return ("MIX");
    return ("INVALID");
}

/*
 * usage --
 *     Display usage statement and exit failure.
 */
static int
usage(void)
{
    fprintf(stderr,
      "usage: %s [-C wiredtiger-config] [-c checkpoint] [-h home] [-k keys]\n\t[-l log] [-n ops] "
      "[-r runs] [-T table-config] [-t f|r|v]\n\t[-W workers]\n",
      progname);
    fprintf(stderr, "%s",
      "\t-C specify wiredtiger_open configuration arguments\n"
      "\t-c checkpoint name to used named checkpoints\n"
      "\t-h set a database home directory\n"
      "\t-k set number of keys to load\n"
      "\t-l specify a log file\n"
      "\t-n set number of operations each thread does\n"
      "\t-p use prepare\n"
      "\t-r set number of runs (0 for continuous)\n"
      "\t-T specify a table configuration\n"
      "\t-t set a file type ( col | mix | row | lsm )\n"
      "\t-W set number of worker threads\n"
      "\t-x use timestamps\n");
    return (EXIT_FAILURE);
}
