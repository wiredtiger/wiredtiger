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
#include "test_util.h"

/*
 * Unit test testutil_parse_opts and testutil_parse_opt functions.
 */

extern char *__wt_optarg; /* option argument */
extern int __wt_optind;   /* argv position, needed to reset __wt_getopt parser */
extern int __wt_optreset; /* needed to reset the parser internal state */

/*
 * This structure aids in testing testutil_parse_opt. That function is useful for test applications
 * that wish to extend/modify the basic option set provided by testutil.
 */
typedef struct {
    char *c_option;
    bool d_option;
    bool e_option;
    int f_option;
} EXTENDED_OPTS;

/*
 * This drives the testing. For each given command_line, there is a matching expected TEST_OPTS
 * structure. When argv[0] is "parse_opts", we are driving testutil_parse_opts. If argv[0] is
 * "parse_opt", then we are parsing some of our "own" options, put into an EXTENDED_OPTS struct, and
 * using testutil_parse_opt to parse any we don't recognize, those are put into TEST_OPTS.
 */
static const char *command_line0[] = {"parse_opts", "-b", "builddir", "-T", "21", NULL};
static const char *command_line1[] = {"parse_opts", "-bbuilddir", "-T", "21", NULL};
static const char *command_line2[] = {"parse_opts", "-v", "-PT", NULL};
static const char *command_line3[] = {"parse_opts", "-v", "-Po", "my_store", "-PT", NULL};
static const char *command_line4[] = {"parse_opts", "-v", "-Pomy_store", "-PT", NULL};

static const char *command_line5[] = {"parse_opt", "-vd", "-Pomy_store",
  "-c"
  "string_opt",
  "-PT", NULL};
static const char *command_line6[] = {
  "parse_opt", "-dv", "-Pomy_store", "-cstring_opt", "-PT", NULL};
static const char *command_line7[] = {
  "parse_opt", "-ev", "-cstring_opt", "-Pomy_store", "-PT", "-f", "22", NULL};
static const char *command_line8[] = {"parse_opt", "-evd", "-Pomy_store", "-PT", "-f22", NULL};
static const char *command_line9[] = {"parse_opt", "-v", "-Pomy_store", "-PT", NULL};

static const char **command_lines[] = {
  command_line0,
  command_line1,
  command_line2,
  command_line3,
  command_line4,
  command_line5,
  command_line6,
  command_line7,
  command_line8,
  command_line9,
};

static TEST_OPTS expected[] = {
  {NULL, NULL, {0}, NULL, (char *)"builddir", NULL, 0, NULL, NULL, false, false, false, false, 0, 0,
    21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, (char *)"builddir", NULL, 0, NULL, NULL, false, false, false, false, 0, 0,
    21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  /* If -PT is used, the tiered_storage source is set to dir_store, even if -Po is not used. */
  {NULL, NULL, {0}, NULL, NULL, (char *)"dir_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},

  /* Extended tests */
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
  {NULL, NULL, {0}, NULL, NULL, (char *)"my_store", 0, NULL, NULL, false, false, true, true, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
};

static EXTENDED_OPTS x_expected[] = {
  {NULL, 0, 0, 0},
  {NULL, 0, 0, 0},
  {NULL, 0, 0, 0},
  {NULL, 0, 0, 0},
  {NULL, 0, 0, 0},

  {(char *)"string_opt", true, false, 0},
  {(char *)"string_opt", true, false, 0},
  {(char *)"string_opt", false, true, 22},
  {NULL, true, true, 22},
  {NULL, false, false, 0},
};

/*
 * report --
 *     Show any changed fields in the options.
 */
static void
report(const TEST_OPTS *opts, EXTENDED_OPTS *x_opts)
{
#define REPORT_INT(o, field) \
    if (o->field != 0)       \
    printf(#field ": %" PRIu64 "\n", (uint64_t)o->field)
#define REPORT_STR(o, field) \
    if (o->field != NULL)    \
    printf(#field ": %s\n", o->field)

    REPORT_STR(opts, home);
    REPORT_STR(opts, build_dir);
    REPORT_STR(opts, tiered_storage_source);
    REPORT_INT(opts, table_type);
    REPORT_INT(opts, do_data_ops);
    REPORT_INT(opts, preserve);
    REPORT_INT(opts, tiered_storage);
    REPORT_INT(opts, verbose);
    REPORT_INT(opts, nrecords);
    REPORT_INT(opts, nops);
    REPORT_INT(opts, nthreads);
    REPORT_INT(opts, n_append_threads);
    REPORT_INT(opts, n_read_threads);
    REPORT_INT(opts, n_write_threads);
    REPORT_STR(x_opts, c_option);
    REPORT_INT(x_opts, d_option);
    REPORT_INT(x_opts, e_option);
    REPORT_INT(x_opts, f_option);
}

/*
 * check --
 *     Call testutil_parse_opts and return options.
 */
static void
check(int argc, char *const *argv, TEST_OPTS *opts, EXTENDED_OPTS *x_opts)
{
    int ch;
    const char *prog;
    const char *x_usage = " [-c string] [-d] [-e] [-f int]";

    memset(opts, 0, sizeof(*opts));
    memset(x_opts, 0, sizeof(*x_opts));

    /* This may be called multiple times, so reset the __wt_getopt parser. */
    __wt_optind = 1;
    __wt_optreset = 1;

    prog = argv[0];
    if (strchr(prog, '/') != NULL)
        prog = strchr(prog, '/') + 1;

    if (strcmp(prog, "parse_opts") == 0) {
        /* Regular test of testutil_parse_opts, using only the options that it provides. */

        testutil_check(testutil_parse_opts(argc, argv, opts));
    } else {
        /*
         * Test of extended parsing, in which we'll parse some options that we know about and rely
         * on testutil_parse_opt to cover the options it knows about.
         */
        testutil_assert(strcmp(prog, "parse_opt") == 0);

        /*
         * For this part of the testing, we're extending the list of options we're parsing, and
         * using testutil_parse_opt to parse a subset of the standard options.
         */
        testutil_parse_opt_begin(argc, argv, "b:P:T:v", opts);
        while ((ch = __wt_getopt(opts->progname, argc, argv, "b:c:def:P:T:v")) != EOF)
            switch (ch) {
            case 'c':
                x_opts->c_option = __wt_optarg;
                break;
            case 'd':
                x_opts->d_option = true;
                break;
            case 'e':
                x_opts->e_option = true;
                break;
            case 'f':
                x_opts->f_option = atoi(__wt_optarg);
                break;
            default:
                if (testutil_parse_opt(opts, ch) != 0) {
                    (void)fprintf(stderr, "usage: %s%s%s\n", opts->progname, x_usage, opts->usage);
                    testutil_assert(false);
                }
            }
        testutil_parse_opt_end(opts);
    }
}

/*
 * verify_expect --
 *     Verify the returned options against the expected options.
 */
static void
verify_expect(TEST_OPTS *opts, EXTENDED_OPTS *x_opts, TEST_OPTS *expect, EXTENDED_OPTS *x_expect)
{
#define VERIFY_INT(o, e, field)         \
    if (o->field != 0 || e->field != 0) \
    testutil_assert(o->field == e->field)
#define VERIFY_STR(o, e, field)                               \
    do {                                                      \
        if (o->field != NULL || e->field != NULL) {           \
            testutil_assert(o->field != NULL);                \
            testutil_assert(e->field != NULL);                \
            testutil_assert(strcmp(o->field, e->field) == 0); \
        }                                                     \
    } while (0)

    /*
     * opts->home is always set, even without -h on the command line, so don't check it here.
     */
    VERIFY_STR(opts, expect, build_dir);
    VERIFY_STR(opts, expect, tiered_storage_source);
    VERIFY_INT(opts, expect, table_type);
    VERIFY_INT(opts, expect, do_data_ops);
    VERIFY_INT(opts, expect, preserve);
    VERIFY_INT(opts, expect, tiered_storage);
    VERIFY_INT(opts, expect, verbose);
    VERIFY_INT(opts, expect, nrecords);
    VERIFY_INT(opts, expect, nops);
    VERIFY_INT(opts, expect, nthreads);
    VERIFY_INT(opts, expect, n_append_threads);
    VERIFY_INT(opts, expect, n_read_threads);
    VERIFY_INT(opts, expect, n_write_threads);

    VERIFY_STR(x_opts, x_expect, c_option);
    VERIFY_INT(x_opts, x_expect, d_option);
    VERIFY_INT(x_opts, x_expect, e_option);
    VERIFY_INT(x_opts, x_expect, f_option);
}

/*
 * cleanup --
 *     Clean up allocated resources.
 */
static void
cleanup(TEST_OPTS *opts, EXTENDED_OPTS *x_opts)
{
    (void)x_opts; /* Nothing to clean up here. */

    testutil_cleanup(opts);
}

/*
 * main --
 *     Unit test for test utility functions.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *expect, opts;
    EXTENDED_OPTS *x_expect, x_opts;
    size_t i;
    int nargs;
    char *const *cmd;

    if (argc > 1) {
        /*
         * If first arg is --parse_opt(s), then make argv[0] point to "parse_opt" or "parse_opts".
         */
        if (strncmp(argv[1], "--parse_opt", 11) == 0) {
            argc--;
            argv++;
            argv[0] += 2; /* skip past -- */
        }
        check(argc, argv, &opts, &x_opts);
        report(&opts, &x_opts);
        cleanup(&opts, &x_opts);
    } else {
        testutil_assert(WT_ELEMENTS(command_lines) == WT_ELEMENTS(expected));
        for (i = 0; i < WT_ELEMENTS(command_lines); i++) {
            cmd = (char *const *)command_lines[i];
            for (nargs = 0; cmd[nargs] != NULL; nargs++)
                ;
            expect = &expected[i];
            x_expect = &x_expected[i];
            check(nargs, cmd, &opts, &x_opts);
            verify_expect(&opts, &x_opts, expect, x_expect);
            cleanup(&opts, &x_opts);
        }
    }

    exit(0);
}
