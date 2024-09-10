#include <math.h>

#include "test_util.h"

/*
 * main --
 *     Initialize the library and if it determines a tick -> nsec ratio, test that ratio looks
 *     reasonable.
 */
int
main(int argc, char *argv[])
{
    static double NSEC_PER_SEC = 1.e9;

    /* Maximum number of attempts to find the smallest error. */
    static const int ATTEMPT_MAX = 3;

    /* Maximum acceptable error as a fraction of measurement. */
    static double ERR_TOLERANCE = 0.001;

    TEST_OPTS *opts, _opts;
    uint64_t tick_start, tick_stop;
    double elapsed_nsec, diff_nsec, err, min_err;
    int i;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    testutil_assert(__wt_library_init() == 0);

    if (__wt_process.use_epochtime) {
        if (opts->verbose)
            printf("tsc -> nsec not support on this platform.");
        goto exit_test;
    }

    if (opts->verbose)
        printf("nsec/tick ratio = %.6g\n", __wt_process.tsc_nsec_ratio);

    min_err = DBL_MAX;
    for (i = 0; i < ATTEMPT_MAX; i++) {
        tick_start = __wt_rdtsc();
        __wt_sleep(1, 0);
        tick_stop = __wt_rdtsc();

        elapsed_nsec = __wt_clock_to_nsec(tick_stop, tick_start);

        diff_nsec = (double)elapsed_nsec - NSEC_PER_SEC;
        err = fabs(diff_nsec / NSEC_PER_SEC);

        if (opts->verbose)
            printf("attempt=%d  period(ns)=%8g  actual(ns)=%8g  diff(ns)=%8g  error(ns)=%8g\n",
              i + 1, NSEC_PER_SEC, elapsed_nsec, diff_nsec, err);

        min_err = WT_MIN(err, min_err);
    }

    testutil_assert(min_err < ERR_TOLERANCE);

exit_test:
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
