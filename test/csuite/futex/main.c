#include "test_util.h"

#include "wt_internal.h"

#define TIME_MS(duration) (((time_t)1000) * (duration))

struct wakeup {
    int ret;                /* Wait return code. */
    int cap_errno;          /* Value of errno on return from wait. */
    WT_FUTEX_WORD wake_val; /* Futex value on return from wait. */
};

struct waiter {
    pthread_t tid;
    WT_FUTEX_WORD *ftx_word;
    WT_FUTEX_WORD expected; /* Expected parameter value for __wt_futex_op_wait(). */
    time_t timeout;         /* Timeout in microseconds. */
    struct wakeup wakeup;   /* Context captured upon wakeup. */
};

struct waiters_outcomes {
    int awoken;   /* Awoken. */
    int spurious; /* Spuriously awoken: must be less
                   * than or equal to awoken. */
    int timedout; /* Timed out. */
    int failed;   /* Wait error other than time out. */
};

#define WAITER_TEST_ASSERT(waiter_, ret_, errno_, val_)           \
    do {                                                          \
        testutil_assert((waiter_)->wakeup.ret == (ret_));         \
        testutil_assert((waiter_)->wakeup.cap_errno == (errno_)); \
        testutil_assert((waiter_)->wakeup.wake_val == (val_));    \
    } while (0)

#define VERBOSE_ANNOUNCE(opts)                    \
    do {                                          \
        if (opts->verbose)                        \
            printf("test_futex: %s\n", __func__); \
    } while (0)

/*
 * waiter_init --
 *     Initialize waiter thread.
 */
static void
waiter_init(struct waiter *w, WT_FUTEX_WORD *ftx_word, WT_FUTEX_WORD expected, time_t timeout)
{
    memset(w, 0, sizeof(*w));
    w->ftx_word = ftx_word;
    w->expected = expected;
    w->timeout = timeout;
}

/*
 * wait_on_futex --
 *     Thread function that waits on futex.
 */
static void *
wait_on_futex(void *arg)
{
    struct waiter *wctx;
    WT_FUTEX_WORD val;
    int ret;

    wctx = arg;
    do {
        errno = 0;
        val = wctx->expected;
        ret = __wt_futex_wait(wctx->ftx_word, wctx->expected, wctx->timeout, &val);
    } while (ret == -1 && (errno == EAGAIN || errno == EINTR));

    wctx->wakeup.ret = ret;
    wctx->wakeup.wake_val = val;
    wctx->wakeup.cap_errno = errno;
    return NULL;
}

/*
 * waiters_start --
 *     Start up a thread for each entry in waiters.
 */
static void
waiters_start(struct waiter *waiters, int waiter_count)
{
    int i;
    for (i = 0; i < waiter_count; i++)
        testutil_check(pthread_create(&waiters[i].tid, NULL, wait_on_futex, &waiters[i]));
}

/*
 * waiters_join --
 *     Wait for all waiter threads to terminate.
 */
static void
waiters_join(struct waiter *waiters, int waiter_count)
{
    int i;
    for (i = 0; i < waiter_count; i++)
        testutil_check(pthread_join(waiters[i].tid, NULL));
}

/*
 * collect_outcomes --
 *     Summarize outcomes for multiple waiter threads.
 */
static void
collect_outcomes(struct waiter *waiters, int waiter_count, WT_FUTEX_WORD futex_wake_val,
  struct waiters_outcomes *outcomes)
{
    struct waiter *w;
    int i;

    memset(outcomes, 0, sizeof(*outcomes));
    for (i = 0; i < waiter_count; i++) {
        w = &waiters[i];
        if (w->wakeup.ret == 0) {
            outcomes->awoken++;
            if (w->wakeup.wake_val != futex_wake_val)
                outcomes->spurious++;
        } else if (w->wakeup.cap_errno == ETIMEDOUT)
            outcomes->timedout++;
        else
            outcomes->failed++;
    }
}

/*
 * check_outcomes --
 *     Spurious wakeups are passed through in this API, so outcome validation must account for valid
 *     variations.
 */
static void
check_outcomes(const struct waiters_outcomes *outcomes, int max_awoken, int max_timedout)
{
    int outcome_total, expected_total;

    /* This may change if a unaligned futex address test is added. */
    testutil_assert(outcomes->failed == 0);

    testutil_assert(outcomes->awoken <= max_awoken);
    testutil_assert(outcomes->timedout <= max_timedout);
    testutil_assert(outcomes->spurious <= (max_awoken + max_timedout));

    outcome_total = outcomes->awoken + outcomes->timedout + outcomes->spurious;
    expected_total = max_awoken + max_timedout;
    testutil_assert(outcome_total == expected_total);
}

/*
 * test_wake_up_single --
 *     Wake all threads, with only a single thread waiting on the futex.
 */
static void
test_wake_up_single(TEST_OPTS *opts)
{
    struct waiter waiter;
    WT_FUTEX_WORD futex;

    VERBOSE_ANNOUNCE(opts);

    futex = 0;
    waiter_init(&waiter, &futex, futex, TIME_MS(200));
    testutil_check(pthread_create(&waiter.tid, NULL, wait_on_futex, &waiter));
    __wt_sleep(0, TIME_MS(50));
    testutil_check(__wt_futex_wake(&futex, WT_FUTEX_WAKE_ONE, 0x1f2e3c4d));
    testutil_check(pthread_join(waiter.tid, NULL));

    WAITER_TEST_ASSERT(&waiter, 0, 0, futex);
}

/*
 * test_time_out_single --
 *     Test timeout for a single thread waiting on the futex.
 */
static void
test_time_out_single(TEST_OPTS *opts)
{
    struct waiter waiter;
    WT_FUTEX_WORD futex;

    VERBOSE_ANNOUNCE(opts);

    futex = 0;
    waiter_init(&waiter, &futex, futex, TIME_MS(200));
    testutil_check(pthread_create(&waiter.tid, NULL, wait_on_futex, &waiter));
    __wt_sleep(0, TIME_MS(50));
    __atomic_store_n(&futex, 0x1f2e3c4d, __ATOMIC_RELEASE);
    testutil_check(pthread_join(waiter.tid, NULL));

    WAITER_TEST_ASSERT(&waiter, -1, ETIMEDOUT, 0);
}

/*
 * test_spurious_wake_up_single --
 *     Simulate a spurious wakeup by calling wake on the futex even though the value has not
 *     changed.
 */
static void
test_spurious_wake_up_single(TEST_OPTS *opts)
{
    struct waiter waiter;
    WT_FUTEX_WORD futex;

    VERBOSE_ANNOUNCE(opts);

    futex = 911;
    waiter_init(&waiter, &futex, futex, TIME_MS(200));
    testutil_check(pthread_create(&waiter.tid, NULL, wait_on_futex, &waiter));
    __wt_sleep(0, TIME_MS(50));
    testutil_check(__wt_futex_wake(&futex, WT_FUTEX_WAKE_ONE, futex));
    testutil_check(pthread_join(waiter.tid, NULL));

    WAITER_TEST_ASSERT(&waiter, 0, 0, futex);
}

/*
 * test_wake_one_of_two --
 *     Only 1 of the 2 threads should wake.
 */
static void
test_wake_one_of_two(TEST_OPTS *opts)
{
    static const int WAKEUP_VAL = 1;

    struct waiters_outcomes outcomes;
#define WAITER_COUNT 2
    struct waiter waiters[WAITER_COUNT];
    WT_FUTEX_WORD futex;
    int i;

    VERBOSE_ANNOUNCE(opts);

    futex = 0;
    for (i = 0; i < WAITER_COUNT; i++)
        waiter_init(&waiters[i], &futex, futex, TIME_MS(200));

    waiters_start(waiters, WAITER_COUNT);

    __wt_sleep(0, TIME_MS(50));
    __atomic_store_n(&futex, WAKEUP_VAL, __ATOMIC_RELAXED);
    testutil_check(__wt_futex_wake(&futex, WT_FUTEX_WAKE_ONE, WAKEUP_VAL));

    waiters_join(waiters, WAITER_COUNT);
    collect_outcomes(waiters, WAITER_COUNT, futex, &outcomes);

    /* Ideally: 1 wake, 1 timeout. */
    check_outcomes(&outcomes, 1, 1);
#undef WAITER_COUNT
}

/*
 * test_wake_two_of_two --
 *     Both threads should wake.
 */
static void
test_wake_two_of_two(TEST_OPTS *opts)
{
    static const int WAKEUP_VAL = 1;

    struct waiters_outcomes outcomes;
#define WAITER_COUNT 2
    struct waiter waiters[WAITER_COUNT];
    WT_FUTEX_WORD futex;
    int i;

    VERBOSE_ANNOUNCE(opts);

    futex = 0;
    for (i = 0; i < WAITER_COUNT; i++)
        waiter_init(&waiters[i], &futex, futex, TIME_MS(200));

    waiters_start(waiters, WAITER_COUNT);

    __wt_sleep(0, TIME_MS(50));
    testutil_check(__wt_futex_wake(&futex, WT_FUTEX_WAKE_ALL, WAKEUP_VAL));

    waiters_join(waiters, WAITER_COUNT);
    collect_outcomes(waiters, WAITER_COUNT, futex, &outcomes);

    check_outcomes(&outcomes, 2, 0);
#undef WAITER_COUNT
}

/*
 * main --
 *     Test driver.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    test_wake_up_single(opts);
    test_time_out_single(opts);
    test_spurious_wake_up_single(opts);
    test_wake_one_of_two(opts);
    test_wake_two_of_two(opts);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
