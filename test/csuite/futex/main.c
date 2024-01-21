#include "test_util.h"

#include "futex.h"


typedef struct {
    /* Futex memory location. */
    void *futex;

    /* Value to write to futex. */
    uint32_t wake_value;

    /* Time to wait before signalling futex. */
    uint64_t delay_sec;
    uint64_t delay_usec;
} waker_thread_ctx;

typedef enum {
    WAITER_EXPECT_WAKEUP,
    WAITER_EXPECT_TIMEOUT
} WAITER_EXPECT;

typedef struct {
    /* Futex memory location. */
    void *futex;

    /* Expected value of futex upon waking. */
    uint32_t initial_value;
    uint32_t wake_value;

    WAITER_EXPECT expect;

    /* Futex wait timeout period. */
    uint32_t timeout_ms;
} waiter_thread_ctx;


#define WAITER_CTX(ftx, init_val, wake_val, exp, timeout) \
    (waiter_thread_ctx){ ftx, init_val, wake_val, exp, timeout }


static void test_wake_one(TEST_OPTS *opts);
static void test_timeout_one(TEST_OPTS *opts);
static void test_wake_many(TEST_OPTS *opts);
static void *waker_thread(void *arg);
static void *waiter_thread(void *arg);


int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));

    test_wake_one(opts);
    test_timeout_one(opts);
    test_wake_many(opts);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}

static void 
test_wake_one(TEST_OPTS *opts)
{
    static const uint32_t WAIT_TIMEOUT_MS = 2000UL;
    static const uint32_t FTX_INITIAL_VAL = 0;

    pthread_t waker_id;
    struct timespec end, start;
    waker_thread_ctx waker_arg;
    double wait_time_ms;
    uint32_t *futex;
    
    if (opts->verbose)
        puts("test_wake_one");

    testutil_check(posix_memalign((void **)&futex, sizeof(void *), sizeof(uint32_t)));
    waker_arg = (waker_thread_ctx){ futex, 1, 1, 0};
    __wt_epoch(NULL, &start);
    WT_PUBLISH(*futex, FTX_INITIAL_VAL);
    testutil_check(pthread_create(&waker_id, NULL, waker_thread, &waker_arg));
    testutil_check(__wt_futex_wait(futex, FTX_INITIAL_VAL, WAIT_TIMEOUT_MS));
    testutil_check(pthread_join(waker_id, NULL));
    __wt_epoch(NULL, &end);

    wait_time_ms = WT_TIMEDIFF_MS(end, start);
    if (opts->verbose) 
        printf("Wait time %.2lf ms\n", wait_time_ms);
    WT_ASSERT(NULL, wait_time_ms < WAIT_TIMEOUT_MS);
    WT_ASSERT(NULL, *futex == waker_arg.wake_value);

    free(futex);
}

static void 
test_timeout_one(TEST_OPTS *opts)
{
    static const uint32_t FTX_INITIAL_VAL = 0;
    static const uint32_t FTX_WAKE_VAL = 0xDEAD;
    static const uint32_t WAITER_TIMEOUT_MS = 1000; 

    waiter_thread_ctx waiter_arg;
    uint32_t *futex;
    pthread_t waiter_id;

    if (opts->verbose)
        puts("test_timeout_one");
    
    testutil_check(posix_memalign((void **)&futex, sizeof(void *), sizeof(uint32_t)));
    waiter_arg = WAITER_CTX(futex, FTX_INITIAL_VAL, FTX_WAKE_VAL, 
        WAITER_EXPECT_TIMEOUT, WAITER_TIMEOUT_MS);
    WT_PUBLISH(*futex, FTX_INITIAL_VAL);
    testutil_check(pthread_create(&waiter_id, NULL, waiter_thread, &waiter_arg));
    testutil_check(pthread_join(waiter_id, NULL));

    free(futex);
}

static void 
test_wake_many(TEST_OPTS *opts)
{
    uint32_t *futex;

    if (opts->verbose)
        puts("test_wake_many");

    testutil_check(posix_memalign((void **)&futex, sizeof(void *), sizeof(uint32_t)));

    free(futex);
}

static void *
waker_thread(void *arg)
{
    waker_thread_ctx *ctx = arg;
    uint32_t *futex = ctx->futex;
    __wt_sleep(ctx->delay_sec, ctx->delay_usec);
    WT_PUBLISH(*futex, ctx->wake_value);
    testutil_check(__wt_futex_wake(futex, WT_FUTEX_WAKE_ONE));
    return (NULL);
}

static void *
waiter_thread(void *arg)
{
    waiter_thread_ctx *ctx = arg;
    uint32_t *futex = ctx->futex;
    int ret;
    uint32_t wake_value;

    ret = __wt_futex_wait(futex, ctx->initial_value, ctx->timeout_ms);
    if (ret == 0) {
        testutil_assert(ctx->expect == WAITER_EXPECT_WAKEUP);
        WT_ORDERED_READ(wake_value, *futex);
        testutil_assert(wake_value == ctx->wake_value);
    }
    else if (ret == -1 && errno == ETIMEDOUT)
        testutil_assert(ctx->expect == WAITER_EXPECT_TIMEOUT);
    else
        testutil_assert_errno(errno);
 
    return (NULL);
}
