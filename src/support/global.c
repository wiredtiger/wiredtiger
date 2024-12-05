/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

WT_PROCESS __wt_process;             /* Per-process structure */
static int __wt_pthread_once_failed; /* If initialization failed */

/*
 * This is the list of the timing stress configuration names and flags. It is a global structure
 * instead of declared in the config function so that other functions can use the name/flag
 * association.
 */
const WT_NAME_FLAG __wt_stress_types[] = {
  /*
   * Each split race delay is controlled using a different flag to allow more effective race
   * condition detection, since enabling all delays at once can lead to an overall slowdown to the
   * point where race conditions aren't encountered.
   *
   * Fail points are also defined in this list and will occur randomly when enabled.
   */
  {"aggressive_stash_free", WT_TIMING_STRESS_AGGRESSIVE_STASH_FREE},
  {"aggressive_sweep", WT_TIMING_STRESS_AGGRESSIVE_SWEEP},
  {"backup_rename", WT_TIMING_STRESS_BACKUP_RENAME},
  {"checkpoint_evict_page", WT_TIMING_STRESS_CHECKPOINT_EVICT_PAGE},
  {"checkpoint_handle", WT_TIMING_STRESS_CHECKPOINT_HANDLE},
  {"checkpoint_slow", WT_TIMING_STRESS_CHECKPOINT_SLOW},
  {"checkpoint_stop", WT_TIMING_STRESS_CHECKPOINT_STOP},
  {"commit_transaction_slow", WT_TIMING_STRESS_COMMIT_TRANSACTION_SLOW},
  {"compact_slow", WT_TIMING_STRESS_COMPACT_SLOW},
  {"evict_reposition", WT_TIMING_STRESS_EVICT_REPOSITION},
  {"failpoint_eviction_split", WT_TIMING_STRESS_FAILPOINT_EVICTION_SPLIT},
  {"failpoint_history_delete_key_from_ts",
    WT_TIMING_STRESS_FAILPOINT_HISTORY_STORE_DELETE_KEY_FROM_TS},
  {"history_store_checkpoint_delay", WT_TIMING_STRESS_HS_CHECKPOINT_DELAY},
  {"history_store_search", WT_TIMING_STRESS_HS_SEARCH},
  {"history_store_sweep_race", WT_TIMING_STRESS_HS_SWEEP},
  {"prefetch_1", WT_TIMING_STRESS_PREFETCH_1}, {"prefetch_2", WT_TIMING_STRESS_PREFETCH_2},
  {"prefetch_3", WT_TIMING_STRESS_PREFETCH_3}, {"prefix_compare", WT_TIMING_STRESS_PREFIX_COMPARE},
  {"prepare_checkpoint_delay", WT_TIMING_STRESS_PREPARE_CHECKPOINT_DELAY},
  {"prepare_resolution_1", WT_TIMING_STRESS_PREPARE_RESOLUTION_1},
  {"prepare_resolution_2", WT_TIMING_STRESS_PREPARE_RESOLUTION_2},
  {"sleep_before_read_overflow_onpage", WT_TIMING_STRESS_SLEEP_BEFORE_READ_OVERFLOW_ONPAGE},
  {"split_1", WT_TIMING_STRESS_SPLIT_1}, {"split_2", WT_TIMING_STRESS_SPLIT_2},
  {"split_3", WT_TIMING_STRESS_SPLIT_3}, {"split_4", WT_TIMING_STRESS_SPLIT_4},
  {"split_5", WT_TIMING_STRESS_SPLIT_5}, {"split_6", WT_TIMING_STRESS_SPLIT_6},
  {"split_7", WT_TIMING_STRESS_SPLIT_7}, {"split_8", WT_TIMING_STRESS_SPLIT_8},
  {"tiered_flush_finish", WT_TIMING_STRESS_TIERED_FLUSH_FINISH}, {NULL, 0}};

/*
 * __endian_check --
 *     Check the build matches the machine.
 */
static int
__endian_check(void)
{
    uint64_t v;
    const char *e;
    bool big;

    v = 1;
    big = *((uint8_t *)&v) == 0;

#ifdef WORDS_BIGENDIAN
    if (big)
        return (0);
    e = "big-endian";
#else
    if (!big)
        return (0);
    e = "little-endian";
#endif
    fprintf(stderr,
      "This is a %s build of the WiredTiger data engine, incompatible with this system\n", e);
    return (EINVAL);
}

/*
 * __reset_thread_tick --
 *     Reset the OS task time slice to raise the probability of uninterrupted run afterwards.
 */
static void
__reset_thread_tick(void)
{
    /*
     * We could use __wt_yield() here but simple yielding doesn't seem to always reset the thread's
     * time slice. Sleeping for a short time does a better job.
     */
    __wt_sleep(0, 10);
}

/*
 * __get_epoch_and_tsc --
 *     Get the current time and TSC ticks before and after the call to __wt_epoch. Returns the
 *     duration of the call in TSC ticks.
 */
static WT_INLINE uint64_t
__get_epoch_and_tsc(struct timespec *clock1, uint64_t *tsc1, uint64_t *tsc2)
{
    *tsc1 = __wt_rdtsc();
    __wt_epoch(NULL, clock1);
    *tsc2 = __wt_rdtsc();
    return (*tsc2 - *tsc1);
}

/*
 * __compare_uint64 --
 *     uint64_t comparison function.
 */
static int
__compare_uint64(const void *a, const void *b)
{
    return (int)(*(uint64_t *)a - *(uint64_t *)b);
}

/*
 * __get_epoch_call_ticks --
 *     Returns how many ticks it takes to call __wt_epoch at best and on average.
 */
static void
__get_epoch_call_ticks(uint64_t *epoch_ticks_min, uint64_t *epoch_ticks_avg)
{
#define EPOCH_CALL_CALIBRATE_SAMPLES 50
    uint64_t duration[EPOCH_CALL_CALIBRATE_SAMPLES];

    __reset_thread_tick();
    for (int i = 0; i < EPOCH_CALL_CALIBRATE_SAMPLES; ++i) {
        struct timespec clock1;
        uint64_t tsc1, tsc2;
        duration[i] = __get_epoch_and_tsc(&clock1, &tsc1, &tsc2);
    }
    __wt_qsort(duration, EPOCH_CALL_CALIBRATE_SAMPLES, sizeof(uint64_t), __compare_uint64);

    /*
     * Use 30% percentile (median) for "average". Also, on some platforms the clock rate is so slow
     * that TSC difference can be 0, so add a little bit for some lee-way.
     */
    *epoch_ticks_avg = duration[EPOCH_CALL_CALIBRATE_SAMPLES / 3] + 1;

    /* Throw away first few results as outliers for the "best". */
    *epoch_ticks_min = duration[2];
}

/*
 * __get_epoch_and_ticks --
 *     Gets the current time as wall clock and TSC ticks. Uses multiple attempts to make sure that
 *     there's a limited time between the two.
 *
 * Returns:
 *
 * - true if it managed to get a good result is good; false upon failure.
 *
 * - clock_time: the wall clock time.
 *
 * - tsc_time: CPU TSC time.
 */
static bool
__get_epoch_and_ticks(struct timespec *clock_time, uint64_t *tsc_time, uint64_t epoch_ticks_min,
  uint64_t epoch_ticks_avg)
{
    uint64_t ticks_best = epoch_ticks_avg + 1; /* Not interested in anything worse than average. */
#define GET_EPOCH_MAX_ATTEMPTS 200
    for (int i = 0; i < GET_EPOCH_MAX_ATTEMPTS; ++i) {
        struct timespec clock1;
        uint64_t tsc1, tsc2;
        uint64_t duration = __get_epoch_and_tsc(&clock1, &tsc1, &tsc2);

        /* If it took the minimum time, we're happy with the result - return it straight away. */
        if (duration <= epoch_ticks_min) {
            *clock_time = clock1;
            *tsc_time = tsc1;
            return (true);
        }

        if (duration <= ticks_best) {
            /* Remember the best result. */
            *clock_time = clock1;
            *tsc_time = tsc1;
            ticks_best = duration;
        }
    }

    /* Return true if we have a good enough result. */
    return (ticks_best <= epoch_ticks_avg);
}

#define CLOCK_CALIBRATE_USEC 10000 /* Number of microseconds for clock calibration. */

/*
 * __global_calibrate_ticks --
 *     Calibrate a ratio from rdtsc ticks to nanoseconds.
 */
static void
__global_calibrate_ticks(void)
{
    /*
     * Default to using __wt_epoch until we have a good value for the ratio.
     */
    __wt_process.tsc_nsec_ratio = WT_TSC_DEFAULT_RATIO;
    __wt_process.use_epochtime = true;

#if defined(__amd64) || defined(__aarch64__)
    {
        uint64_t epoch_ticks_min, epoch_ticks_avg, tsc_start, tsc_stop;
        struct timespec clock_start, clock_stop;

        __get_epoch_call_ticks(&epoch_ticks_min, &epoch_ticks_avg);

        if (!__get_epoch_and_ticks(&clock_start, &tsc_start, epoch_ticks_min, epoch_ticks_avg))
            return;

        __wt_sleep(0, CLOCK_CALIBRATE_USEC);

        if (!__get_epoch_and_ticks(&clock_stop, &tsc_stop, epoch_ticks_min, epoch_ticks_avg))
            return;

        uint64_t diff_nsec = WT_TIMEDIFF_NS(clock_stop, clock_start);
        uint64_t diff_tsc = tsc_stop - tsc_start;
#define CLOCK_MIN_DIFF_NSEC 10
#define CLOCK_MIN_DIFF_TSC 10
        if (diff_nsec < CLOCK_MIN_DIFF_NSEC || diff_tsc < CLOCK_MIN_DIFF_TSC)
            /* Too short to be meaningful or not enough granularity. */
            return;

        double ratio = (double)diff_tsc / (double)diff_nsec;
        if (ratio <= DBL_EPSILON)
            /* Too small to be meaningful. */
            return;

        __wt_process.tsc_nsec_ratio = ratio;
        __wt_process.use_epochtime = false;
    }
#endif
}

/*
 * __global_once --
 *     Global initialization, run once.
 */
static void
__global_once(void)
{
    WT_DECL_RET;

    if ((ret = __wt_spin_init(NULL, &__wt_process.spinlock, "global")) != 0) {
        __wt_pthread_once_failed = ret;
        return;
    }

    TAILQ_INIT(&__wt_process.connqh);

    /*
     * Set up the checksum functions. If there's only one, set it as the alternate, that way code
     * doesn't have to check if it's set or not.
     */
    __wt_process.checksum = wiredtiger_crc32c_func();
    __wt_process.checksum_with_seed = wiredtiger_crc32c_with_seed_func();

    __global_calibrate_ticks();

    /* Run-time configuration. */
#ifdef WT_STANDALONE_BUILD
    __wt_process.fast_truncate_2022 = true;
    __wt_process.tiered_shared_2023 = true;
#endif
}

/*
 * __wt_library_init --
 *     Some things to do, before we do anything else.
 */
int
__wt_library_init(void)
{
    static bool first = true;
    WT_DECL_RET;

    /* Check the build matches the machine. */
    WT_RET(__endian_check());

    /*
     * Do per-process initialization once, before anything else, but only once. I don't know how
     * heavy-weight the function (pthread_once, in the POSIX world), might be, so I'm front-ending
     * it with a local static and only using that function to avoid a race.
     */
    if (first) {
        if ((ret = __wt_once(__global_once)) != 0)
            __wt_pthread_once_failed = ret;
        first = false;
    }
    return (__wt_pthread_once_failed);
}
