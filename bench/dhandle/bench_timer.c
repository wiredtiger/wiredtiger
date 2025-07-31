/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "test_util.h"
#include "bench_timer.h"

/*
 * bench_timer_init --
 *     Initialize the bench timer structure.
 */
void
bench_timer_init(BENCH_TIMER *timer, const char *name)
{
    timer->name = name;
    timer->total_ns = 0;
    timer->start_ns = 0;
    timer->count = 0;
}

/*
 * bench_timer_start --
 *     Start a timing.
 */
void
bench_timer_start(BENCH_TIMER *timer, uint64_t ns)
{
    assert(ns != 0);
    assert(timer->start_ns == 0);
    timer->start_ns = ns;
}

/*
 * bench_timer_stop --
 *     Stop a timing.
 */
void
bench_timer_stop(BENCH_TIMER *timer, uint64_t ns)
{
    assert(ns != 0);
    assert(timer->start_ns != 0);
    assert(ns > timer->start_ns);
    timer->total_ns += (ns - timer->start_ns);
    timer->count++;
}

/*
 * bench_timer_add --
 *     Add results from another timer to this one.
 */
void
bench_timer_add(BENCH_TIMER *timer, const BENCH_TIMER *that)
{
    timer->total_ns += that->total_ns;
    timer->count += that->count;
}

/*
 * bench_timer_add_to_shared --
 *     Add timing results to this timer, that is shared among multiple threads.
 */
void
bench_timer_add_to_shared(BENCH_TIMER *timer, uint64_t ns, uint64_t count)
{
    WT_RELEASE_WRITE_WITH_BARRIER(timer->total_ns, timer->total_ns + ns);
    WT_RELEASE_WRITE_WITH_BARRIER(timer->count, timer->count + count);
}

/*
 * bench_timer_add_to_shared --
 *     Add results from another shared timer to this (non-shared) timer.
 */
void
bench_timer_add_from_shared(BENCH_TIMER *timer, BENCH_TIMER *that)
{
    uint64_t ns, count;

    WT_ACQUIRE_READ_WITH_BARRIER(ns, that->total_ns);
    WT_ACQUIRE_READ_WITH_BARRIER(count, that->count);
    timer->total_ns += ns;
    timer->count += count;
}

/*
 * bench_timer_format --
 *     Format a number, given as nanoseconds per operation, in a readable way.
 */
static void
__bench_timer_format(char *buf, size_t len, double ns_op)
{
    if (ns_op > WT_BILLION)
        snprintf(buf, len, "%10.3f secs/op", ns_op / WT_BILLION);
    else if (ns_op > WT_MILLION)
        snprintf(buf, len, "%10.3f msecs/op", ns_op / WT_MILLION);
    else if (ns_op > WT_THOUSAND)
        snprintf(buf, len, "%10.3f usecs/op", ns_op / WT_THOUSAND);
    else
        snprintf(buf, len, "%10.3f nsecs/op", ns_op);
}

/*
 * bench_timer_show_change --
 *     For a difference between two timers, show a summary of the number of operations, and the time
 *     taken per operation.
 */
bool
bench_timer_show_change(BENCH_TIMER *before, BENCH_TIMER *after)
{
    uint64_t ns, count;
    char num[20];

    if (before->count != after->count) {
        assert(before->count < after->count);
        assert(before->total_ns <= after->total_ns);
        ns = after->total_ns - before->total_ns;
        count = after->count - before->count;
        __bench_timer_format(num, sizeof(num), (double)ns / count);
        fprintf(stderr, " %ss: %" PRIu64 " ops, %s\n", after->name, count, num);
        return (true);
    }
    return (false);
}
