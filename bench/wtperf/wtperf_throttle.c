/*-
 * Public Domain 2014-2015 MongoDB, Inc.
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

#include "wtperf.h"

/*
 * Put the initial config together for running a throttled workload.
 */
int
setup_throttle(CONFIG_THREAD *thread) {
	THROTTLE_CONFIG *throttle_cfg;
	throttle_cfg = &thread->throttle_cfg;

	/* If the interval is very small, we do laps of 1 */
	if (thread->workload->throttle < THROTTLE_OPS) {
		throttle_cfg->tickets_per_increment = 1;
		throttle_cfg->usecs_increment =
			USEC_PER_SEC / thread->workload->throttle;
	} else if (thread->workload->throttle < THROTTLE_INTVL) {
		throttle_cfg->usecs_increment =
		    USEC_PER_SEC / thread->workload->throttle * THROTTLE_OPS;
		throttle_cfg->tickets_per_increment = THROTTLE_OPS;
	} else {
		/* If the interval is too large, we do more tickets per batch */
		throttle_cfg->usecs_increment = THROTTLE_OPS;
		throttle_cfg->tickets_per_increment =
		    thread->workload->throttle / THROTTLE_INTVL;
	}

	/* Give the queue some initial tickets to work with */
	throttle_cfg->ticket_queue = throttle_cfg->tickets_per_increment;

	/* Set the first timestamp of when we incremented */
	WT_RET(__wt_epoch(NULL, &throttle_cfg->last_increment));
	printf("setup to run with throttle. This thread will do %lu ops every %lu us\n",
	    throttle_cfg->tickets_per_increment, throttle_cfg->usecs_increment);
	printf("this means we are performing %llu ops per second\n",
	    (USEC_PER_SEC / throttle_cfg->usecs_increment) * throttle_cfg->tickets_per_increment); 
	return (0);
}

/*
 * TODO: Spread the stalls out, so we don't flood at the start of each
 * second and then pause. Doing this every 10th of a second is probably enough
 */
int
worker_throttle(CONFIG_THREAD *thread) {
	THROTTLE_CONFIG *throttle_cfg;
	struct timespec now;
	uint64_t usecs_delta;

	throttle_cfg = &thread->throttle_cfg;

	if (throttle_cfg->ticket_queue != 0)
		return (0);

	WT_RET(__wt_epoch(NULL, &now));

	/*
	 * If we did enough operations in the current interval, sleep for
	 * the rest of the interval. Then add more tickets to the queue.
	 */
	usecs_delta = WT_TIMEDIFF_US(now, throttle_cfg->last_increment);
	if (usecs_delta < throttle_cfg->usecs_increment - THROTTLE_OPS) {
		(void)usleep(
		    (useconds_t)(throttle_cfg->usecs_increment - usecs_delta));
		throttle_cfg->ticket_queue =
		     throttle_cfg->tickets_per_increment;
	} else
		throttle_cfg->ticket_queue =
		     (usecs_delta / throttle_cfg->usecs_increment) *
		     throttle_cfg->tickets_per_increment;

	/* Don't over-fill the queue */
	throttle_cfg->ticket_queue =
	    WT_MIN(throttle_cfg->ticket_queue, thread->workload->throttle);

	/*
	 * After sleeping, set the interval to the current time.
	 */
	WT_RET(__wt_epoch(NULL, &throttle_cfg->last_increment));
	return (0);
}
