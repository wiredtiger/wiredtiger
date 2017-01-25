/*-
 * Public Domain 2014-2016 MongoDB, Inc.
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

static int
check_timing(WTPERF *wtperf,
    const char *name, struct timespec start, struct timespec *stop)
{
	CONFIG_OPTS *opts;
	uint64_t last_interval;

	opts = wtperf->opts;

	__wt_epoch(NULL, stop);

	last_interval = (uint64_t)(WT_TIMEDIFF_SEC(*stop, start));

	if (last_interval > opts->idle_table_cycle) {
		lprintf(wtperf, ETIMEDOUT, 0,
		    "Cycling idle table failed because %s took %" PRIu64
		    " seconds which is longer than configured acceptable"
		    " maximum of %" PRIu32 ".",
		    name, last_interval, opts->idle_table_cycle);
		wtperf->error = true;
		return (ETIMEDOUT);
	}
	return (0);
}
/*
 * Regularly create, open a cursor and drop a table.
 * Measure how long each step takes, and flag an error if it exceeds the
 * configured maximum.
 */
static void *
cycle_idle_tables(void *arg)
{
	struct timespec start, stop;
	CONFIG_OPTS *opts;
	WTPERF *wtperf;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	int cycle_count, ret;
	char uri[512];

	wtperf = (WTPERF *)arg;
	opts = wtperf->opts;
	cycle_count = 0;

	if ((ret = wtperf->conn->open_session(
	    wtperf->conn, NULL, opts->sess_config, &session)) != 0) {
		lprintf(wtperf, ret, 0,
		    "Error opening a session on %s", wtperf->home);
		return (NULL);
	}

	for (cycle_count = 0; wtperf->idle_cycle_run; ++cycle_count) {
		snprintf(uri, sizeof(uri),
		    "%s_cycle%07d", wtperf->uris[0], cycle_count);
		/* Don't busy cycle in this loop. */
		__wt_sleep(1, 0);

		/* Setup a start timer. */
		__wt_epoch(NULL, &start);

		/* Create a table. */
		if ((ret = session->create(
		    session, uri, opts->table_config)) != 0) {
			if (ret == EBUSY)
				continue;
			lprintf(wtperf, ret, 0,
			     "Table create failed in cycle_idle_tables.");
			wtperf->error = true;
			return (NULL);
		}
		if (check_timing(wtperf, "create", start, &stop) != 0)
			return (NULL);
		start = stop;

		/* Open and close cursor. */
		if ((ret = session->open_cursor(
		    session, uri, NULL, NULL, &cursor)) != 0) {
			lprintf(wtperf, ret, 0,
			     "Cursor open failed in cycle_idle_tables.");
			wtperf->error = true;
			return (NULL);
		}
		if ((ret = cursor->close(cursor)) != 0) {
			lprintf(wtperf, ret, 0,
			     "Cursor close failed in cycle_idle_tables.");
			wtperf->error = true;
			return (NULL);
		}
		if (check_timing(wtperf, "cursor", start, &stop) != 0)
			return (NULL);
		start = stop;

#if 1
		/*
		 * Drop the table. Keep retrying on EBUSY failure - it is an
		 * expected return when checkpoints are happening.
		 */
		while ((ret = session->drop(
		    session, uri, "force,checkpoint_wait=false")) == EBUSY)
			__wt_sleep(1, 0);

		if (ret != 0 && ret != EBUSY) {
			lprintf(wtperf, ret, 0,
			     "Table drop failed in cycle_idle_tables.");
			wtperf->error = true;
			return (NULL);
		}
		if (check_timing(wtperf, "drop", start, &stop) != 0)
			return (NULL);
#endif
	}

	return (NULL);
}

/*
 * Start a thread the creates and drops tables regularly.
 * TODO: Currently accepts a pthread_t as a parameter, since it is not
 * possible to portably statically initialize it in the global configuration
 * structure. Should reshuffle the configuration structure so explicit static
 * initialization isn't necessary.
 */
int
start_idle_table_cycle(WTPERF *wtperf, pthread_t *idle_table_cycle_thread)
{
	CONFIG_OPTS *opts;
	pthread_t thread_id;
	int ret;

	opts = wtperf->opts;

	if (opts->idle_table_cycle == 0)
		return (0);

	wtperf->idle_cycle_run = true;
	if ((ret = pthread_create(
	    &thread_id, NULL, cycle_idle_tables, wtperf)) != 0) {
		lprintf(wtperf,
		    ret, 0, "Error creating idle table cycle thread.");
		wtperf->idle_cycle_run = false;
		return (ret);
	}
	*idle_table_cycle_thread = thread_id;

	return (0);
}

int
stop_idle_table_cycle(WTPERF *wtperf, pthread_t idle_table_cycle_thread)
{
	CONFIG_OPTS *opts;
	int ret;

	opts = wtperf->opts;

	if (opts->idle_table_cycle == 0 || !wtperf->idle_cycle_run)
		return (0);

	wtperf->idle_cycle_run = false;
	if ((ret = pthread_join(idle_table_cycle_thread, NULL)) != 0) {
		lprintf(
		    wtperf, ret, 0, "Error joining idle table cycle thread.");
		return (ret);
	}
	return (0);
}
