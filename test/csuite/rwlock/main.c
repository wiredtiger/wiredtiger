/*-
 * Public Domain 2014-2017 MongoDB, Inc.
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
 * JIRA ticket reference: HELP-4355
 * Test rwlock collapse under load.
 */
#define	MAX_THREADS 1000
//#define	READS_PER_WRITE	10000
//#define	READS_PER_WRITE	1000000
#define	READS_PER_WRITE	100
#define	USE_POSIX false

WT_RWLOCK rwlock;
pthread_rwlock_t p_rwlock;
void *thread_rwlock(void *);

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	struct timespec te, ts;
	pthread_t id[MAX_THREADS];
	int i;

	if (!testutil_enable_long_tests())	/* Ignore unless requested */
		return (EXIT_SUCCESS);

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	opts->nthreads = 100;
	opts->nops = 1000000; /* per thread */
	testutil_check(testutil_parse_opts(argc, argv, opts));

	testutil_make_work_dir(opts->home);
	testutil_check(wiredtiger_open(opts->home, NULL,
	    "create,session_max=1000,statistics=(fast)", &opts->conn));

	__wt_rwlock_init(NULL, &rwlock);
	pthread_rwlock_init(&p_rwlock, NULL);

	__wt_epoch(NULL, &ts);
	for (i = 0; i < (int)opts->nthreads; ++i) {
		testutil_check(pthread_create(
		    &id[i], NULL, thread_rwlock, (void *)opts));
	}
	while (--i >= 0)
		testutil_check(pthread_join(id[i], NULL));
	__wt_epoch(NULL, &te);
	printf("%.2lf\n", WT_TIMEDIFF_MS(te, ts) / 1000.0);

	pthread_rwlock_destroy(&p_rwlock);
	testutil_cleanup(opts);
	return (EXIT_SUCCESS);
}

/*
 * Acquire a rwlock, every Nth operation, acquire exclusive.
 */
void *
thread_rwlock(void *arg)
{
	TEST_OPTS *opts;
	WT_SESSION *wt_session;
	WT_SESSION_IMPL *session;
	uint64_t i, counter;
	bool writelock;

	opts = (TEST_OPTS *)arg;
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &wt_session));
	session = (WT_SESSION_IMPL *)wt_session;

	printf("Running rwlock thread\n");
	for (i = 1; i <= opts->nops; ++i) {
		writelock = (i % READS_PER_WRITE == 0);

		if (USE_POSIX) {
			if (writelock)
				pthread_rwlock_wrlock(&p_rwlock);
			else
				pthread_rwlock_rdlock(&p_rwlock);
		} else {
			if (writelock)
				__wt_writelock(session, &rwlock);
			else
				__wt_readlock(session, &rwlock);
		}

		__wt_atomic_add64(&counter, 1);

		if (USE_POSIX) {
			pthread_rwlock_unlock(&p_rwlock);
		} else {
			if (writelock)
				__wt_writeunlock(session, &rwlock);
			else
				__wt_readunlock(session, &rwlock);
		}

		if (i % 10000 == 0) {
			printf(".");
			fflush(stdout);
		}
	}

	opts->running = false;

	return (NULL);
}
