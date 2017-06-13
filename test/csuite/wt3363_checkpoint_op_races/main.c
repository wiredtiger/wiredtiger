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
 * JIRA ticket reference: WT-3362
 *
 * Test case description: There are a number of operations that we run that we
 * expect not to conflict with or block against a running checkpoint. This test
 * aims to run repeated checkpoints in a thread, while running an assortment
 * of operations that we expect to execute quickly on further threads. To
 * ensure that we catch any blockages we introduce a very large delay into the
 * checkpoint and measure that no operation takes 1/2 the length of this delay.
 *
 * Failure mode: We monitor the execution time of all operations and if we find
 * any operation taking longer than 1/2 the delay time, we abort dumping a core
 * file which can be used to determine what operation was blocked.
 */

int handle_error(WT_EVENT_HANDLER *, WT_SESSION *, int, const char *);
int handle_message(WT_EVENT_HANDLER *, WT_SESSION *, const char *);
void* monitor(void *);
void* do_checkpoints(void *);
void* do_ops(void *);
void op_bulk(WT_CONNECTION *, const char *);
void op_bulk_unique(WT_CONNECTION *, const char *, int, int);
void op_create(WT_CONNECTION *, const char *);
void op_create_unique(WT_CONNECTION *, const char *, int, int);
void op_cursor(WT_CONNECTION *);
void op_drop(WT_CONNECTION *, int);

#define	N_THREADS 10
#define	RUNTIME 900.0

typedef struct {
	TEST_OPTS *testopts;
	int threadnum;
	int nthread;
	int done;
} THREAD_ARGS;

pthread_rwlock_t single;
/*
 * Time delay to introduce into checkpoints in seconds. Should be at-least
 * double the maximum time that any one of the operations should take. Currently
 * this is set to 10 seconds and we expect no single operation to take longer
 * than 5 seconds.
 */
const uint64_t max_execution_time = 10;

/* Int used to make unique table names */
uint64_t uid = 1;

/*
 * An array of the number of operations that each of the worker threads has
 * performed.
 */
uint64_t thread_counters[N_THREADS];
const char *uri;

int
main(int argc, char *argv[])
{
	static WT_EVENT_HANDLER event_handler = {
		handle_error,
		handle_message,
		NULL,
		NULL
	};
	TEST_OPTS *opts, _opts;
	THREAD_ARGS thread_args[N_THREADS];
	pthread_t ckpt_thread, mon_thread, threads[N_THREADS];
	int i;

	if (!testutil_enable_long_tests())	/* Ignore unless requested */
		return (EXIT_SUCCESS);

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));

	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(wiredtiger_open(
	    opts->home, &event_handler, "create,cache_size=1G,", &opts->conn));

	uri = opts->uri;

	testutil_check(pthread_create(
	    &ckpt_thread, NULL, do_checkpoints, (void *)opts->conn));

	for (i = 0; i < N_THREADS; ++i) {
		thread_counters[i] = 0;
		thread_args[i].threadnum = i;
		thread_args[i].nthread = N_THREADS;
		thread_args[i].testopts = opts;
		testutil_check(pthread_create(&threads[i], NULL,
		    do_ops, (void *)&thread_args[i]));
	}

	testutil_check(pthread_create(&mon_thread, NULL, monitor, NULL));

	for (i = 0; i < N_THREADS; ++i)
		testutil_check(pthread_join(threads[i], NULL));

	testutil_check(pthread_join(mon_thread, NULL));

	testutil_check(pthread_join(ckpt_thread, NULL));

	printf("Success\n");

	testutil_cleanup(opts);
	return (EXIT_SUCCESS);
}

/* WiredTiger error handling function */
int
handle_error(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, int error, const char *errmsg)
{
	(void)(handler);
	(void)(session);
	(void)(error);

	/* Ignore complaints about missing files. */
	if (error == ENOENT)
		return (0);

	/* Ignore complaints about failure to open bulk cursors. */
	if (strstr(
	    errmsg, "bulk-load is only supported on newly created") != NULL)
		return (0);

	return (fprintf(stderr, "%s\n", errmsg) < 0 ? -1 : 0);
}

/* WiredTiger message handling function */
int
handle_message(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, const char *message)
{
	(void)(handler);
	(void)(session);

	/* Ignore messages about failing to create forced checkpoints. */
	if (strstr(
	    message, "forced or named checkpoint") != NULL)
		return (0);

	return (printf("%s\n", message) < 0 ? -1 : 0);
}

/* Function for repeatedly running checkpoint operations */
void *
do_checkpoints(void *connection)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	time_t now, start;
	int ret;
	char config[64];

	conn = (WT_CONNECTION *)connection;
	testutil_check(__wt_snprintf(config, sizeof(config),
	    "force,debug_checkpoint_latency=%" PRIu64, max_execution_time));
	(void)time(&start);
	(void)time(&now);

	while (difftime(now, start) < RUNTIME) {
		if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
			testutil_die(ret, "conn.session");

		if ((ret = session->checkpoint(session, config)) != 0)
			if (ret != EBUSY && ret != ENOENT)
				testutil_die(ret, "session.checkpoint");

		if ((ret = session->close(session, NULL)) != 0)
			testutil_die(ret, "session.close");
		(void)time(&now);

		/*
		 * A short sleep to let operations process and avoid back to
		 * back checkpoints locking up resources.
		 */
		sleep(1);
	}

	return (NULL);
}

/*
 * Function to monitor running operations and abort to dump core in the event
 * that we catch an operation running long.
 */
void *
monitor(void *args)
{
	time_t now, start;
	uint64_t last_ops[N_THREADS];
	int i;

	/* Unused */
	(void)args;

	(void)time(&start);
	(void)time(&now);

	for (i = 0; i < N_THREADS; i++)
		last_ops[i] = 0;

	while (difftime(now, start) < RUNTIME) {
		/*
		 * Checkpoints will run for slightly over max_execution_time.
		 * max_execution_times should always be long enough that we can
		 * complete any single operation in 1/2 that time.
		 */
		sleep(max_execution_time/2);

		for (i = 0; i < N_THREADS; i++) {
			/* Ignore any threads which may not have started yet */
			if (thread_counters[i] == 0)
				continue;
			/*
			 * We track how many operations each thread has done. If
			 * we have slept and the counter remains the same for a
			 * thread it is stuck and should drop a core so the
			 * cause of the hang can be investigated.
			 */
			if (thread_counters[i] != last_ops[i])
				last_ops[i] = thread_counters[i];
			else {
				printf("Thread %d had a task running"
				    " for more than %" PRIu64  " seconds\n",
				    i, max_execution_time/2);
				abort();

			}
		}
		(void)time(&now);
	}

	return (NULL);
}

/* Worker thread. Executes random operations from the set of 6 */
void *
do_ops(void *args)
{
	THREAD_ARGS *arg;
	WT_CONNECTION *conn;
	WT_RAND_STATE rnd;
	time_t now, start;
	const char *config = NULL;

	arg = (THREAD_ARGS *)args;
	conn = arg->testopts->conn;
	__wt_random_init_seed(NULL, &rnd);
	(void)time(&start);
	(void)time(&now);

	while (difftime(now, start) < RUNTIME) {
		switch (__wt_random(&rnd) % 6) {
			case 0:
				op_bulk(conn, config);
				break;
			case 1:
				op_create(conn, config);
				break;
			case 2:
				op_cursor(conn);
				break;
			case 3:
				op_drop(conn, __wt_random(&rnd) & 1);
				break;
			case 4:
				op_bulk_unique(conn, config,
				    __wt_random(&rnd) & 1, arg->threadnum);
				break;
			case 5:
				op_create_unique(conn, config,
				    __wt_random(&rnd) & 1, arg->threadnum);
				break;
		}
		/* Increment how many ops this thread has performed */
		__wt_atomic_add64(&thread_counters[arg->threadnum], 1);
		(void)time(&now);
	}

	return (NULL);
}

/*
 * There are 6 operations below. These are taken originally from the operations
 * we do in test/fops and modified somewhat bit to avoid blocking states.
 * The operations borrowed from fops are:
 * - op_bulk
 * - op_bulk_unique
 * - op_cursor
 * - op_create
 * - op_create_unique
 * - op_drop
 */

void
op_bulk(WT_CONNECTION *conn, const char *config)
{
	WT_CURSOR *c;
	WT_SESSION *session;
	int ret;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->create(session, uri, config)) != 0)
		if (ret != EEXIST && ret != EBUSY)
			testutil_die(ret, "session.create");

	if (ret == 0) {
		__wt_yield();
		if ((ret = session->open_cursor(
		  session, uri, NULL, "bulk,checkpoint_wait=false", &c)) == 0) {
			if ((ret = c->close(c)) != 0)
				testutil_die(ret, "cursor.close");
		} else if (ret != ENOENT && ret != EBUSY && ret != EINVAL)
			testutil_die(ret, "session.open_cursor bulk");
	}
	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}

void
op_bulk_unique(WT_CONNECTION *conn, const char *config, int force, int tid)
{
	WT_CURSOR *c;
	WT_SESSION *session;
	int ret;
	char new_uri[64];

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	/* Generate a unique object name. */
	if ((ret = pthread_rwlock_wrlock(&single)) != 0)
		testutil_die(ret, "pthread_rwlock_wrlock single");
	testutil_check(__wt_snprintf(
	  new_uri, sizeof(new_uri), "%s.%u", uri, __wt_atomic_add64(&uid,1)));
	if ((ret = pthread_rwlock_unlock(&single)) != 0)
		testutil_die(ret, "pthread_rwlock_unlock single");

	if ((ret = session->create(session, new_uri, config)) != 0)
		testutil_die(ret, "session.create: %s", new_uri);

	__wt_yield();
	/*
	 * Opening a bulk cursor may have raced with a forced checkpoint
	 * which created a checkpoint of the empty file, and triggers an EINVAL
	 */
	if ((ret = session->open_cursor(
	  session, new_uri, NULL, "bulk,checkpoint_wait=false", &c)) == 0) {
		if ((ret = c->close(c)) != 0)
			testutil_die(ret, "cursor.close");
	} else if (ret != EINVAL && ret != EBUSY)
		testutil_die(ret,
		  "session.open_cursor bulk unique: %s", new_uri);

	while ((ret = session->drop(session, new_uri, force ?
	    "force,checkpoint_wait=false" : "checkpoint_wait=false")) != 0)
		if (ret != EBUSY)
			testutil_die(ret, "session.drop: %s", new_uri);
		else
			/*
			 * The EBUSY is expected when we run with
			 * checkpoint_wait set to false, so we increment the
			 * counter while in this loop to avoid false positives.
			 */
			__wt_atomic_add64(&thread_counters[tid], 1);

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}

void
op_cursor(WT_CONNECTION *conn)
{
	WT_SESSION *session;
	WT_CURSOR *cursor;
	int ret;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret =
	  session->open_cursor(session, uri, NULL, NULL, &cursor)) != 0) {
		if (ret != ENOENT && ret != EBUSY)
			testutil_die(ret, "session.open_cursor");
	} else {
		if ((ret = cursor->close(cursor)) != 0)
			testutil_die(ret, "cursor.close");
	}
	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}

void
op_create(WT_CONNECTION *conn, const char *config)
{
	WT_SESSION *session;
	int ret;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->create(session, uri, config)) != 0)
		if (ret != EEXIST && ret != EBUSY)
			testutil_die(ret, "session.create");

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}

void
op_create_unique(WT_CONNECTION *conn, const char *config, int force, int tid)
{
	WT_SESSION *session;
	int ret;
	char new_uri[64];

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	/* Generate a unique object name. */
	if ((ret = pthread_rwlock_wrlock(&single)) != 0)
		testutil_die(ret, "pthread_rwlock_wrlock single");
	testutil_check(__wt_snprintf(
	  new_uri, sizeof(new_uri), "%s.%u", uri, __wt_atomic_add64(&uid, 1)));
	if ((ret = pthread_rwlock_unlock(&single)) != 0)
		testutil_die(ret, "pthread_rwlock_unlock single");

	if ((ret = session->create(session, new_uri, config)) != 0)
		testutil_die(ret, "session.create");

	__wt_yield();
	while ((ret = session->drop(session, new_uri,force ?
	    "force,checkpoint_wait=false" : "checkpoint_wait=false")) != 0)
		if (ret != EBUSY)
			testutil_die(ret, "session.drop: %s", new_uri);
		else
			/*
			 * The EBUSY is expected when we run with
			 * checkpoint_wait set to false, so we increment the
			 * counter while in this loop to avoid false positives.
			 */
			__wt_atomic_add64(&thread_counters[tid], 1);

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}

void
op_drop(WT_CONNECTION *conn, int force)
{
	WT_SESSION *session;
	int ret;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->drop(session, uri, force ?
	    "force,checkpoint_wait=false" : "checkpoint_wait=false")) != 0)
		if (ret != ENOENT && ret != EBUSY)
			testutil_die(ret, "session.drop");

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");
}
