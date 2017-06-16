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
 * A thread dedicated to appending records into a table. Works with fixed
 * length column stores and variable length column stores.
 * One thread (the first thread created by an application) checks for a
 * terminating condition after each insert.
 */
void *
thread_append(void *arg)
{
	TEST_OPTS *opts;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	uint64_t id, recno;
	char buf[64];

	opts = (TEST_OPTS *)arg;
	conn = opts->conn;

	id = __wt_atomic_fetch_addv64(&opts->next_threadid, 1);
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, "append", &cursor));

	buf[0] = '\2';
	for (recno = 1; opts->running; ++recno) {
		if (opts->table_type == TABLE_FIX)
			cursor->set_value(cursor, buf[0]);
		else {
			testutil_check(__wt_snprintf(buf, sizeof(buf),
			    "%" PRIu64 " VALUE ------", recno));
			cursor->set_value(cursor, buf);
		}
		testutil_check(cursor->insert(cursor));
		if (id == 0) {
			testutil_check(
			    cursor->get_key(cursor, &opts->max_inserted_id));
			if (opts->max_inserted_id >= opts->nrecords)
				opts->running = false;
		}
	}

	return (NULL);
}

/*
 * Append into a row store table.
 */
void *
thread_insert_append(void *arg)
{
	TEST_OPTS *opts;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	uint64_t i;
	char kbuf[64];

	opts = (TEST_OPTS *)arg;
	conn = opts->conn;

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(
	    session, opts->uri, NULL, NULL, &cursor));

	for (i = 0; i < opts->nrecords; ++i) {
		testutil_check(__wt_snprintf(
		    kbuf, sizeof(kbuf), "%010d KEY------", (int)i));
		cursor->set_key(cursor, kbuf);
		cursor->set_value(cursor, "========== VALUE =======");
		testutil_check(cursor->insert(cursor));
		if (i % 100000 == 0) {
			printf("insert: %" PRIu64 "\r", i);
			fflush(stdout);
		}
	}
	printf("\n");

	opts->running = false;

	return (NULL);
}

/*
 * Repeatedly walk backwards through the records in a table.
 */
void *
thread_prev(void *arg)
{
	TEST_OPTS *opts;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	int ret;

	opts = (TEST_OPTS *)arg;
	ret = 0;

	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &session));
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, NULL, &cursor));
	while (opts->running) {
		while (opts->running && (ret = cursor->prev(cursor)) == 0)
			;
		if (ret == WT_NOTFOUND)
			ret = 0;
		testutil_check(ret);
	}

	testutil_check(session->close(session, NULL));
	return (NULL);
}

/*
 *
 */

/*
 * Create a table and open a bulk cursor on it.
 */
void *
op_bulk(void *arg)
{
	TEST_OPTS *opts;
	PER_THREAD_ARGS *args;
	WT_CONNECTION *conn;
	WT_CURSOR *c;
	WT_SESSION *session;
	int ret;
	char *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->create(session, uri, NULL)) != 0)
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

	return (NULL);
}

/*
 * Create a guaranteed unique table and open and close a bulk cursor on it.
 */
void *
op_bulk_unique(void *arg)
{
	TEST_OPTS *opts;
	PER_THREAD_ARGS *args;
	WT_CONNECTION *conn;
	WT_CURSOR *c;
	WT_SESSION *session;
	int force, ret;
	char new_uri[64], *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;
	force = __wt_random(&args->s_args->rnd) & 1;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	/* Generate a unique object name. */
	if ((ret = pthread_rwlock_wrlock(&args->s_args->lock)) != 0)
		testutil_die(ret, "pthread_rwlock_wrlock lock");
	testutil_check(__wt_snprintf(
	    new_uri, sizeof(new_uri), "%s.%u",
	    uri, __wt_atomic_add64(&args->s_args->uid, 1)));
	if ((ret = pthread_rwlock_unlock(&args->s_args->lock)) != 0)
		testutil_die(ret, "pthread_rwlock_unlock lock");

	if ((ret = session->create(session, new_uri, NULL)) != 0)
		testutil_die(ret, "session.create: %s", new_uri);

	__wt_yield();
	/*
	 * Opening a bulk cursor may have raced with a forced checkpoint
	 * which created a checkpoint of the empty file, and triggers an EINVAL.
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
			args->thread_counter++;

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");

	return (NULL);
}

/*
 * Open and close cursor on a table.
 */
void *
op_cursor(void *arg)
{
	TEST_OPTS *opts;
	PER_THREAD_ARGS *args;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	int ret;
	char *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;

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

	return (NULL);
}

/*
 * Create a table.
 */
void *
op_create(void *arg)
{
	TEST_OPTS *opts;
	PER_THREAD_ARGS *args;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int ret;
	char *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->create(session, uri, NULL)) != 0)
		if (ret != EEXIST && ret != EBUSY)
			testutil_die(ret, "session.create");

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");

	return (NULL);
}

/*
 * Create and drop a unique guaranteed table.
 */
void *
op_create_unique(void *arg)
{
	TEST_OPTS *opts;
	PER_THREAD_ARGS *args;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int force, ret;
	char new_uri[64], *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;
	force = __wt_random(&args->s_args->rnd) & 1;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	/* Generate a unique object name. */
	if ((ret = pthread_rwlock_wrlock(&args->s_args->lock)) != 0)
		testutil_die(ret, "pthread_rwlock_wrlock lock");
	testutil_check(__wt_snprintf(
	    new_uri, sizeof(new_uri), "%s.%u",
	    uri, __wt_atomic_add64(&args->s_args->uid, 1)));
	if ((ret = pthread_rwlock_unlock(&args->s_args->lock)) != 0)
		testutil_die(ret, "pthread_rwlock_unlock lock");

	if ((ret = session->create(session, new_uri, NULL)) != 0)
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
			args->thread_counter++;

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");

	return (NULL);
}

/*
 * Drop a table.
 */
void *
op_drop(void *arg)
{
	PER_THREAD_ARGS *args;
	TEST_OPTS *opts;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int force, ret;
	char *uri;

	args = (PER_THREAD_ARGS *)arg;
	opts = args->testopts;
	conn = opts->conn;
	uri = args->s_args->uri;
	force = __wt_random(&args->s_args->rnd) & 1;

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		testutil_die(ret, "conn.session");

	if ((ret = session->drop(session, uri, force ?
	    "force,checkpoint_wait=false" : "checkpoint_wait=false")) != 0)
		if (ret != ENOENT && ret != EBUSY)
			testutil_die(ret, "session.drop");

	if ((ret = session->close(session, NULL)) != 0)
		testutil_die(ret, "session.close");

	return (NULL);
}
