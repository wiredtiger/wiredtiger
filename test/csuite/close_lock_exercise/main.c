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
#include "test_util.h"

void (*custom_die)(void) = NULL;

#define	NR_THREADS 10
#define	NR_RUNS 1000

static void
*sweep_work_thread(void *arg) {
	TEST_OPTS *opts;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	uint64_t id, ret;
	size_t i;
	char uri[20];

	opts = (TEST_OPTS *)arg;
	id = __wt_atomic_fetch_addv64(&opts->next_threadid, 1);
	snprintf(uri, 20, "table:test%" PRIu64, id);

	testutil_check(opts->conn->open_session(
	    opts->conn, NULL, NULL, &session));

	testutil_check(session->create(session,
	    uri, "key_format=Q,value_format=Q"));
	testutil_check(
	    session->open_cursor(session, uri, NULL, "append", &cursor));
	for (i = 0; i < 10; ++i) {
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, i);
		testutil_check(cursor->insert(cursor));
	}
	testutil_check(cursor->close(cursor));

	// Create a table and let everyone rush to delete it
	for (i = 0; i < NR_RUNS; ++i) {
		testutil_check(
		    session->open_cursor(session, uri, NULL, NULL, &cursor));
		cursor->set_key(cursor, i%10);
		cursor->set_value(cursor, i+1);
		testutil_check(cursor->update(cursor));
		testutil_check(cursor->close(cursor));
		sleep(2);
		ret = session->verify(session, uri, NULL);
		if (ret !=0)
			printf("ret is %" PRIu64 " on %s\n", ret, uri);
		sleep(1);
	}
	testutil_check(session->close(session, NULL));
	return (0);
}

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	pthread_t thr[NR_THREADS];
	size_t t;

	opts = &_opts;

	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(wiredtiger_open(opts->home, NULL,
	    "create,cache_size=1G,checkpoint=(wait=30),"
	    "file_manager=(close_handle_minimum=1,close_idle_time=1,"
	    "close_scan_interval=1)", &opts->conn));

	for (t = 0; t < NR_THREADS; ++t)
		testutil_check(pthread_create(
		    &thr[t], NULL, sweep_work_thread, (void *)opts));

	for (t = 0; t < NR_THREADS; ++t)
		(void)pthread_join(thr[t], NULL);

	testutil_cleanup(opts);

	return (0);
}
