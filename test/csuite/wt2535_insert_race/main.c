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

/*
 * JIRA ticket reference: WT-2535
 * Test case description: This is a test case that looks for lost updates to
 * a single record. That is multiple threads each do the same number of read
 * modify write operations on a single record. At the end verify that the
 * data contains the expected value.
 * Failure mode: Check that the data is correct at the end of the run.
 */

void (*custom_die)(void) = NULL;

void *thread_insert_race(void *);

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	WT_CURSOR *c;
	WT_SESSION *session;
	clock_t ce, cs;
	pthread_t id[100];
	int i;
	uint64_t current_value;

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	opts->nthreads = 10;
	opts->nrecords = 1000;
	opts->table_type = TABLE_ROW;
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(wiredtiger_open(opts->home, NULL,
	    "create,"
	    "cache_size=2G,"
	    "eviction=(threads_max=5),"
	    "statistics=(fast)", &opts->conn));
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &session));
	testutil_check(session->create(session, opts->uri,
	    "key_format=Q,value_format=Q,"
	    "leaf_page_max=32k,"));

	/* Create the single record. */
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, NULL, &c));
	c->set_key(c, 1);
	c->set_value(c, 0);
	testutil_check(c->insert(c));
	testutil_check(c->close(c));
	cs = clock();
	for (i = 0; i < (int)opts->nthreads; ++i) {
		testutil_check(pthread_create(
		    &id[i], NULL, thread_insert_race, (void *)opts));
	}
	while (--i >= 0)
		testutil_check(pthread_join(id[i], NULL));
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, NULL, &c));
	c->set_key(c, 1);
	testutil_check(c->search(c));
	c->get_value(c, &current_value);
	if (current_value != opts->nthreads * opts->nrecords) {
		fprintf(stderr,
		    "ERROR: didn't get expected number of changes\n");
		fprintf(stderr, "got: %d, expected: %d\n",
		    (int)current_value, (int)(opts->nthreads * opts->nrecords));
		return (EXIT_FAILURE);
	}
	testutil_check(session->close(session, NULL));
	ce = clock();
	printf("%" PRIu64 ": %.2lf\n",
	    opts->nrecords, (ce - cs) / (double)CLOCKS_PER_SEC);

	testutil_cleanup(opts);
	return (EXIT_SUCCESS);
}

/*
 * Append to a table in a "racy" fashion - that is attempt to insert the
 * same record another thread is likely to also be inserting.
 */
void *
thread_insert_race(void *arg)
{
	TEST_OPTS *opts;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	int ret;
	uint64_t i, value;

	opts = (TEST_OPTS *)arg;
	conn = opts->conn;

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(
	    session, opts->uri, NULL, NULL, &cursor));

	printf("Running insert thread\n");
	for (i = 0; i < opts->nrecords; ++i) {
		testutil_check(
		    session->begin_transaction(session, "isolation=snapshot"));
		cursor->set_key(cursor, 1);
		cursor->search(cursor);
		cursor->get_value(cursor, &value);
		cursor->set_key(cursor, 1);
		cursor->set_value(cursor, value + 1);
		if ((ret = cursor->update(cursor)) != 0) {
			if (ret == WT_ROLLBACK) {
				testutil_check(session->rollback_transaction(
				    session, NULL));
				i--;
				continue;
			}
			printf("Error in update: %d\n", ret);
		}
		testutil_check(session->commit_transaction(session, NULL));
		if (i % 10000 == 0) {
			printf("insert: %d\r", (int)i);
			fflush(stdout);
		}
	}
	if (i > 10000)
		printf("\n");

	opts->running = false;

	return (NULL);
}
