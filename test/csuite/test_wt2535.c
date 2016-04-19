#include "test_util.h"

void (*custom_die)(void) = NULL;

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
		printf("WARNING: didn't get expected number of changes\n");
		printf("got: %d, expected: %d\n",
		    (int)current_value, (int)(opts->nthreads * opts->nrecords));
	}
	testutil_check(session->close(session, NULL));
	ce = clock();
	printf("%" PRIu64 ": %.2lf\n",
	    opts->nrecords, (ce - cs) / (double)CLOCKS_PER_SEC);

	testutil_cleanup(opts);
	return (0);
}
