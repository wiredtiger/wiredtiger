#include "test_util.h"

/* Don't move into shared function there is a cross platform solution */
#include <signal.h>

#define	MILLION		1000000

void (*custom_die)(void) = NULL;

/* Needs to be global for signal handling. */
TEST_OPTS *opts;

static void
page_init(uint64_t n)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	uint64_t recno, vrecno;
	char buf[64];

	conn = opts->conn;

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, "append", &cursor));

	vrecno = 0;
	buf[0] = '\2';
	for (recno = 1;; ++recno) {
		if (opts->table_type == TABLE_FIX)
			cursor->set_value(cursor, buf[0]);
		else {
			if (recno % 3 == 0)
				++vrecno;
			snprintf(buf,
			    sizeof(buf), "%" PRIu64 " VALUE ------", vrecno);
			cursor->set_value(cursor, buf);
		}
		testutil_check(cursor->insert(cursor));
		testutil_check(cursor->get_key(cursor, &opts->max_inserted_id));
		if (opts->max_inserted_id >= n)
			break;
	}
}

/*
 * TODO: Platform specific?
 */
static void
onsig(int signo)
{
	WT_UNUSED(signo);
	opts->running = false;
}

#define	N_APPEND_THREADS	6

int
main(int argc, char *argv[])
{
	TEST_OPTS _opts;
	WT_SESSION *session;
	clock_t ce, cs;
	pthread_t idlist[100];
	uint64_t i, id;
	char buf[100];

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	opts->table_type = TABLE_ROW;
	opts->n_append_threads = N_APPEND_THREADS;
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	snprintf(buf, sizeof(buf), 
	    "create,"
	    "cache_size=%s,"
	    "eviction=(threads_max=5),"
	    "statistics=(fast)",
	    opts->table_type == TABLE_FIX ? "500MB" : "2GB");
	testutil_check(wiredtiger_open(opts->home, NULL, buf, &opts->conn));
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &session));
	snprintf(buf, sizeof(buf),
	    "key_format=r,value_format=%s,"
	    "allocation_size=4K,leaf_page_max=64K",
	    opts->table_type == TABLE_FIX ? "8t" : "S");
	testutil_check(session->create(session, opts->uri, buf));
	testutil_check(session->close(session, NULL));

	page_init(5000);

	/* Force to disk and re-open. */
	testutil_check(opts->conn->close(opts->conn, NULL));
	testutil_check(wiredtiger_open(opts->home, NULL, NULL, &opts->conn));

	(void)signal(SIGINT, onsig);

	cs = clock();
	id = 0;
	for (i = 0; i < opts->n_append_threads; ++i, ++id) {
		printf("append: %" PRIu64 "\n", id);
		testutil_check(pthread_create(
		    &idlist[id], NULL, thread_append, (void *)opts));
	}

#if 0
	while (opts->running) {
		for (i = 0; i < 5; ++i) {
			if (!opts->running)
				break;
			sleep(1);
		}
	}
#endif
	for (i = 0; i < id; ++i)
		testutil_check(pthread_join(idlist[i], NULL));

	ce = clock();
	printf("%" PRIu64 "M: %.2lf\n",
	    opts->max_inserted_id / MILLION,
	    (ce - cs) / (double)CLOCKS_PER_SEC);

	testutil_cleanup(opts);
	/* NOTREACHED */

	return (0);
}
