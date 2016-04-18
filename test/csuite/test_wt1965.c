#include "test_util.i"

void (*custom_die)(void) = NULL;

#define	NR_OBJECTS 100
#define	NR_FIELDS 8
#define	NR_THREADS 4
#define	BUF_SIZE 256

static uint64_t g_ts;

static void *thread_func(void *arg)
{
	TEST_OPTS *opts;
	int i, o, r;
	WT_SESSION *session;
	WT_CURSOR *cursor, *idx_cursor;
	uint64_t thr_idx;
	uint64_t *obj_data;
	uint64_t ts = g_ts;

	opts = (TEST_OPTS *)arg;
	thr_idx = __wt_atomic_fetch_addv64(&opts->next_threadid, 1);
	obj_data = calloc(
	    (NR_OBJECTS/NR_THREADS + 1) * NR_FIELDS, sizeof(*obj_data));

	testutil_check(opts->conn->open_session(
	    opts->conn, NULL, NULL, &session));

	testutil_check(session->open_cursor(
	    session, opts->uri, NULL, NULL, &cursor));
	testutil_check(session->open_cursor(
	    session, "table:index", NULL, NULL, &idx_cursor));

	for (r = 1; r < 10; ++r) {
		for (o = thr_idx, i = 0;
		    o < NR_OBJECTS; o += NR_THREADS, i += NR_FIELDS) {

			testutil_check(
			    session->begin_transaction(session, "sync=false"));

			cursor->set_key(cursor, ((uint64_t)o) << 40 | r);
			cursor->set_value(cursor, ts,
			    obj_data[i+0], obj_data[i+1], obj_data[i+2],
			    obj_data[i+3], obj_data[i+4], obj_data[i+5],
			    obj_data[i+6], obj_data[i+7]);
			testutil_check(cursor->insert(cursor));

			idx_cursor->set_key(
			    idx_cursor, ((uint64_t)o) << 40 | ts);
			idx_cursor->set_value(idx_cursor, r);
			testutil_check(idx_cursor->insert(idx_cursor));

			testutil_check(
			    session->commit_transaction(session, NULL));

			/* change object fields */
			++obj_data[i + ((o + r) % NR_FIELDS)];
			++obj_data[i + ((o + r + 1) % NR_FIELDS)];

			++g_ts;
			/* 5K updates/sec */
			usleep(1000000ULL * NR_THREADS / 5000);
		}
	}

	testutil_check(session->close(session, NULL));
	free(obj_data);
	return (NULL);
}

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	char table_format[256];
	int i, ret;
	size_t t;
	uint64_t r, ts, f[NR_FIELDS];
	pthread_t thr[NR_THREADS];

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(wiredtiger_open(opts->home, NULL,
	    "create,cache_size=1G,checkpoint=(wait=30),"
	    "eviction_trigger=80,eviction_target=64,eviction_dirty_target=65,"
	    "log=(enabled,file_max=10M),"
	    "transaction_sync=(enabled=true,method=none)", &opts->conn));
	testutil_check(opts->conn->open_session(
	    opts->conn, NULL, NULL, &session));

	sprintf(table_format, "key_format=r,value_format=");
	for (i = 0; i < NR_FIELDS; i++)
		strcat(table_format, "Q");

	/* recno -> timestamp + NR_FIELDS * Q */
	testutil_check(session->create(
	    session, opts->uri, table_format));
	/* timestamp -> recno */
	testutil_check(session->create(session,
	    "table:index", "key_format=Q,value_format=Q"));

	testutil_check(session->close(session, NULL));

	for (t = 0; t < NR_THREADS; ++t)
		testutil_check(pthread_create(
		    &thr[t], NULL, thread_func, (void *)opts));

	for (t = 0; t < NR_THREADS; ++t)
		pthread_join(thr[t], NULL);

	testutil_check(opts->conn->open_session(
	    opts->conn, NULL, NULL, &session));

	/* recno -> timestamp + NR_FIELDS * Q */
	testutil_check(session->create(session, opts->uri, table_format));

	testutil_check(session->open_cursor(
	    session, opts->uri, NULL, NULL, &cursor));

	while ((ret = cursor->next(cursor)) == 0) {
		testutil_check(cursor->get_key(cursor, &r));
		testutil_check(cursor->get_value(cursor, &ts,
		    &f[0], &f[1], &f[2], &f[3], &f[4], &f[5], &f[6], &f[7]));

		if (!opts->verbose)
			continue;

		printf("(%" PRIu64 ",%llu)\t\t%" PRIu64,
		    (r >> 40), r & ((1ULL << 40) - 1), ts);

		for (i = 0; i < NR_FIELDS; i++)
			printf("\t%" PRIu64, f[i]);
		printf("\n");
	}

	testutil_cleanup(opts);

	return (0);
}
