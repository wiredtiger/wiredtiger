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

#include <sys/wait.h>
#include <signal.h>

static char home[1024];			/* Program working dir */

/*
 * Create three tables that we will write the same data to and verify that
 * all the types of usage have the expected data in them after a crash and
 * recovery.  We want:
 * 1. A table that is logged and is not involved in timestamps.  This table
 * simulates a user local table.
 * 2. A table that is logged and involved in timestamps.  This simulates
 * the oplog.
 * 3. A table that is not logged and involved in timestamps.  This simulates
 * a typical collection file.
 *
 * We also create a fourth table that is not logged and not involved directly
 * in timestamps to store the stable timestamp.  That way we can know what the
 * latest stable timestamp is on checkpoint.
 *
 * We also create several files that are not WiredTiger tables.  The checkpoint
 * thread creates a file indicating that a checkpoint has completed.  The parent
 * process uses this to know when at least one checkpoint is done and it can
 * start the timer to abort.
 *
 * Each worker thread creates its own records file that records the data it
 * inserted and it records the timestamp that was used for that insertion.
 */
#define	MAX_CKPT_INVL	5	/* Maximum interval between checkpoints */
#define	MAX_TH		12
#define	MAX_TIME	40
#define	MAX_VAL		1024
#define	MIN_TH		5
#define	MIN_TIME	10
#define	RECORDS_FILE	"records-%" PRIu32
#define	STABLE_PERIOD	100

static const char * const uri_local = "table:local";
static const char * const uri_oplog = "table:oplog";
static const char * const uri_collection = "table:collection";

static const char * const stable_store = "table:stable";
static const char * const ckpt_file = "checkpoint_done";

static bool compat, inmem, use_ts;
static volatile uint64_t global_ts = 1;
static uint64_t th_ts[MAX_TH];

#define	ENV_CONFIG_COMPAT	",compatibility=(release=\"2.9\")"
#define	ENV_CONFIG_DEF						\
    "create,log=(archive=false,file_max=10M,enabled)"
#define	ENV_CONFIG_TXNSYNC					\
    "create,log=(archive=false,file_max=10M,enabled),"			\
    "transaction_sync=(enabled,method=none)"
#define	ENV_CONFIG_REC "log=(archive=false,recover=on)"

static void usage(void)
    WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-h dir] [-T threads] [-t time] [-Cmvz]\n", progname);
	exit(EXIT_FAILURE);
}

typedef struct {
	WT_CONNECTION *conn;
	uint64_t start;
	uint32_t info;
} WT_THREAD_DATA;

/*
 * thread_ts_run --
 *	Runner function for a timestamp thread.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
	WT_CURSOR *cur_stable;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	uint64_t i, last_ts, oldest_ts;
	char tscfg[64];

	td = (WT_THREAD_DATA *)arg;
	last_ts = 0;

	testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(
	    session, stable_store, NULL, NULL, &cur_stable));

	/*
	 * Every N records we will record our stable timestamp into the stable
	 * table. That will define our threshold where we expect to find records
	 * after recovery.
	 */
	for (;;) {
		oldest_ts = UINT64_MAX;
		/*
		 * For the timestamp thread, the info field contains the number
		 * of worker threads.
		 */
		for (i = 0; i < td->info; ++i) {
			/*
			 * We need to let all threads get started, so if we find
			 * any thread still with a zero timestamp we go to
			 * sleep.
			 */
			if (th_ts[i] == 0)
				goto ts_wait;
			if (th_ts[i] != 0 && th_ts[i] < oldest_ts)
				oldest_ts = th_ts[i];
		}

		if (oldest_ts != UINT64_MAX &&
		    oldest_ts - last_ts > STABLE_PERIOD) {
			/*
			 * Set both the oldest and stable timestamp so that we
			 * don't need to maintain read availability at older
			 * timestamps.
			 */
			testutil_check(__wt_snprintf(
			    tscfg, sizeof(tscfg),
			    "oldest_timestamp=%" PRIx64
			    ",stable_timestamp=%" PRIx64,
			    oldest_ts, oldest_ts));
			testutil_check(
			    td->conn->set_timestamp(td->conn, tscfg));
			last_ts = oldest_ts;
			cur_stable->set_key(cur_stable, td->info);
			cur_stable->set_value(cur_stable, oldest_ts);
			testutil_check(cur_stable->insert(cur_stable));
		} else
ts_wait:		__wt_sleep(0, 1000);
	}
	/* NOTREACHED */
}

/*
 * thread_ckpt_run --
 *	Runner function for the checkpoint thread.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
	FILE *fp;
	WT_RAND_STATE rnd;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	uint64_t ts;
	uint32_t sleep_time;
	int i;
	bool first_ckpt;

	__wt_random_init(&rnd);

	td = (WT_THREAD_DATA *)arg;
	/*
	 * Keep a separate file with the records we wrote for checking.
	 */
	(void)unlink(ckpt_file);
	testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
	first_ckpt = true;
	ts = 0;
	for (i = 0; ;++i) {
		sleep_time = __wt_random(&rnd) % MAX_CKPT_INVL;
		sleep(sleep_time);
		if (use_ts)
			ts = global_ts;
		/*
		 * Since this is the default, send in this string even if
		 * running without timestamps.
		 */
		testutil_check(session->checkpoint(
		    session, "use_timestamp=true"));
		printf("Checkpoint %d complete.  Minimum ts %" PRIu64 "\n",
		    i, ts);
		fflush(stdout);
		/*
		 * Create the checkpoint file so that the parent process knows
		 * at least one checkpoint has finished and can start its
		 * timer.
		 */
		if (first_ckpt) {
			testutil_checksys((fp = fopen(ckpt_file, "w")) == NULL);
			first_ckpt = false;
			testutil_checksys(fclose(fp) != 0);
		}
	}
	/* NOTREACHED */
}

/*
 * thread_run --
 *	Runner function for the worker threads.
 */
static WT_THREAD_RET
thread_run(void *arg)
{
	FILE *fp;
	WT_CURSOR *cur_coll, *cur_local, *cur_oplog;
	WT_ITEM data;
	WT_RAND_STATE rnd;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	uint64_t i, stable_ts;
	char cbuf[MAX_VAL], lbuf[MAX_VAL], obuf[MAX_VAL];
	char kname[64], tscfg[64];

	__wt_random_init(&rnd);
	memset(cbuf, 0, sizeof(cbuf));
	memset(lbuf, 0, sizeof(lbuf));
	memset(obuf, 0, sizeof(obuf));
	memset(kname, 0, sizeof(kname));

	td = (WT_THREAD_DATA *)arg;
	/*
	 * Set up the separate file for checking.
	 */
	testutil_check(__wt_snprintf(
	    cbuf, sizeof(cbuf), RECORDS_FILE, td->info));
	(void)unlink(cbuf);
	testutil_checksys((fp = fopen(cbuf, "w")) == NULL);
	/*
	 * Set to line buffering.  But that is advisory only.  We've seen
	 * cases where the result files end up with partial lines.
	 */
	__wt_stream_set_line_buffer(fp);
	testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
	/*
	 * Open a cursor to each table.
	 */
	testutil_check(session->open_cursor(session,
	    uri_collection, NULL, NULL, &cur_coll));
	testutil_check(session->open_cursor(session,
	    uri_local, NULL, NULL, &cur_local));
	testutil_check(session->open_cursor(session,
	    uri_oplog, NULL, NULL, &cur_oplog));

	/*
	 * Write our portion of the key space until we're killed.
	 */
	printf("Thread %" PRIu32 " starts at %" PRIu64 "\n",
	    td->info, td->start);
	stable_ts = 0;
	for (i = td->start;; ++i) {
		if (use_ts)
			stable_ts = __wt_atomic_addv64(&global_ts, 1);
		testutil_check(__wt_snprintf(
		    kname, sizeof(kname), "%" PRIu64, i));

		testutil_check(session->begin_transaction(session, NULL));
		cur_coll->set_key(cur_coll, kname);
		cur_local->set_key(cur_local, kname);
		cur_oplog->set_key(cur_oplog, kname);
		/*
		 * Put an informative string into the value so that it
		 * can be viewed well in a binary dump.
		 */
		testutil_check(__wt_snprintf(cbuf, sizeof(cbuf),
		    "COLL: thread:%" PRIu64 " ts:%" PRIu64 " key: %" PRIu64,
		    td->info, stable_ts, i));
		testutil_check(__wt_snprintf(lbuf, sizeof(lbuf),
		    "LOCAL: thread:%" PRIu64 " ts:%" PRIu64 " key: %" PRIu64,
		    td->info, stable_ts, i));
		testutil_check(__wt_snprintf(obuf, sizeof(obuf),
		    "OPLOG: thread:%" PRIu64 " ts:%" PRIu64 " key: %" PRIu64,
		    td->info, stable_ts, i));
		data.size = __wt_random(&rnd) % MAX_VAL;
		data.data = cbuf;
		cur_coll->set_value(cur_coll, &data);
		testutil_check(cur_coll->insert(cur_coll));
		data.size = __wt_random(&rnd) % MAX_VAL;
		data.data = obuf;
		cur_oplog->set_value(cur_oplog, &data);
		testutil_check(cur_oplog->insert(cur_oplog));
		if (use_ts) {
			testutil_check(__wt_snprintf(tscfg, sizeof(tscfg),
			    "commit_timestamp=%" PRIx64, stable_ts));
			testutil_check(
			    session->commit_transaction(session, tscfg));
			th_ts[td->info] = stable_ts;
		} else
			testutil_check(
			    session->commit_transaction(session, NULL));
		/*
		 * Insert into the local table outside the timestamp txn.
		 */
		data.size = __wt_random(&rnd) % MAX_VAL;
		data.data = lbuf;
		cur_local->set_value(cur_local, &data);
		testutil_check(cur_local->insert(cur_local));

		/*
		 * Save the timestamp and key separately for checking later.
		 */
		if (fprintf(fp,
		    "%" PRIu64 " %" PRIu64 "\n", stable_ts, i) < 0)
			testutil_die(EIO, "fprintf");
	}
	/* NOTREACHED */
}

/*
 * Child process creates the database and table, and then creates worker
 * threads to add data until it is killed by the parent.
 */
static void run_workload(uint32_t)
    WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
run_workload(uint32_t nth)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	wt_thread_t *thr;
	uint32_t ckpt_id, i, ts_id;
	char envconf[512];

	thr = dcalloc(nth+2, sizeof(*thr));
	td = dcalloc(nth+2, sizeof(WT_THREAD_DATA));
	if (chdir(home) != 0)
		testutil_die(errno, "Child chdir: %s", home);
	if (inmem)
		strcpy(envconf, ENV_CONFIG_DEF);
	else
		strcpy(envconf, ENV_CONFIG_TXNSYNC);
	if (compat)
		strcat(envconf, ENV_CONFIG_COMPAT);

	testutil_check(wiredtiger_open(NULL, NULL, envconf, &conn));
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	/*
	 * Create all the tables.
	 */
	testutil_check(session->create(session, uri_collection,
		"key_format=S,value_format=u,log=(enabled=false)"));
	testutil_check(session->create(session,
	    uri_local, "key_format=S,value_format=u"));
	testutil_check(session->create(session,
	    uri_oplog, "key_format=S,value_format=u"));
	/*
	 * Don't log the stable timestamp table so that we know what timestamp
	 * was stored at the checkpoint.
	 */
	testutil_check(session->create(session, stable_store,
	    "key_format=Q,value_format=Q,log=(enabled=false)"));
	testutil_check(session->close(session, NULL));

	/*
	 * The checkpoint thread and the timestamp threads are added at the end.
	 */
	ckpt_id = nth;
	td[ckpt_id].conn = conn;
	td[ckpt_id].info = nth;
	printf("Create checkpoint thread\n");
	testutil_check(__wt_thread_create(
	    NULL, &thr[ckpt_id], thread_ckpt_run, &td[ckpt_id]));
	ts_id = nth + 1;
	if (use_ts) {
		td[ts_id].conn = conn;
		td[ts_id].info = nth;
		printf("Create timestamp thread\n");
		testutil_check(__wt_thread_create(
		    NULL, &thr[ts_id], thread_ts_run, &td[ts_id]));
	}
	printf("Create %" PRIu32 " writer threads\n", nth);
	for (i = 0; i < nth; ++i) {
		td[i].conn = conn;
		td[i].start = (UINT64_MAX / nth) * i;
		td[i].info = i;
		testutil_check(__wt_thread_create(
		    NULL, &thr[i], thread_run, &td[i]));
	}
	/*
	 * The threads never exit, so the child will just wait here until
	 * it is killed.
	 */
	fflush(stdout);
	for (i = 0; i <= ts_id; ++i)
		testutil_check(__wt_thread_join(NULL, thr[i]));
	/*
	 * NOTREACHED
	 */
	free(thr);
	free(td);
	exit(EXIT_SUCCESS);
}

/*
 * Determines whether this is a timestamp build or not
 */
static bool
timestamp_build(void)
{
#ifdef HAVE_TIMESTAMPS
	return (true);
#else
	return (false);
#endif
}

extern int __wt_optind;
extern char *__wt_optarg;

int
main(int argc, char *argv[])
{
	struct stat sb;
	FILE *fp;
	WT_CONNECTION *conn;
	WT_CURSOR *cur_coll, *cur_local, *cur_oplog, *cur_stable;
	WT_RAND_STATE rnd;
	WT_SESSION *session;
	pid_t pid;
	uint64_t absent_coll, absent_local, absent_oplog, count, key, last_key;
	uint64_t first_miss, middle_coll, middle_local, middle_oplog;
	uint64_t stable_fp, stable_val, val[MAX_TH+1];
	uint32_t i, nth, timeout;
	int ch, status, ret;
	const char *working_dir;
	char buf[512], fname[64], kname[64], statname[1024];
	bool fatal, rand_th, rand_time, verify_only;

	/* We have nothing to do if this is not a timestamp build */
	if (!timestamp_build())
		return (EXIT_SUCCESS);

	(void)testutil_set_progname(argv);

	compat = inmem = false;
	use_ts = true;
	nth = MIN_TH;
	rand_th = rand_time = true;
	timeout = MIN_TIME;
	verify_only = false;
	working_dir = "WT_TEST.timestamp-abort";

	while ((ch = __wt_getopt(progname, argc, argv, "Ch:mT:t:vz")) != EOF)
		switch (ch) {
		case 'C':
			compat = true;
			break;
		case 'h':
			working_dir = __wt_optarg;
			break;
		case 'm':
			inmem = true;
			break;
		case 'T':
			rand_th = false;
			nth = (uint32_t)atoi(__wt_optarg);
			break;
		case 't':
			rand_time = false;
			timeout = (uint32_t)atoi(__wt_optarg);
			break;
		case 'v':
			verify_only = true;
			break;
		case 'z':
			use_ts = false;
			break;
		default:
			usage();
		}
	argc -= __wt_optind;
	argv += __wt_optind;
	if (argc != 0)
		usage();

	testutil_work_dir_from_path(home, sizeof(home), working_dir);
	/*
	 * If the user wants to verify they need to tell us how many threads
	 * there were so we can find the old record files.
	 */
	if (verify_only && rand_th) {
		fprintf(stderr,
		    "Verify option requires specifying number of threads\n");
		exit (EXIT_FAILURE);
	}
	if (!verify_only) {
		testutil_make_work_dir(home);

		__wt_random_init_seed(NULL, &rnd);
		if (rand_time) {
			timeout = __wt_random(&rnd) % MAX_TIME;
			if (timeout < MIN_TIME)
				timeout = MIN_TIME;
		}
		if (rand_th) {
			nth = __wt_random(&rnd) % MAX_TH;
			if (nth < MIN_TH)
				nth = MIN_TH;
		}
		printf("Parent: compatibility: %s, "
		    "in-mem log sync: %s, timestamp in use: %s\n",
		    compat ? "true" : "false",
		    inmem ? "true" : "false",
		    use_ts ? "true" : "false");
		printf("Parent: Create %" PRIu32
		    " threads; sleep %" PRIu32 " seconds\n", nth, timeout);
		/*
		 * Fork a child to insert as many items.  We will then randomly
		 * kill the child, run recovery and make sure all items we wrote
		 * exist after recovery runs.
		 */
		testutil_checksys((pid = fork()) < 0);

		if (pid == 0) { /* child */
			run_workload(nth);
			return (EXIT_SUCCESS);
		}

		/* parent */
		/*
		 * Sleep for the configured amount of time before killing
		 * the child.  Start the timeout from the time we notice that
		 * the file has been created.  That allows the test to run
		 * correctly on really slow machines.  Verify the process ID
		 * still exists in case the child aborts for some reason we
		 * don't stay in this loop forever.
		 */
		testutil_check(__wt_snprintf(
		    statname, sizeof(statname), "%s/%s", home, ckpt_file));
		while (stat(statname, &sb) != 0 && kill(pid, 0) == 0)
			sleep(1);
		sleep(timeout);

		/*
		 * !!! It should be plenty long enough to make sure more than
		 * one log file exists.  If wanted, that check would be added
		 * here.
		 */
		printf("Kill child\n");
		testutil_checksys(kill(pid, SIGKILL) != 0);
		testutil_checksys(waitpid(pid, &status, 0) == -1);
	}
	/*
	 * !!! If we wanted to take a copy of the directory before recovery,
	 * this is the place to do it.
	 */
	if (chdir(home) != 0)
		testutil_die(errno, "parent chdir: %s", home);
	testutil_check(__wt_snprintf(buf, sizeof(buf),
	    "rm -rf ../%s.SAVE && mkdir ../%s.SAVE && "
	    "cp -p WiredTigerLog.* ../%s.SAVE",
	     home, home, home));
	(void)system(buf);
	printf("Open database, run recovery and verify content\n");

	/*
	 * Open the connection which forces recovery to be run.
	 */
	testutil_check(wiredtiger_open(NULL, NULL, ENV_CONFIG_REC, &conn));
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	/*
	 * Open a cursor on all the tables.
	 */
	testutil_check(session->open_cursor(session,
	    uri_collection, NULL, NULL, &cur_coll));
	testutil_check(session->open_cursor(session,
	    uri_local, NULL, NULL, &cur_local));
	testutil_check(session->open_cursor(session,
	    uri_oplog, NULL, NULL, &cur_oplog));
	testutil_check(session->open_cursor(session,
	    stable_store, NULL, NULL, &cur_stable));

	/*
	 * Find the biggest stable timestamp value that was saved.
	 */
	stable_val = 0;
	memset(val, 0, sizeof(val));
	while (cur_stable->next(cur_stable) == 0) {
		testutil_check(cur_stable->get_key(cur_stable, &key));
		testutil_check(cur_stable->get_value(cur_stable, &val[key]));
		if (val[key] > stable_val)
			stable_val = val[key];

		if (use_ts)
			printf("Stable: key %" PRIu64 " value %" PRIu64 "\n",
			    key, val[key]);
	}
	if (use_ts)
		printf("Got stable_val %" PRIu64 "\n", stable_val);

	count = 0;
	absent_coll = absent_local = absent_oplog = 0;
	fatal = false;
	for (i = 0; i < nth; ++i) {
		first_miss = middle_coll = middle_local = middle_oplog = 0;
		testutil_check(__wt_snprintf(
		    fname, sizeof(fname), RECORDS_FILE, i));
		if ((fp = fopen(fname, "r")) == NULL)
			testutil_die(errno, "fopen: %s", fname);

		/*
		 * For every key in the saved file, verify that the key exists
		 * in the table after recovery.  If we're doing in-memory
		 * log buffering we never expect a record missing in the middle,
		 * but records may be missing at the end.  If we did
		 * write-no-sync, we expect every key to have been recovered.
		 */
		for (last_key = UINT64_MAX;; ++count, last_key = key) {
			ret = fscanf(fp, "%" SCNu64 "%" SCNu64 "\n",
			    &stable_fp, &key);
			if (ret != EOF && ret != 2) {
				/*
				 * If we find a partial line, consider it
				 * like an EOF.
				 */
				if (ret == 1 || ret == 0)
					break;
				testutil_die(errno, "fscanf");
			}
			if (ret == EOF)
				break;
			/*
			 * If we're unlucky, the last line may be a partially
			 * written key at the end that can result in a false
			 * negative error for a missing record.  Detect it.
			 */
			if (last_key != UINT64_MAX && key != last_key + 1) {
				printf("%s: Ignore partial record %" PRIu64
				    " last valid key %" PRIu64 "\n",
				    fname, key, last_key);
				break;
			}
			testutil_check(__wt_snprintf(
			    kname, sizeof(kname), "%" PRIu64, key));
			cur_coll->set_key(cur_coll, kname);
			cur_local->set_key(cur_local, kname);
			cur_oplog->set_key(cur_oplog, kname);
			/*
			 * The collection table should always only have the
			 * data as of the checkpoint.
			 */
			if ((ret = cur_coll->search(cur_coll)) != 0) {
				if (ret != WT_NOTFOUND)
					testutil_die(ret, "search");
				/*
				 * If we don't find a record, the stable
				 * timestamp written to our file better be
				 * larger than the saved one.
				 */
				if (!inmem &&
				    stable_fp != 0 && stable_fp <= val[i]) {
					printf("%s: COLLECTION no record with "
					    "key %" PRIu64 " record ts %" PRIu64
					    " <= stable ts %" PRIu64 "\n",
					    fname, key, stable_fp, val[i]);
					absent_coll++;
				}
				if (middle_coll == 0)
					first_miss = key;
				middle_coll = key;
			} else if (middle_coll != 0) {
				/*
				 * We should never find an existing key after
				 * we have detected one missing.
				 */
				printf("%s: COLLECTION after absent records %"
				    PRIu64 "-%" PRIu64 " key %" PRIu64
				    " exists\n",
				    fname, first_miss, middle_coll, key);
				fatal = true;
			}
			/*
			 * The local table should always have all data.
			 */
			if ((ret = cur_local->search(cur_local)) != 0) {
				if (ret != WT_NOTFOUND)
					testutil_die(ret, "search");
				if (!inmem)
					printf("%s: LOCAL no record with key %"
					    PRIu64 "\n", fname, key);
				absent_local++;
				middle_local = key;
			} else if (middle_local != 0) {
				/*
				 * We should never find an existing key after
				 * we have detected one missing.
				 */
				printf("%s: LOCAL after absent record at %"
				    PRIu64 " key %" PRIu64 " exists\n",
				    fname, middle_local, key);
				fatal = true;
			}
			/*
			 * The oplog table should always have all data.
			 */
			if ((ret = cur_oplog->search(cur_oplog)) != 0) {
				if (ret != WT_NOTFOUND)
					testutil_die(ret, "search");
				if (!inmem)
					printf("%s: OPLOG no record with key %"
					    PRIu64 "\n", fname, key);
				absent_oplog++;
				middle_oplog = key;
			} else if (middle_oplog != 0) {
				/*
				 * We should never find an existing key after
				 * we have detected one missing.
				 */
				printf("%s: OPLOG after absent record at %"
				    PRIu64 " key %" PRIu64 " exists\n",
				    fname, middle_oplog, key);
				fatal = true;
			}
		}
		testutil_checksys(fclose(fp) != 0);
	}
	testutil_check(conn->close(conn, NULL));
	if (fatal)
		return (EXIT_FAILURE);
	if (!inmem && absent_coll) {
		printf("COLLECTION: %" PRIu64
		    " record(s) absent from %" PRIu64 "\n",
		    absent_coll, count);
		fatal = true;
	}
	if (!inmem && absent_local) {
		printf("LOCAL: %" PRIu64 " record(s) absent from %" PRIu64 "\n",
		    absent_local, count);
		fatal = true;
	}
	if (!inmem && absent_oplog) {
		printf("OPLOG: %" PRIu64 " record(s) absent from %" PRIu64 "\n",
		    absent_oplog, count);
		fatal = true;
	}
	if (fatal)
		return (EXIT_FAILURE);
	printf("%" PRIu64 " records verified\n", count);
	return (EXIT_SUCCESS);
}
