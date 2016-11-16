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

#include <wiredtiger.h>
#include "wt_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/wait.h>
#include <signal.h>

static char home[512];			/* Program working dir */
static const char *progname;		/* Program name */
static const char * const uri = "table:main";
static int inmem;

#define	MAX_TH	12
#define	MIN_TH	5
#define	MAX_TIME	40
#define	MIN_TIME	10
#define	RECORDS_FILE	"records-%" PRIu32

#define	ENV_CONFIG_DEF						\
    "create,log=(file_max=10M,archive=false,enabled),"		\
    "transaction_sync=(enabled=false,method=none)"
#define	ENV_CONFIG_TXNSYNC					\
    "create,log=(file_max=10M,archive=false,enabled),"		\
    "transaction_sync=(enabled,method=none)"
#define	ENV_CONFIG_REC "log=(recover=on)"
#define	MAX_VAL	4096

static void
usage(void)
{
	fprintf(stderr, "usage: %s [-h dir] [-T threads]\n", progname);
	exit(EXIT_FAILURE);
}

static void
die(int e, const char *m)
{
	fprintf(stderr, "%s: %s: %s\n", progname, m, wiredtiger_strerror(e));
	exit(EXIT_FAILURE);
}

typedef struct {
	WT_CONNECTION *conn;
	uint64_t start;
	uint32_t id;
} WT_THREAD_DATA;

static void *
thread_run(void *arg)
{
	FILE *fp;
	WT_CURSOR *cursor;
	WT_ITEM data;
	WT_RAND_STATE rnd;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	uint64_t i;
	int ret;
	size_t lsize;
	char buf[MAX_VAL], kname[64], lgbuf[8];
	char large[128*1024];

	__wt_random_init(&rnd);
	memset(buf, 0, sizeof(buf));
	memset(kname, 0, sizeof(kname));
	lsize = sizeof(large);
	memset(large, 0, lsize);

	td = (WT_THREAD_DATA *)arg;
	/*
	 * The value is the name of the record file with our id appended.
	 */
	snprintf(buf, sizeof(buf), RECORDS_FILE, td->id);
	/*
	 * Set up a large value putting our id in it.  Write it in there a
	 * bunch of times, but the rest of the buffer can just be zero.
	 */
	snprintf(lgbuf, sizeof(lgbuf), "th-%" PRIu32, td->id);
	for (i = 0; i < 128; i += strlen(lgbuf))
		snprintf(&large[i], lsize - i, "%s", lgbuf);
	/*
	 * Keep a separate file with the records we wrote for checking.
	 */
	(void)unlink(buf);
	if ((fp = fopen(buf, "w")) == NULL)
		die(errno, "fopen");
	/*
	 * Set to line buffering.  But that is advisory only.  We've seen
	 * cases where the result files end up with partial lines.
	 */
	(void)setvbuf(fp, NULL, _IOLBF, 1024);
	if ((ret = td->conn->open_session(td->conn, NULL, NULL, &session)) != 0)
		die(ret, "WT_CONNECTION:open_session");
	if ((ret =
	    session->open_cursor(session, uri, NULL, NULL, &cursor)) != 0)
		die(ret, "WT_SESSION.open_cursor");
	data.data = buf;
	data.size = sizeof(buf);
	/*
	 * Write our portion of the key space until we're killed.
	 */
	for (i = td->start; ; ++i) {
		snprintf(kname, sizeof(kname), "%" PRIu64, i);
		cursor->set_key(cursor, kname);
		/*
		 * Every 30th record write a very large record that exceeds the
		 * log buffer size.  This forces us to use the unbuffered path.
		 */
		if (i % 30 == 0) {
			data.size = 128 * 1024;
			data.data = large;
		} else {
			data.size = __wt_random(&rnd) % MAX_VAL;
			data.data = buf;
		}
		cursor->set_value(cursor, &data);
		if ((ret = cursor->insert(cursor)) != 0)
			die(ret, "WT_CURSOR.insert");
		/*
		 * Save the key separately for checking later.
		 */
		if (fprintf(fp, "%" PRIu64 "\n", i) == -1)
			die(errno, "fprintf");
	}
	return (NULL);
}

/*
 * Child process creates the database and table, and then creates worker
 * threads to add data until it is killed by the parent.
 */
static void
fill_db(uint32_t nth)
{
	pthread_t *thr;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	uint32_t i;
	int ret;
	const char *envconf;

	thr = calloc(nth, sizeof(pthread_t));
	td = calloc(nth, sizeof(WT_THREAD_DATA));
	if (chdir(home) != 0)
		die(errno, "Child chdir");
	if (inmem)
		envconf = ENV_CONFIG_DEF;
	else
		envconf = ENV_CONFIG_TXNSYNC;
	if ((ret = wiredtiger_open(NULL, NULL, envconf, &conn)) != 0)
		die(ret, "wiredtiger_open");
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "WT_CONNECTION:open_session");
	if ((ret = session->create(session,
	    uri, "key_format=S,value_format=u")) != 0)
		die(ret, "WT_SESSION.create");
	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "WT_SESSION:close");

	printf("Create %" PRIu32 " writer threads\n", nth);
	for (i = 0; i < nth; ++i) {
		td[i].conn = conn;
		td[i].start = (UINT64_MAX / nth) * i;
		td[i].id = i;
		if ((ret = pthread_create(
		    &thr[i], NULL, thread_run, &td[i])) != 0)
			die(ret, "pthread_create");
	}
	printf("Spawned %" PRIu32 " writer threads\n", nth);
	fflush(stdout);
	/*
	 * The threads never exit, so the child will just wait here until
	 * it is killed.
	 */
	for (i = 0; i < nth; ++i)
		(void)pthread_join(thr[i], NULL);
	/*
	 * NOTREACHED
	 */
	free(thr);
	free(td);
	exit(EXIT_SUCCESS);
}

extern int __wt_optind;
extern char *__wt_optarg;

int
main(int argc, char *argv[])
{
	FILE *fp;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	WT_RAND_STATE rnd;
	uint64_t absent, count, key, last_key, middle;
	uint32_t i, nth, timeout;
	int ch, status, ret;
	pid_t pid;
	int fatal, rand_th, rand_time, verify_only;
	const char *working_dir;
	char cmd[1024], fname[64], kname[64];

	if ((progname = strrchr(argv[0], '/')) == NULL)
		progname = argv[0];
	else
		++progname;

	inmem = 0;
	nth = MIN_TH;
	rand_th = rand_time = 1;
	timeout = MIN_TIME;
	verify_only = 0;
	working_dir = "WT_TEST.random-abort";

	while ((ch = __wt_getopt(progname, argc, argv, "h:mT:t:v")) != EOF)
		switch (ch) {
		case 'h':
			working_dir = __wt_optarg;
			break;
		case 'm':
			inmem = 1;
			break;
		case 'T':
			rand_th = 0;
			nth = (uint32_t)atoi(__wt_optarg);
			break;
		case 't':
			rand_time = 0;
			timeout = (uint32_t)atoi(__wt_optarg);
			break;
		case 'v':
			verify_only = 1;
			break;
		default:
			usage();
		}
	argc -= __wt_optind;
	argv += __wt_optind;
	if (argc != 0)
		usage();

	if ((strlen(working_dir) + 1) > 512) {
		fprintf(stderr,
		    "Not enough memory in buffer for directory %s\n",
		    working_dir);
		return (EXIT_FAILURE);
	}
	snprintf(home, 512, "%s", working_dir);
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
		snprintf(cmd, 1024, "rm -rf %s", home);
		if ((ret = system(cmd)) != 0)
			die(ret, "system rm");
		snprintf(cmd, 1024, "mkdir %s", home);
		if ((ret = system(cmd)) != 0)
			die(ret, "system mkdir");

		__wt_random_init(&rnd);
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
		printf("Parent: Create %" PRIu32
		    " threads; sleep %" PRIu32 " seconds\n", nth, timeout);
		/*
		 * Fork a child to insert as many items.  We will then randomly
		 * kill the child, run recovery and make sure all items we wrote
		 * exist after recovery runs.
		 */
		if ((pid = fork()) < 0)
			die(errno, "fork");

		if (pid == 0) { /* child */
			fill_db(nth);
			return (EXIT_SUCCESS);
		}

		/* parent */
		/*
		 * Sleep for the configured amount of time before killing
		 * the child.
		 */
		sleep(timeout);

		/*
		 * !!! It should be plenty long enough to make sure more than
		 * one log file exists.  If wanted, that check would be added
		 * here.
		 */
		printf("Kill child\n");
		if (kill(pid, SIGKILL) != 0)
			die(errno, "kill");
		if (waitpid(pid, &status, 0) == -1)
			die(errno, "waitpid");
	}
	/*
	 * !!! If we wanted to take a copy of the directory before recovery,
	 * this is the place to do it.
	 */
	if (chdir(home) != 0)
		die(errno, "parent chdir");
	printf("Open database, run recovery and verify content\n");
	if ((ret = wiredtiger_open(NULL, NULL, ENV_CONFIG_REC, &conn)) != 0)
		die(ret, "wiredtiger_open");
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "WT_CONNECTION:open_session");
	if ((ret =
	    session->open_cursor(session, uri, NULL, NULL, &cursor)) != 0)
		die(ret, "WT_SESSION.open_cursor");

	absent = count = 0;
	fatal = 0;
	for (i = 0; i < nth; ++i) {
		middle = 0;
		snprintf(fname, sizeof(fname), RECORDS_FILE, i);
		if ((fp = fopen(fname, "r")) == NULL) {
			fprintf(stderr,
			    "Failed to open %s. i %" PRIu32 "\n", fname, i);
			die(errno, "fopen");
		}

		/*
		 * For every key in the saved file, verify that the key exists
		 * in the table after recovery.  If we're doing in-memory
		 * log buffering we never expect a record missing in the middle,
		 * but records may be missing at the end.  If we did
		 * write-no-sync, we expect every key to have been recovered.
		 */
		for (last_key = UINT64_MAX;; ++count, last_key = key) {
			ret = fscanf(fp, "%" SCNu64 "\n", &key);
			if (ret != EOF && ret != 1)
				die(errno, "fscanf");
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
			snprintf(kname, sizeof(kname), "%" PRIu64, key);
			cursor->set_key(cursor, kname);
			if ((ret = cursor->search(cursor)) != 0) {
				if (ret != WT_NOTFOUND)
					die(ret, "search");
				if (!inmem)
					printf("%s: no record with key %"
					    PRIu64 "\n", fname, key);
				absent++;
				middle = key;
			} else if (middle != 0) {
				/*
				 * We should never find an existing key after
				 * we have detected one missing.
				 */
				printf("%s: after absent record at %" PRIu64
				    " key %" PRIu64 " exists\n",
				    fname, middle, key);
				fatal = 1;
			}
		}
		if (fclose(fp) != 0)
			die(errno, "fclose");
	}
	if ((ret = conn->close(conn, NULL)) != 0)
		die(ret, "WT_CONNECTION:close");
	if (fatal)
		return (EXIT_FAILURE);
	if (!inmem && absent) {
		printf("%" PRIu64 " record(s) absent from %" PRIu64 "\n",
		    absent, count);
		return (EXIT_FAILURE);
	}
	printf("%" PRIu64 " records verified\n", count);
	return (EXIT_SUCCESS);
}
