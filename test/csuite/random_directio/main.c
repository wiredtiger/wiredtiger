/*-
 * Public Domain 2014-2018 MongoDB, Inc.
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

/*
 * This test simulates system crashes. It uses direct IO, and currently
 * runs only on Linux.
 *
 * Our strategy is to run a subordinate 'writer' process that creates/modifies
 * data, including schema modifications. Every N seconds, asynchronously, we
 * send a stop signal to the writer and then copy (with direct IO) the entire
 * contents of its database home to a new saved location where we can run and
 * verify the recovered home. Then we send a continue signal. We repeat this:
 *
 *   sleep N, STOP, copy, run recovery, CONTINUE
 *
 * which allows the writer to make continuing progress, while the main
 * process is verifying what's on disk.
 *
 * By using stop signal to suspend the process and copying with direct IO,
 * we are roughly simulating a system crash, by seeing what's actually on
 * disk (not in file system buffer cache) at the moment that the copy is
 * made. It's not quite as harsh as a system crash, as suspending does not
 * halt writes that are in-flight. Still, it's a reasonable proxy for testing.
 *
 * In the main table, the keys look like:
 *
 *   xxxx:T:LARGE_STRING
 *
 * where xxxx represents an increasing decimal id (0 padded to 12 digits).
 * These ids are only unique per thread, so this key is the xxxx-th key
 * written by a thread.  T represents the thread id reduced to a single
 * hex digit. LARGE_STRING is a portion of a large string that includes
 * the thread id and a lot of spaces, over and over (see the large_buf
 * function).  When forming the key, the large string is truncated so
 * that the key is effectively padded to the right length.
 *
 * The key space for the main table is designed to be interleaved tightly
 * among all the threads.  The matching values in the main table are the
 * same, except with the xxxx string reversed.  So the keys and values
 * are the same size.
 *
 * There is also a reverse table where the keys/values are swapped.

 */

#include "test_util.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>

static char home[1024];			/* Program working dir */

/*
 * These two names for the URI and file system must be maintained in tandem.
 */
static const char * const uri_main = "table:main";
static const char * const fs_main = "main.wt";

static const char * const uri_rev = "table:rev";

/*
 * The number of threads cannot be more than 16, we are using a hex digit
 * to encode this in the key.
 */
#define	MAX_TH			16
#define	MIN_TH			5

#define	MAX_TIME		40
#define	MIN_TIME		10

#define	LARGE_WRITE_SIZE	(128*1024)
#define	MIN_DATA_SIZE    	30
#define	DEFAULT_DATA_SIZE	50

#define	DEFAULT_CYCLES		5
#define	DEFAULT_INTERVAL	3

#define	KEY_SEP			"_"		/* Must be one char string */

#define	ENV_CONFIG							\
    "create,log=(file_max=10M,enabled),"				\
    "transaction_sync=(enabled,method=%s)"
#define	ENV_CONFIG_REC "log=(recover=on)"

/* 64 spaces */
#define	SPACES								\
	"                                                                "

/*
 * Set higher to be less stressful for schema operations.
 */
#define	SCHEMA_OP_FREQUENCY	100
#define	SCHEMA_OP(id, offset)						\
	(((offset) == 0 || (id) > (offset)) &&				\
	    (((id) - (offset)) % SCHEMA_OP_FREQUENCY < 10 ||		\
	    ((id) - (offset)) % SCHEMA_OP_FREQUENCY == 0))

extern int __wt_optind;
extern char *__wt_optarg;

static void handler(int);
static void usage(void)
    WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
usage(void)
{
	fprintf(stderr, "usage: %s [options]\n", progname);
	fprintf(stderr, "options:\n"
	    "  -d data_size   \t"
	    "approximate size of keys and values [1000]\n"
	    "  -h home        \t"
	    "WiredTiger home directory [WT_TEST.directio]\n"
	    "  -i interval    \t"
	    "interval timeout between copy/recover cycles [3]\n"
	    "  -m method      \t"
	    "sync method: fsync, dsync, none [none]\n"
	    "  -n num_cycles  \t"
	    "number of copy/recover cycles [5]\n"
	    "  -p             \t"
	    "populate only [false]\n"
	    "  -S             \t"
	    "schema operations on [false]\n"
	    "  -T num_threads \t"
	    "number of threads in writer[ 4]\n"
	    "  -t timeout     \t"
	    "initial timeout before first copy [10]\n"
	    "  -v             \t"
	    "verify only [false]\n");
	exit(EXIT_FAILURE);
}

typedef struct {
	WT_CONNECTION *conn;
	char *data;
	uint32_t datasize;
	uint32_t id;
	bool schema_test;
} WT_THREAD_DATA;

#define	TEST_STREQ(expect, got, message)				\
	do {								\
		if (!WT_STREQ(expect, got)) {				\
			printf("FAIL: %s: expect %s, got %s", message,	\
			    expect, got);				\
			testutil_assert(WT_STREQ(expect, got));		\
		}							\
	} while (0)

/*
 * large_buf --
 *	Fill or check a large buffer.
 */
static void
large_buf(char *large, size_t lsize, uint32_t id, bool fill)
{
	size_t len;
	uint64_t i;
	char lgbuf[1024 + 20];

	/*
	 * Set up a large value putting our id in it every 1024 bytes or so.
	 */
	testutil_check(__wt_snprintf(
	    lgbuf, sizeof(lgbuf), "th-%" PRIu32
		"%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s", id,
		SPACES, SPACES, SPACES, SPACES,
		SPACES, SPACES, SPACES, SPACES,
		SPACES, SPACES, SPACES, SPACES,
		SPACES, SPACES, SPACES, SPACES));

	len = strlen(lgbuf);
	for (i = 0; i < lsize - len; i += len)
		if (fill)
			testutil_check(__wt_snprintf(
			    &large[i], lsize - i, "%s", lgbuf));
		else
			testutil_check(strncmp(&large[i], lgbuf, len));
}

/*
 * reverse --
 *	Reverse a string in place.
 */
static void
reverse(char *s)
{
	size_t i, j, len;
	char tmp;

	len = strlen(s);
	for (i = 0, j = len - 1; i < len / 2; i++, j--) {
		tmp = s[i];
		s[i] = s[j];
		s[j] = tmp;
	}
}

/*
 * gen_kv --
 *	Generate a key/value.
 */
static void
gen_kv(char *buf, size_t buf_size, uint64_t id, uint32_t threadid,
    const char *large, bool forward)
{
	size_t keyid_size, large_size;
	char keyid[64];

	testutil_check(__wt_snprintf(keyid, sizeof(keyid),
	    "%10.10" PRIu64, id));
	keyid_size = strlen(keyid);
	if (!forward)
		reverse(keyid);
	testutil_assert(keyid_size + 4 <= buf_size);
	large_size = buf_size - 4 - keyid_size;
	testutil_check(__wt_snprintf(buf, buf_size,
	    "%s" KEY_SEP "%1.1x" KEY_SEP "%.*s",
	    keyid, threadid, (int)large_size, large));
}

/*
 * gen_table_name --
 *	Generate a table name used for the schema test.
 */
static void
gen_table_name(char *buf, size_t buf_size, uint64_t id, uint32_t threadid)
{
	testutil_check(__wt_snprintf(buf, buf_size,
	    "table:A%" PRIu64 "-%" PRIu32, id, threadid));
}

/*
 * gen_table2_name --
 *	Generate a second table name used for the schema test.
 */
static void
gen_table2_name(char *buf, size_t buf_size, uint64_t id, uint32_t threadid)
{
	testutil_check(__wt_snprintf(buf, buf_size,
	    "table:B%" PRIu64 "-%" PRIu32, id, threadid));
}

/*
 * thread_run --
 *	Run a writer thread.
 */
static WT_THREAD_RET thread_run(void *)
	WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static WT_THREAD_RET
thread_run(void *arg)
{
	WT_CURSOR *cursor, *rev, *sch;
	WT_RAND_STATE rnd;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	size_t lsize;
	uint64_t i;
	uint32_t kvsize;
	int ret;
	char *buf1, *buf2;
	char large[LARGE_WRITE_SIZE];
	bool retryable_error;

	__wt_random_init(&rnd);
	lsize = sizeof(large);
	memset(large, 0, lsize);

	td = (WT_THREAD_DATA *)arg;
	large_buf(large, lsize, td->id, true);

	testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(session, uri_main, NULL, NULL,
	    &cursor));
	testutil_check(session->open_cursor(session, uri_rev, NULL, NULL,
	    &rev));

	/*
	 * Split the allocated buffer into two parts, one for
	 * the key, one for the value.
	 */
	kvsize = td->datasize / 2;
	buf1 = td->data;
	buf2 = &td->data[kvsize];

	/*
	 * Continuing writing until we're killed.
	 */
	printf("Thread %" PRIu32 "\n", td->id);
	for (i = 0; ; ++i) {
	again:
		retryable_error = false;
		/*
		if (i > 0 && i % 10000 == 0)
			printf("Thread %d completed %d entries\n",
			    (int)td->id, (int)i);
		*/

		gen_kv(buf1, kvsize, i, td->id, large, true);
		gen_kv(buf2, kvsize, i, td->id, large, false);

		testutil_check(session->begin_transaction(session, NULL));
		cursor->set_key(cursor, buf1);
		/*
		 * Every 1000th record write a very large value that exceeds the
		 * log buffer size.  This forces us to use the unbuffered path.
		 */
		if (i % 1000 == 0) {
			cursor->set_value(cursor, large);
		} else {
			cursor->set_value(cursor, buf2);
		}
		testutil_check(cursor->insert(cursor));

		/*
		 * The reverse table has no very large records.
		 */
		rev->set_key(rev, buf2);
		rev->set_value(rev, buf1);
		testutil_check(rev->insert(rev));

		/*
		 * If we are doing a schema test, generate operations
		 * for additional tables.  Each table has a 'lifetime'
		 * of 4 values of the id.
		 */
		if (td->schema_test) {
			if (!retryable_error && SCHEMA_OP(i, 0)) {
				/* Create a table. */
				gen_table_name(buf1, kvsize, i, td->id);
				testutil_check(session->create(session, buf1,
				    "key_format=S,value_format=S"));
			}
			if (!retryable_error && SCHEMA_OP(i, 1)) {
				/* Insert a value into the table. */
				gen_table_name(buf1, kvsize, i - 1, td->id);
				testutil_check(session->open_cursor(session,
				    buf1, NULL, NULL, &sch));
				sch->set_key(sch, buf1);
				sch->set_value(sch, buf1);
				testutil_check(sch->insert(sch));
				sch->close(sch);
			}
			if (!retryable_error && SCHEMA_OP(i, 2)) {
				/* Rename the table. */
				gen_table_name(buf1, kvsize, i - 2, td->id);
				gen_table2_name(buf2, kvsize, i - 2, td->id);
				/*
				 * XXX
				 * We notice occasional EBUSY errors from
				 * rename, even though neither URI should be
				 * used by any other thread.
				 */
				/*
				printf(" rename(\"%s\", \"%s\")\n", buf1, buf2);
				*/
				if ((ret = session->rename(session, buf1, buf2,
				    NULL)) == EBUSY) {
					printf("rename(\"%s\", \"%s\") failed,"
					    " retrying transaction\n", buf1,
					    buf2);
					retryable_error = true;
				} else {
					if (ret != 0)
						printf("FAIL: "
						    "rename(\"%s\", \"%s\") "
						    "returns %d: %s\n",
						    buf1, buf2, ret,
						    wiredtiger_strerror(ret));
					testutil_check(ret);
				}
			}
			if (!retryable_error && SCHEMA_OP(i, 3)) {
				/* Update the single value in the table. */
				gen_table_name(buf1, kvsize, i - 3, td->id);
				gen_table2_name(buf2, kvsize, i - 3, td->id);
				testutil_check(session->open_cursor(session,
				    buf2, NULL, NULL, &sch));
				sch->set_key(sch, buf1);
				sch->set_value(sch, buf2);
				testutil_check(sch->insert(sch));
				sch->close(sch);
			}
			if (!retryable_error && SCHEMA_OP(i, 4)) {
				/* Drop the table. */
				gen_table2_name(buf1, kvsize, i - 4, td->id);
				/*
				 * XXX
				 * We notice occasional EBUSY errors from drop,
				 * even though the URI should not be used by
				 * any other thread.
				 */
				/*
				printf(" drop(\"%s\")\n", buf1);
				*/
				if ((ret = session->drop(session, buf1, NULL))
				    == EBUSY) {
					printf("drop(\"%s\") failed,"
					    " retrying transaction\n", buf1);
					retryable_error = true;
				} else {
					if (ret != 0)
						printf("FAIL: "
						    "drop(\"%s\") "
						    "returns %d: %s\n",
						    buf1, ret,
						    wiredtiger_strerror(ret));
					testutil_check(ret);
				}
			}
		}
		if (retryable_error) {
			testutil_check(session->rollback_transaction(
			    session, NULL));
			sleep(1);
			goto again;
		}
		testutil_check(session->commit_transaction(session, NULL));
	}
	/* NOTREACHED */
}

/*
 * fill_db --
 *	The child process creates the database and table, and then creates
 *	worker threads to add data until it is killed by the parent.
 */
static void fill_db(uint32_t, uint32_t, const char *, bool)
    WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
static void
fill_db(uint32_t nth, uint32_t datasize, const char *method, bool schema_test)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_THREAD_DATA *td;
	wt_thread_t *thr;
	uint32_t i;
	char envconf[512];

	thr = dcalloc(nth, sizeof(*thr));
	td = dcalloc(nth, sizeof(WT_THREAD_DATA));
	if (chdir(home) != 0)
		testutil_die(errno, "Child chdir: %s", home);
	testutil_check(__wt_snprintf(envconf, sizeof(envconf),
	    ENV_CONFIG, method));

	testutil_check(wiredtiger_open(".", NULL, envconf, &conn));
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->create(
	    session, uri_main, "key_format=S,value_format=S"));
	testutil_check(session->create(
	    session, uri_rev, "key_format=S,value_format=S"));
	/*
	 * TODO: consider putting data size, etc. into the database,
	 * or into a text file.
	 */
	testutil_check(session->close(session, NULL));

	datasize += 1;   /* Add an extra byte for string termination */
	printf("Create %" PRIu32 " writer threads\n", nth);
	for (i = 0; i < nth; ++i) {
		td[i].conn = conn;
		td[i].data = dcalloc(datasize, 1);
		td[i].datasize = datasize;
		td[i].id = i;
		td[i].schema_test = schema_test;
		testutil_check(__wt_thread_create(
		    NULL, &thr[i], thread_run, &td[i]));
	}
	printf("Spawned %" PRIu32 " writer threads\n", nth);
	fflush(stdout);
	/*
	 * The threads never exit, so the child will just wait here until
	 * it is killed.
	 */
	for (i = 0; i < nth; ++i) {
		testutil_check(__wt_thread_join(NULL, &thr[i]));
		free(td[i].data);
	}
	/*
	 * NOTREACHED
	 */
	free(thr);
	free(td);
	exit(EXIT_SUCCESS);
}

/*
 * check_kv --
 *	Check that a key exists with a value, or does not exist.
 */
static void
check_kv(WT_CURSOR *cursor, const char *key, const char *value, bool exists)
{
	int ret;
	char *got;

	cursor->set_key(cursor, key);
	ret = cursor->search(cursor);
	if ((ret = cursor->search(cursor)) == WT_NOTFOUND) {
		if (exists) {
			printf("FAIL: expected rev file to have: %s\n", key);
			testutil_assert(!exists);
		}
	} else {
		testutil_check(ret);
		if (!exists) {
			printf("FAIL: unexpected key in rev file: %s\n", key);
			testutil_assert(exists);
		}

		cursor->get_value(cursor, &got);
		TEST_STREQ(value, got, "value");
	}
}

/*
 * check_dropped --
 *	Check that the uri has been dropped.
 */
static void
check_dropped(WT_SESSION *session, const char *uri)
{
	WT_CURSOR *cursor;
	int ret;

	ret = session->open_cursor(session, uri, NULL, NULL, &cursor);
	testutil_assert(ret == WT_NOTFOUND);
}

/*
 * check_empty --
 *	Check that the uri exists and is empty.
 */
static void
check_empty(WT_SESSION *session, const char *uri)
{
	WT_CURSOR *cursor;
	int ret;

	testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
	ret = cursor->next(cursor);
	testutil_assert(ret == WT_NOTFOUND);
	testutil_check(cursor->close(cursor));
}

/*
 * check_empty --
 *	Check that the uri exists and has one entry.
 */
static void
check_one_entry(WT_SESSION *session, const char *uri, const char *key,
    const char *value)
{
	WT_CURSOR *cursor;
	int ret;
	char *gotkey, *gotvalue;

	testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
	testutil_check(cursor->next(cursor));
	cursor->get_key(cursor, &gotkey);
	cursor->get_value(cursor, &gotvalue);
	testutil_assert(WT_STREQ(key, gotkey));
	testutil_assert(WT_STREQ(value, gotvalue));
	ret = cursor->next(cursor);
	testutil_assert(ret == WT_NOTFOUND);
	testutil_check(cursor->close(cursor));
}

/*
 * check_schema
 *	Check that the database has the expected schema according to the
 *	last id seen for this thread.
 */
static void
check_schema(WT_SESSION *session, uint64_t lastid, uint32_t threadid)
{
	char uri[50], uri2[50];

	if (SCHEMA_OP(lastid, 0)) {
		/* Create table operation. */
		gen_table_name(uri, sizeof(uri), lastid, threadid);
		check_empty(session, uri);
	}
	if (SCHEMA_OP(lastid, 1)) {
		/* Insert value operation. */
		gen_table_name(uri, sizeof(uri), lastid - 1, threadid);
		check_one_entry(session, uri, uri, uri);
	}
	if (SCHEMA_OP(lastid, 2)) {
		/* Table rename operation. */
		gen_table_name(uri, sizeof(uri), lastid - 2, threadid);
		gen_table2_name(uri2, sizeof(uri2), lastid - 2, threadid);
		check_dropped(session, uri);
		check_one_entry(session, uri2, uri, uri);
	}
	if (SCHEMA_OP(lastid, 3)) {
		/* Value update operation. */
		gen_table_name(uri, sizeof(uri), lastid - 2, threadid);
		gen_table2_name(uri2, sizeof(uri2), lastid - 2, threadid);
		check_one_entry(session, uri2, uri, uri2);
	}
	if (SCHEMA_OP(lastid, 4)) {
		/* Drop table operation. */
		gen_table2_name(uri2, sizeof(uri2), lastid - 2, threadid);
		check_dropped(session, uri2);
	}
}

/*
 * check_db --
 *	Make a copy of the database and verify its contents.
 */
static bool
check_db(uint32_t nth, uint32_t datasize, bool directio, bool schema_test)
{
	struct sigaction sa;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor, *meta, *rev;
	WT_SESSION *session;
	uint64_t gotid, id;
	uint64_t *lastid;
	uint32_t gotth, kvsize, th, threadmap;
	int ret, status;
	char buf[4096];
	char *gotkey, *gotvalue, *keybuf, *p;
	char **large_arr;

	keybuf = dcalloc(datasize, 1);
	lastid = dcalloc(nth, sizeof(uint64_t));

	large_arr = dcalloc(nth, sizeof(char *));
	for (th = 0; th < nth; th++) {
		large_arr[th] = dcalloc(LARGE_WRITE_SIZE, 1);
		large_buf(large_arr[th], LARGE_WRITE_SIZE, th, true);
	}

	/*
	 * We make a copy of the directory (possibly using direct IO)
	 * for recovery and checking, and an identical copy that
	 * keeps the state of all files before recovery starts.
	 */
	testutil_check(__wt_snprintf(buf, sizeof(buf),
	    "H='%s'; C=$H.CHECK; S=$H.SAVE; rm -rf $C $S;"
	    " mkdir $C $S; for f in `ls $H/`; do "
	    " dd if=$H/$f of=$C/$f bs=4096 %s >/dev/null 2>&1 || exit 1; done;"
	    " cp -pr $C $S",
	    home, directio ? "iflag=direct" : ""));
	printf("Shell command: %s\n", buf);

	/* Temporarily turn off the child handler while running 'system' */
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = SIG_DFL;
	testutil_checksys(sigaction(SIGCHLD, &sa, NULL));
	if ((status = system(buf)) < 0)
		testutil_die(status, "system: %s", buf);
	sa.sa_handler = handler;
	testutil_checksys(sigaction(SIGCHLD, &sa, NULL));

	testutil_check(__wt_snprintf(buf, sizeof(buf), "%s.CHECK", home));

	printf("Open database, run recovery and verify content\n");
	testutil_check(wiredtiger_open(buf, NULL, ENV_CONFIG_REC, &conn));
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(session, uri_main, NULL, NULL,
	    &cursor));
	testutil_check(session->open_cursor(session, uri_rev, NULL, NULL,
	    &rev));
	kvsize = datasize / 2;

	/*
	 * We're most interested in the final records on disk.
	 * Rather than walk all records, we do a quick scan
	 * to find the last complete set of written ids.
	 * Each thread writes each id, along with the thread id,
	 * so they are interleaved.  Once we have the neighborhood
	 * where some keys may be missing, we'll back up to do a scan
	 * from that point.
	 */

#define	CHECK_INCR	1000
	for (id = 0; ; id += CHECK_INCR) {
		gen_kv(keybuf, kvsize, id, 0, large_arr[0], true);
		cursor->set_key(cursor, keybuf);
		if ((ret = cursor->search(cursor)) == WT_NOTFOUND)
			break;
		testutil_check(ret);
		for (th = 1; th < nth; th++) {
			gen_kv(keybuf, kvsize, id, th, large_arr[th], true);
			cursor->set_key(cursor, keybuf);
			if ((ret = cursor->search(cursor)) == WT_NOTFOUND)
				break;
			testutil_check(ret);
		}
		if (ret == WT_NOTFOUND)
			break;
	}
	if (id < CHECK_INCR * 2)
		id = 0;
	else
		id -= CHECK_INCR * 2;

	printf("starting full scan at %" PRIu64 "\n", id);
	gen_kv(keybuf, kvsize, id, 0, large_arr[0], true);
	cursor->set_key(cursor, keybuf);
	testutil_check(cursor->search(cursor));
	th = 0;

	/* Keep bitmap of "active" threads. */
	threadmap = (0x1U << nth) - 1;
	for (ret = 0; ret != WT_NOTFOUND && threadmap != 0;
	     ret = cursor->next(cursor)) {
		testutil_check(ret);
		cursor->get_key(cursor, &gotkey);
		gotid = (uint64_t)strtol(gotkey, &p, 10);
		testutil_assert(*p == KEY_SEP[0]);
		p++;
		testutil_assert(isxdigit(*p));
		if (isdigit(*p))
			gotth = (uint32_t)(*p - '0');
		else if (*p >= 'a' && *p <= 'f')
			gotth = (uint32_t)(*p - 'a' + 10);
		else
			gotth = (uint32_t)(*p - 'A' + 10);
		p++;
		testutil_assert(*p == KEY_SEP[0]);
		p++;

		/*
		 * See if the expected thread has finished at this point.
		 * If so, remove it from the thread map.
		 */
		while (gotth != th) {
			if ((threadmap & (0x1U << th)) != 0) {
				threadmap &= ~(0x1U << th);
				lastid[th] = id - 1;
				/*
				 * Any newly removed should not be present
				 * in the reverse table, since they
				 * were transactionally inserted at the
				 * same time.
				 */
				gen_kv(keybuf, kvsize, id, th, large_arr[th],
				    false);
				check_kv(rev, keybuf, NULL, false);
				if (schema_test)
					check_schema(session, id - 1, th);
			}
			th = (th + 1) % nth;
			if (th == 0)
				id++;
		}
		testutil_assert(gotid == id);
		/*
		 * Check that the key and value fully match.
		 */
		gen_kv(keybuf, kvsize, id, th, large_arr[th], true);
		gen_kv(&keybuf[kvsize], kvsize, id, th, large_arr[th], false);
		cursor->get_value(cursor, &gotvalue);
		TEST_STREQ(keybuf, gotkey, "main table key");

		/*
		 * Every 1000th record is large.
		 */
		if (id % 1000 == 0)
			TEST_STREQ(large_arr[th], gotvalue,
			    "main table large value");
		else
			TEST_STREQ(&keybuf[kvsize], gotvalue,
			    "main table value");

		/*
		 * Check the reverse file, with key/value reversed.
		 */
		check_kv(rev, &keybuf[kvsize], keybuf, true);

		/* Bump thread number and id to the next expected key. */
		th = (th + 1) % nth;
		if (th == 0)
			id++;
	}
	printf("scanned to %" PRIu64 "\n", id);

	if (schema_test) {
		/*
		 * Check metadata to see if there are any tables
		 * present that shouldn't be there.
		 */
		testutil_check(session->open_cursor(session, "metadata:", NULL,
		    NULL, &meta));
		while ((ret = meta->next(meta)) != WT_NOTFOUND) {
			testutil_check(ret);
			meta->get_key(meta, &gotkey);
			/*
			 * Names involved in schema testing are off the form:
			 *   table:Axxx-t
			 *   table:Bxxx-t
			 * xxx corresponds to the id inserted into the main
			 * table when the table was created, and t corresponds
			 * to the thread id that did this.
			 */
			if (WT_PREFIX_SKIP(gotkey, "table:") &&
			    (*gotkey == 'A' || *gotkey == 'B')) {
				gotid = (uint64_t)strtol(gotkey + 1, &p, 10);
				testutil_assert(*p == '-');
				th = (uint32_t)strtol(p + 1, &p, 10);
				testutil_assert(*p == '\0');
				/*
				 * XXX
				 * If table operations are transactional,
				 * then there is more to do here.
				 */
			}
		}
		testutil_check(meta->close(meta));

	}

	testutil_check(cursor->close(cursor));
	testutil_check(rev->close(rev));
	testutil_check(session->close(session, NULL));
	testutil_check(conn->close(conn, NULL));

	for (th = 0; th < nth; th++)
		free(large_arr[th]);
	free(large_arr);
	free(keybuf);
	free(lastid);
	return (true);
}

/*
 * handler --
 *	Child signal handler
 */
static void
handler(int sig)
{
	pid_t pid;
	int status, termsig;

	WT_UNUSED(sig);
	pid = waitpid(-1, &status, WNOHANG|WUNTRACED);
	if (pid == 0)
		return;		/* Nothing to wait for. */
	if (WIFSTOPPED(status))
		return;
	if (WIFSIGNALED(status)) {
		termsig = WTERMSIG(status);
		if (termsig == SIGCONT || termsig == SIGSTOP)
			return;
		printf("Child got signal %d (status = %d, 0x%x)\n",
		    termsig, status, (unsigned int)status);
#ifdef WCOREDUMP
		if (WCOREDUMP(status))
			printf("Child process id=%d created core file\n", pid);
#endif
	}

	/*
	 * The core file will indicate why the child exited. Choose EINVAL here.
	 */
	testutil_die(EINVAL,
	    "Child process %" PRIu64 " abnormally exited, status=%d (0x%x)",
	    (uint64_t)pid, status, status);
}

/*
 * sleep_wait --
 *	Wait for a process up to a number of seconds.
 */
static void
sleep_wait(uint32_t seconds, pid_t pid)
{
	pid_t got;
	int status;

	while (seconds > 0) {
		if ((got = waitpid(pid, &status, WNOHANG|WUNTRACED)) == pid) {
			if (WIFEXITED(status))
				testutil_die(EINVAL,
				    "Child process %" PRIu64 " exited early"
				    " with status %d", (uint64_t)pid,
				    WEXITSTATUS(status));
			if (WIFSIGNALED(status))
				testutil_die(EINVAL,
				    "Child process %" PRIu64 " terminated "
				    " with signal %d", (uint64_t)pid,
				    WTERMSIG(status));
		} else if (got == -1)
			testutil_die(errno, "waitpid");

		--seconds;
		sleep(1);
	}
}

/*
 * has_direct_io --
 *	Check for direct I/O support.
 */
static bool
has_direct_io(void)
{
#ifdef O_DIRECT
	return (true);
#else
	return (false);
#endif
}

/*
 * main --
 *	Top level test.
 */
int
main(int argc, char *argv[])
{
	struct sigaction sa;
	struct stat sb;
	WT_RAND_STATE rnd;
	pid_t pid;
	uint32_t datasize, i, interval, ncycles, nth, timeout;
	int ch, status;
	const char *method, *working_dir;
	char buf[1024];
	bool populate_only, rand_th, rand_time, schema_test, verify_only;

	(void)testutil_set_progname(argv);

	datasize = DEFAULT_DATA_SIZE;
	nth = MIN_TH;
	ncycles = DEFAULT_CYCLES;
	rand_th = rand_time = true;
	timeout = MIN_TIME;
	interval = DEFAULT_INTERVAL;
	populate_only = schema_test = verify_only = false;
	working_dir = "WT_TEST.random-directio";
	method = "none";
	pid = 0;

	if (!has_direct_io()) {
		fprintf(stderr, "**** test_random_directio: this system does "
		    "not support direct I/O.\n**** Skipping test.\n");
		return (EXIT_SUCCESS);
	}
	while ((ch = __wt_getopt(progname, argc, argv,
	    "dh:i:m:n:pST:t:v")) != EOF)
		switch (ch) {
		case 'd':
			datasize = (uint32_t)atoi(__wt_optarg);
			if (datasize > LARGE_WRITE_SIZE ||
			    datasize < MIN_DATA_SIZE) {
				fprintf(stderr,
				    "-d value is larger than maximum %"
				    PRId32 "\n",
				    LARGE_WRITE_SIZE);
				exit (EXIT_FAILURE);
			}
			break;
		case 'h':
			working_dir = __wt_optarg;
			break;
		case 'i':
			interval = (uint32_t)atoi(__wt_optarg);
			break;
		case 'm':
			method = __wt_optarg;
			if (!WT_STREQ(method, "fsync") &&
			    !WT_STREQ(method, "dsync") &&
			    !WT_STREQ(method, "none")) {
				fprintf(stderr,
				    "-m option requires fsync|dsync|none\n");
				exit (EXIT_FAILURE);
			}
			break;
		case 'n':
			ncycles = (uint32_t)atoi(__wt_optarg);
			break;
		case 'p':
			populate_only = true;
			break;
		case 'S':
			schema_test = true;
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
		default:
			usage();
		}
	argc -= __wt_optind;
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
		testutil_check(__wt_snprintf(buf, sizeof(buf),
		    "rm -rf %s", home));
		if ((status = system(buf)) < 0)
			testutil_die(status, "system: %s", buf);
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
		printf("Parent: Create %" PRIu32
		    " threads; sleep %" PRIu32 " seconds\n", nth, timeout);

		if (!populate_only) {
			/*
			 * Fork a child to insert as many items.  We will
			 * then randomly suspend the child, run recovery and
			 * make sure all items we wrote exist after recovery
			 * runs.
			 */
			memset(&sa, 0, sizeof(sa));
			sa.sa_handler = handler;
			testutil_checksys(sigaction(SIGCHLD, &sa, NULL));
			if ((pid = fork()) < 0)
				testutil_die(errno, "fork");
		}
		if (pid == 0) { /* child, or populate_only */
			fill_db(nth, datasize, method, schema_test);
			return (EXIT_SUCCESS);
		}

		/* parent */
		/*
		 * Sleep for the configured amount of time before killing
		 * the child.  Start the timeout from the time we notice that
		 * the table has been created.  That allows the test to run
		 * correctly on really slow machines.
		 */
		testutil_check(__wt_snprintf(
		    buf, sizeof(buf), "%s/%s", home, fs_main));
		while (stat(buf, &sb) != 0 || sb.st_size < 4096)
			sleep_wait(1, pid);
		sleep_wait(timeout, pid);

		/*
		 * Begin our cycles of suspend, copy, recover.
		 */
		for (i = 0; i < ncycles; i++) {
			printf("Beginning cycle %" PRIu32 "/%" PRIu32 "\n",
			    i + 1, ncycles);
			if (i != 0)
				sleep_wait(interval, pid);
			printf("Suspend child\n");
			if (kill(pid, SIGSTOP) != 0)
				testutil_die(errno, "kill");
			printf("Check DB\n");
			fflush(stdout);
			if (!check_db(nth, datasize, true, schema_test))
				return (EXIT_FAILURE);
			if (kill(pid, SIGCONT) != 0)
				testutil_die(errno, "kill");
			printf("\n");
		}

		printf("Kill child\n");
		sa.sa_handler = SIG_DFL;
		testutil_checksys(sigaction(SIGCHLD, &sa, NULL));
		if (kill(pid, SIGKILL) != 0)
			testutil_die(errno, "kill");
		if (waitpid(pid, &status, 0) == -1)
			testutil_die(errno, "waitpid");
	}
	if (verify_only && !check_db(nth, datasize, false, schema_test)) {
		printf("FAIL\n");
		return (EXIT_FAILURE);
	}
	printf("SUCCESS\n");
	return (EXIT_SUCCESS);
}
