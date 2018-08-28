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
#include "test_util.h"

#include <sys/wait.h>
#include <signal.h>

#define	CORRUPT "file:zzz-corrupt.SS"
#define	KEY	"key"
#define	VALUE	"value,value,value"

/*
 * NOTE: This assumes the default page size of 4096. If that changes these
 * sizes need to change along with it.
 */
#define	APP_MD_SIZE 	4096
#define	APP_BUF_SIZE	3 * 1024
#define	APP_STR		"long app metadata. "

static bool saw_corruption = false;
static bool test_abort = false;

static int
handle_message(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, int error, const char *message)
{
	(void)(handler);

	/* Skip the error messages we're expecting to see. */
	if ((strstr(message, "database corruption detected") != NULL))
		saw_corruption = true;

	(void)fprintf(stderr, "%s: %s\n",
	    message, session->strerror(session, error));
	if (test_abort) {
		fprintf(stderr, "Got unexpected error. Aborting\n");
		abort();
	}
	return (0);
}

static WT_EVENT_HANDLER event_handler = {
	handle_message,
	NULL,
	NULL,
	NULL
};

typedef struct table_info {
	const char *name;
	const char *kvformat;
	bool verified;
} TABLE_INFO;

/*
 * byte_str --
 * 	A byte-string version to find a sub-string. The metadata we read
 * 	contains a lot of zeroes so we cannot use string-based functions.
 */
static uint8_t *
byte_str(uint8_t *buf, size_t bufsize, const char *str)
{
	size_t buflen, slen;
	uint8_t *end, *p, *s;
	int c;

	p = buf;
	end = buf + bufsize;
	s = NULL;
	c = (int)str[0];
	buflen = bufsize;
	slen = strlen(str);
	/*
	 * Find the first character and then compare.
	 */
	while ((s = memchr(p, c, buflen)) != NULL) {
		/*
		 * If we don't have enough buffer left to compare we do not
		 * have a match.
		 */
		buflen = (size_t)(end - s);
		if (buflen < slen)
			return (NULL);
		if (memcmp(s, str, slen) == 0)
			return (s);
		/*
		 * This one didn't match, increment in the buffer and find the
		 * next one.
		 */
		++s;
		--buflen;
		p = s;
	}
	return (NULL);
}

/*
 * cursor_insert --
 *	Insert some data into a table.
 */
static void
cursor_insert(WT_SESSION *session, const char *uri)
{
	WT_CURSOR *cursor;
	WT_ITEM vu;
	char keybuf[100], valuebuf[100];
	bool recno;

	/* Reserve requires a running transaction. */
	testutil_check(session->begin_transaction(session, NULL));

	memset(&vu, 0, sizeof(vu));

	/* Open a cursor. */
	testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

	/* Operations change based on the key/value formats. */
	recno = strcmp(cursor->key_format, "r") == 0;
	if (recno)
		cursor->set_key(cursor, (uint64_t)1);
	else {
		strcpy(keybuf, KEY);
		cursor->set_key(cursor, keybuf);
	}
	strcpy(valuebuf, VALUE);
	cursor->set_value(cursor, valuebuf);
	testutil_check(cursor->insert(cursor));
	testutil_check(cursor->close(cursor));
}

/*
 * create_data --
 * 	Create a table and insert a piece of data.
 */
static void
create_data(WT_CONNECTION *conn, TABLE_INFO *t)
{
	WT_SESSION *session;
	size_t len;
	uint64_t i;
	char buf[APP_BUF_SIZE], cfg[APP_MD_SIZE];

	memset(buf, 0, sizeof(buf));
	memset(cfg, 0, sizeof(cfg));

	/*
	 * Create an app-specific metadata string that fills most of page
	 * so that each table in the metadata has its own page.
	 */
	len = strlen(APP_STR);
	for (i = 0; i + len < APP_BUF_SIZE; i += len)
		testutil_check(__wt_snprintf(
		    &buf[i], APP_BUF_SIZE - i, "%s", APP_STR));
	testutil_check(__wt_snprintf(cfg, sizeof(cfg),
	    "%s,app_metadata=\"%s\"", t->kvformat, buf));
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->create(session, t->name, cfg));
	cursor_insert(session, t->name);
	testutil_check(session->close(session, NULL));
}

/*
 * corrupt_metadata --
 *	Corrupt the metadata by scribbling on the "corrupt" URI string.
 */
static void
corrupt_metadata(const char *home)
{
	FILE *fp;
	struct stat sb;
	long off;
	size_t meta_size;
	bool corrupted;
	uint8_t *buf, *corrupt;
	char path[256];

	/*
	 * Open the file, read its contents. Find the string "corrupt" and
	 * modify one byte at that offset. That will cause a checksum error
	 * when WiredTiger next reads it.
	 */
	sprintf(path, "%s/%s", home, WT_METAFILE);
	testutil_check(stat(path, &sb));
	meta_size = (size_t)sb.st_size;
	buf = dmalloc(meta_size);
	memset(buf, 0, meta_size);
	if ((fp = fopen(path, "r+")) == NULL)
		testutil_die(errno, "fopen: %s", WT_METAFILE);
	if (fread(buf, 1, meta_size, fp) != meta_size)
		testutil_die(errno, "fread: %" PRIu64, (uint64_t)meta_size);
	corrupted = false;
	/*
	 * Corrupt all occurrences of the string in the file.
	 */
	while ((corrupt = byte_str(buf, meta_size, CORRUPT)) != NULL) {
		corrupted = true;
		testutil_assert(*(char *)corrupt != 'X');
		*(char *)corrupt = 'X';
		off = (long)(corrupt - buf);
		if (fseek(fp, off, SEEK_SET) != 0)
			testutil_die(errno, "fseek: %" PRIu64, (uint64_t)off);
		if (fwrite("X", 1, 1, fp) != 1)
			testutil_die(errno, "fwrite");
	}
	if (!corrupted)
		testutil_die(errno, "corrupt string did not occur");
	if (fclose(fp) != 0)
		testutil_die(errno, "fclose");
}

/*
 * file_exists --
 *	Return if the file exists.
 */
static int
file_exists(const char *path)
{
	struct stat sb;

	return (stat(path, &sb) == 0);
}

/*
 * reset_verified --
 *	Reset the verified field in the table array.
 */
static void
reset_verified(TABLE_INFO *tables)
{
	TABLE_INFO *t;

	for (t = tables; t->name != NULL; t++)
		t->verified = false;
}

/*
 * verify_metadata --
 *	Verify all the tables expected are in the metadata. We expect all but
 *	the "corrupt" table name.
 */
static void
verify_metadata(WT_CONNECTION *conn, TABLE_INFO *tables)
{
	TABLE_INFO *t;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	int ret;
	const char *kv;

	/*
	 * Open a metadata cursor.
	 */
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->open_cursor(
	    session, "metadata:", NULL, NULL, &cursor));
	reset_verified(tables);

	/*
	 * We have to walk the cursor and walk the tables to match up that
	 * the expected tables are in the metadata. It is not efficient, but
	 * the list of tables is small. Walk the cursor once and the array
	 * of tables each time.
	 */
	while ((ret = cursor->next(cursor)) == 0) {
		testutil_check(cursor->get_key(cursor, &kv));
		for (t = tables; t->name != NULL; t++) {
			if (strcmp(t->name, kv) == 0) {
				testutil_assert(t->verified == false);
				t->verified = true;
				break;
			}
		}
	}
	cursor->close(cursor);
	/*
	 * Any tables that were salvaged, make sure we can read the data.
	 * The corrupt table should never be salvaged.
	 */
	for (t = tables; t->name != NULL; t++) {
		if (strcmp(t->name, CORRUPT) == 0)
			testutil_assert(t->verified == false);
		else if (t->verified != true)
			printf("%s not seen in metadata\n", t->name);
		else {
			testutil_check(session->open_cursor(
			    session, t->name, NULL, NULL, &cursor));
			while ((ret = cursor->next(cursor)) == 0) {
				testutil_check(cursor->get_value(cursor, &kv));
				testutil_assert(strcmp(kv, VALUE) == 0);
			}
			cursor->close(cursor);
			printf("%s metadata salvaged and data verified\n",
			    t->name);
		}
	}
}

static int
wt_open_corrupt(const char *home)
{
	WT_CONNECTION *conn;
	int ret;

	conn = NULL;
	ret = wiredtiger_open(home, &event_handler, NULL, &conn);
	testutil_assert(conn == NULL);
	testutil_assert(ret == WT_PANIC);
	testutil_assert(saw_corruption == true);
	exit (EXIT_SUCCESS);
}

int
main(int argc, char *argv[])
{
	/*
	 * Add a bunch of tables so that some of the metadata ends up on
	 * other pages and a good number of tables are available after
	 * salvage completes.
	 */
	TABLE_INFO table_data[] = {
		{ "file:aaa-file.SS", "key_format=S,value_format=S", false },
		{ "file:bbb-file.rS", "key_format=r,value_format=S", false },
		{ "lsm:ccc-lsm.SS", "key_format=S,value_format=S", false },
		{ "table:ddd-table.SS", "key_format=S,value_format=S", false },
		{ "table:eee-table.rS", "key_format=r,value_format=S", false },
		{ "file:fff-file.SS", "key_format=S,value_format=S", false },
		{ "file:ggg-file.rS", "key_format=r,value_format=S", false },
		{ "lsm:hhh-lsm.SS", "key_format=S,value_format=S", false },
		{ "table:iii-table.SS", "key_format=S,value_format=S", false },
		{ "table:jjj-table.rS", "key_format=r,value_format=S", false },
		{ CORRUPT, "key_format=S,value_format=S", false },
		{ NULL, NULL, false }
	};
	TABLE_INFO *t;
	TEST_OPTS *opts, _opts;
	pid_t pid;
	int ret, status;
	char buf[1024];

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(
	    wiredtiger_open(opts->home, &event_handler, "create", &opts->conn));

	/*
	 * Create a bunch of different tables.
	 */
	for (t = table_data; t->name != NULL; t++)
		create_data(opts->conn, t);

	testutil_check(opts->conn->close(opts->conn, NULL));
	opts->conn = NULL;

	/*
	 * Make copy of original directory.
	 */
	testutil_check(__wt_snprintf(buf, sizeof(buf),
	    "rm -rf ./%s.SAVE; mkdir ./%s.SAVE; "
	    "cp -p %s/* ./%s.SAVE;",
	    opts->home, opts->home, opts->home, opts->home));
	printf("copy: %s\n", buf);
	if ((ret = system(buf)) < 0)
		testutil_die(ret, "system: %s", buf);

	/*
	 * Damage/corrupt WiredTiger.wt.
	 */
	printf("corrupt metadata\n");
	corrupt_metadata(opts->home);
	testutil_check(__wt_snprintf(buf, sizeof(buf),
	    "cp -p %s/WiredTiger.wt ./%s.SAVE/WiredTiger.wt.CORRUPT",
	    opts->home, opts->home));
	printf("copy: %s\n", buf);
	if ((ret = system(buf)) < 0)
		testutil_die(ret, "system: %s", buf);

	/*
	 * Call wiredtiger_open. We expect to see a corruption panic so we
	 * run this in a forked process. In diagnostic mode, the panic will
	 * cause an abort and core dump. So we want to catch that and
	 * continue running with salvage.
	 */
	if ((pid = fork()) < 0)
		testutil_die(errno, "fork");
	if (pid == 0) { /* child */
		wt_open_corrupt(opts->home);
		return (EXIT_SUCCESS);
	}
	/* parent */
	if (waitpid(pid, &status, 0) == -1)
		testutil_die(errno, "waitpid");
	/*
	 * Check the child exited successfully and did not fail any of
	 * the assertions tested on return.
	 */
#ifdef HAVE_DIAGNOSTIC
	testutil_assert(WIFSIGNALED(status) == true);
#else
	testutil_assert(WIFSIGNALED(status) == false);
#endif

	printf("=== wt_open with salvage ===\n");
	/*
	 * Then call wiredtiger_open with the salvage configuration setting.
	 * That should succeed. We should be able to then verify the contents
	 * of the metadata file.
	 */
	test_abort = true;
	testutil_check(wiredtiger_open(opts->home,
	    &event_handler, "salvage=true,verbose=(salvage)", &opts->conn));
	testutil_assert(opts->conn != NULL);
	sprintf(buf, "%s/%s", opts->home, WT_METAFILE_SLVG);
	testutil_assert(file_exists(buf));

	/*
	 * Confirm we salvaged the metadata file by looking for the saved
	 * copy of the original metadata.
	 */
	printf("verify with salvaged connection\n");
	verify_metadata(opts->conn, &table_data[0]);

	/*
	 * Close and reopen the connection and verify again.
	 */
	testutil_check(opts->conn->close(opts->conn, NULL));
	opts->conn = NULL;
	testutil_check(wiredtiger_open(opts->home,
	    &event_handler, NULL, &opts->conn));
	printf("close and reopen connection, verify\n");
	verify_metadata(opts->conn, &table_data[0]);

	testutil_cleanup(opts);

	return (EXIT_SUCCESS);
}
