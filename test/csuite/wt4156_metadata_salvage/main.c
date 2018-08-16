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

#define	CORRUPT "table:corrupt.SS"
#define	KEY	"key"
#define	VALUE	"value,value,value"

static int ignore_errors;

static int
handle_error(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, int error, const char *message)
{
	(void)(handler);

	/* Skip the error messages we're expecting to see. */
	if (ignore_errors > 0 &&
	    (strstr(message, "requires key be set") != NULL ||
	    strstr(message, "requires value be set") != NULL)) {
		--ignore_errors;
		return (0);
	}

	(void)fprintf(stderr, "%s: %s\n",
	    message, session->strerror(session, error));
	return (0);
}

static WT_EVENT_HANDLER event_handler = {
	handle_error,
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
 * cursor_insert --
 *	Insert some data into a table.
 */
static void
cursor_insert(WT_SESSION *session, const char *uri)
{
	WT_CURSOR *cursor;
	WT_ITEM vu;
	const char *key, *vs;
	char keybuf[100], valuebuf[100];
	bool recno;

	/* Reserve requires a running transaction. */
	testutil_check(session->begin_transaction(session, NULL));

	key = vs = NULL;
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
 *	Create a table and insert a piece of data.
 */
static void
create_data(WT_CONNECTION *conn, TABLE_INFO *t)
{
	WT_SESSION *session;

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->create(session, t->name, t->kvformat));
	cursor_insert(session, t->name);
	testutil_check(session->close(session, NULL));
}

/*
 * corrupt_metadata --
 *	Corrupt the metadata by scribbling on the "corrupt" URI string.
 */
static void
corrupt_metadata(void)
{
	FILE *fp;
	struct stat sb;
	long off;
	size_t meta_size;
	char *buf, *corrupt;

	/*
	 * Open the file, read its contents. Find the string "corrupt" and
	 * modify one byte at that offset. That will cause a checksum error
	 * when WiredTiger next reads it.
	 */
	testutil_check(stat(WT_METAFILE, &sb));
	meta_size = (size_t)sb.st_size;
	buf = dmalloc(meta_size);
	memset(buf, 0, meta_size);
	if ((fp = fopen(WT_METAFILE, "r+")) == NULL)
		testutil_die(errno, "fopen: %s", WT_METAFILE);
	if (fread(buf, 1, meta_size, fp) != meta_size)
		testutil_die(errno, "fread: %" PRIu64, (uint64_t)meta_size);
	if ((corrupt = strstr(buf, "corrupt")) == NULL)
		testutil_die(errno, "strstr: corrupt did not occur");
	off = (long)(corrupt - buf);
	if (fseek(fp, off, SEEK_SET) != 0)
		testutil_die(errno, "fseek: %" PRIu64, (uint64_t)off);
	if (fwrite("X", 1, 1, fp) != 1)
		testutil_die(errno, "fwrite");
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
	char key[64];

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
		testutil_check(cursor->get_key(cursor, key));
		for (t = tables; t->name != NULL; t++) {
			if (strcmp(t->name, key) == 0) {
				testutil_assert(t->verified == false);
				t->verified = true;
				break;
			}
		}
	}
	cursor->close(cursor);
	/*
	 * Make sure all tables exist except the corrupt one.
	 */
	for (t = tables; t->name != NULL; t++) {
		if (strcmp(t->name, CORRUPT) == 0)
			testutil_assert(t->verified == false);
		else
			testutil_assert(t->verified == true);
	}
}

int
main(int argc, char *argv[])
{
	TABLE_INFO table_data[] = {
		{ "file:file.SS", "key_format=S,value_format=S", false },
		{ "file:file.rS", "key_format=r,value_format=S", false },
		{ "lsm:lsm.SS", "key_format=S,value_format=S", false },
		{ "table:table.SS", "key_format=S,value_format=S", false },
		{ "table:table.rS", "key_format=r,value_format=S", false },
		{ CORRUPT, "key_format=S,value_format=S", false },
		{ NULL, NULL, false }
	};
	TABLE_INFO *t;
	TEST_OPTS *opts, _opts;
	int ret;
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
	printf("creating tables and data\n");
	for (t = table_data; t->name != NULL; t++)
		create_data(opts->conn, t);

	testutil_check(opts->conn->close(opts->conn, NULL));
	opts->conn = NULL;

	/*
	 * Make copy of directory.
	 */
	printf("copy directory\n");
	if (chdir(opts->home) != 0)
		testutil_die(errno, "chdir %s", opts->home);
	testutil_check(__wt_snprintf(buf, sizeof(buf),
	    "rm -rf ../%s.SAVE; mkdir ../%s.SAVE; "
	    "cp -p * ../%s.SAVE;",
	    opts->home, opts->home, opts->home));
	printf("copy: %s\n", buf);
	if ((ret = system(buf)) < 0)
		testutil_die(ret, "system: %s", buf);

	/*
	 * Damage/corrupt WiredTiger.wt.
	 */
	printf("corrupt metadata\n");
	corrupt_metadata();

	/*
	 * Call wiredtiger_open. We expect to get back WT_DATA_CORRUPTION.
	 * Then call wiredtiger_open with the salvage configuration setting.
	 * That should succeed. We should be able to then verify the contents
	 * of the metadata file.
	 */
	printf("wt_open\n");
	ret = wiredtiger_open(opts->home, &event_handler, NULL, &opts->conn);
	testutil_assert(ret == WT_DATA_CORRUPTION);
	testutil_assert(opts->conn == NULL);

	printf("wt_open with salvage\n");
	testutil_check(wiredtiger_open(opts->home,
	    &event_handler, "salvage=true", &opts->conn));
	testutil_assert(opts->conn != NULL);
	testutil_assert(file_exists(WT_METAFILE_SLVG));

	/*
	 * Confirm we salvaged the metadata file by looking for the saved
	 * copy of the original metadata.
	 */
	printf("verify 1\n");
	verify_metadata(opts->conn, &table_data[0]);

	/*
	 * Close and reopen the connection and verify again.
	 */
	testutil_check(opts->conn->close(opts->conn, NULL));
	opts->conn = NULL;
	ret = wiredtiger_open(opts->home, &event_handler, NULL, &opts->conn);
	printf("verify 2\n");
	verify_metadata(opts->conn, &table_data[0]);

	testutil_cleanup(opts);

	return (EXIT_SUCCESS);
}
