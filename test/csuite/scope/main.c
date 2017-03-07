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

#define	KEY	"key"
#define	VALUE	"value"

static void
cursor_scope_ops(WT_SESSION *session, const char *uri)
{
	struct {
		const char *op;
		enum { INSERT,
		    SEARCH, SEARCH_NEAR, REMOVE, RESERVE, UPDATE } func;
		const char *config;
	} *op, ops[] = {
		{ "insert", INSERT, NULL, },
		{ "search", SEARCH, NULL, },
		{ "search", SEARCH_NEAR, NULL, },
#if 0
		{ "reserve", RESERVE, NULL, },
#endif
		{ "update", UPDATE, NULL, },
		{ "remove", REMOVE, NULL, },
		{ NULL, INSERT, NULL }
	};
	WT_CURSOR *cursor;
	const char *key, *value;
	char keybuf[100], valuebuf[100];
	int exact;

	/* Reserve requires a running transaction. */
	testutil_check(session->begin_transaction(session, NULL));

	cursor = NULL;
	for (op = ops; op->op != NULL; op++) {
		key = value = NULL;

		/* Open a cursor. */
		if (cursor != NULL)
			testutil_check(cursor->close(cursor));
		testutil_check(session->open_cursor(
		    session, uri, NULL, op->config, &cursor));

		/* Set up application buffers so we can detect overwrites. */
		strcpy(keybuf, KEY);
		cursor->set_key(cursor, keybuf);
		strcpy(valuebuf, VALUE);
		cursor->set_value(cursor, valuebuf);

		/*
		 * The application must keep key and value memory valid until
		 * the next operation that positions the cursor, modifies the
		 * data, or resets or closes the cursor.
		 *
		 * Modifying either the key or value buffers is not permitted.
		 */
		switch (op->func) {
		case INSERT:
			testutil_check(cursor->insert(cursor));
			break;
		case SEARCH:
			testutil_check(cursor->search(cursor));
			break;
		case SEARCH_NEAR:
			testutil_check(cursor->search_near(cursor, &exact));
			break;
		case REMOVE:
			testutil_check(cursor->remove(cursor));
			break;
		case RESERVE:
#if 0
			testutil_check(cursor->reserve(cursor));
#endif
			break;
		case UPDATE:
			testutil_check(cursor->update(cursor));
			break;
		}

		/*
		 * The cursor should no longer reference application memory,
		 * and application buffers can be safely overwritten.
		 */
		memset(keybuf, 'K', sizeof(keybuf));
		memset(valuebuf, 'V', sizeof(valuebuf));

		/*
		 * Check that get_key/get_value behave as expected after the
		 * operation.
		 */
		switch (op->func) {
		case INSERT:
		case REMOVE:
			/*
			 * Insert and remove configured with a search key do
			 * not position the cursor and have no key or value.
			 */
			printf("%s: two WiredTiger error messages expected:\n",
			    progname);
			printf("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");
			testutil_assert(cursor->get_key(cursor, &key) != 0);
			testutil_assert(cursor->get_value(cursor, &value) != 0);
			printf("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n");
			break;
		case RESERVE:
		case SEARCH:
		case SEARCH_NEAR:
		case UPDATE:
			/*
			 * Reserve, search, search-near and update position the
			 * cursor and have both a key and value.
			 */
			testutil_assert(cursor->get_key(cursor, &key) == 0);
			testutil_assert(cursor->get_value(cursor, &value) == 0);

			/*
			 * Any key/value should not reference application
			 * memory.
			 */
			testutil_assert(key != keybuf);
			testutil_assert(strcmp(key, KEY) == 0);
			testutil_assert(value != valuebuf);
			testutil_assert(strcmp(value, VALUE) == 0);
			break;
		}
	}
}

static void
scope_ops(WT_CONNECTION *conn, const char *uri, const char *config)
{
	WT_SESSION *session;

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_check(session->create(session, uri, config));
	cursor_scope_ops(session, uri);
	testutil_check(session->close(session, NULL));
}

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(
	    wiredtiger_open(opts->home, NULL, "create", &opts->conn));

	scope_ops(opts->conn,
	    "file:scope_file", "key_format=S,value_format=S");
	scope_ops(opts->conn,
	    "table:scope_file", "key_format=S,value_format=S,columns=(k,v)");

	testutil_cleanup(opts);
	return (EXIT_SUCCESS);
}
