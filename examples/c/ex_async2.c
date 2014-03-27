/*-
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
 *
 * ex_async.c
 * 	demonstrates how to use the asynchronous API.
 */
#include <stdio.h>
#include <string.h>

#include <wiredtiger.h>

const char *home = NULL;
const char *uri = "table:async";
int global_compact_count;

/*! [async example callback implementation] */
static int
cb_compact(WT_SESSION *session, void *cookie, int ret, uint32_t flags)
{

	(void)session;
	WT_ATOMIC_SUB(global_compact_count, 1);
	printf("Compact of %s completed: %d\n", (char *)cookie, ret);
	return (0);
}

static int
cb_cursor(WT_CURSOR *cursor, void *cookie, int ret, uint32_t flags)
{
	const char *key, *value;
	int t_ret;

	(void)cb;
	t_ret = cursor->get_key(cursor, &key);
	t_ret = cursor->get_value(cursor, &value);
	printf("Got record: %s : %s\n", key, value);
	return (t_ret);
}

/*! [async example callback implementation] */

int main(void)
{
	/*! [async example connection] */
	WT_CONNECTION *wt_conn;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	int ret;

	if ((ret = wiredtiger_open(home, NULL, "create", &conn)) != 0) {
		fprintf(stderr, "Error connecting to %s: %s\n",
		    home, wiredtiger_strerror(ret));
		return (ret);
	}
	/*! [async example connection] */

	/*! [async example table create] */
	ret = conn->open_session(wt_conn, NULL, NULL, &session);
	ret = session->create(session, uri,
	    "key_format=S,value_format=S,async=(enabled=true,threads=2)");
	/*! [async example table create] */

	/*! [async example open cursor] */
	ret = session->open_cursor(session, uri,
	    "async=(enabled=true)", &cursor);
	ret = cursor->set_async(cursor, &cb_cursor, NULL);
	/*! [async example open cursor] */

	/*! [async example insert] */
	cursor->set_key(cursor, "key1");
	cursor->set_value(cursor, "value1");
	ret = cursor->insert(cursor);
	/*! [async example insert] */

	/*! [async example wait] */
	ret = cursor->wait_async(cursor);
	/*! [async example wait] */

	/*! [async example cursor close] */
	ret = cursor->close(cursor);
	/*! [async example cursor close] */

	/*! [async example compact] */
	global_compact_count = 1;
	ret = session->compact(session, uri,
	    "timeout=0", &cb_compact, (void *)uri);
	while (global_compact_count != 0)
		sleep(1);
	/*! [async example compact] */

	/*! [async example close] */
	ret = conn->close(conn, NULL);
	/*! [async example close] */

	return (ret);
}
