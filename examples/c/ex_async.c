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

#include <wiredtiger_async.h>

const char *home = NULL;

/*! [async example callback implementation] */
static void cb_complete(WT_ASYNC_CALLBACK *cb, WT_ASYNC_OP *op, int ret)
{
	(void)cb;
	(void)op;
	printf("Operation completed: %d\n", ret);
}

static int cb_next(WT_ASYNC_CALLBACK *cb, WT_ASYNC_OP *op)
{
	const char *key, *value;
	int ret;

	(void)cb;
	ret = op->ds->get_key(op->ds, op, &key);
	ret = op->ds->get_value(op->ds, op, &value);
	printf("Got record: %s : %s\n", key, value);
	return (ret);
}

static WT_ASYNC_CALLBACK cb = { cb_complete, cb_next };
/*! [async example callback implementation] */

int main(void)
{
	/*! [async example connection] */
	WT_ASYNC_CONNECTION *conn;
	WT_ASYNC_DS *ds;
	WT_ASYNC_OP *op;
	WT_CONNECTION *wt_conn;
	WT_SESSION *session;
	int ret;

	if ((ret = wiredtiger_async_open(home, NULL, "create", &conn)) != 0) {
		fprintf(stderr, "Error connecting to %s: %s\n",
		    home, wiredtiger_strerror(ret));
		return (ret);
	}
	/*! [async example connection] */

	/*! [async example table create] */
	ret = conn->get_conn(conn, &wt_conn);
	ret = wt_conn->open_session(wt_conn, NULL, NULL, &session);
	ret = session->create(session,
	    "table:async", "key_format=S,value_format=S");
	/*! [async example table create] */

	/*! [async example open data source] */
	ret = conn->open_data_source(conn,
	    "table:async", NULL, &ds);
	ret = ds->alloc_op(ds, &op);
	/*! [async example open data source] */

	/*! [async example insert] */
	ds->set_key(ds, op, "key1");
	ds->set_value(ds, op, "value1");
	ret = ds->execute(ds, op, WT_AOP_INSERT, &cb);
	/*! [async example insert] */

	/*! [async example wait] */
	ret = ds->wait(ds, op);
	/*! [async example wait] */

	/*! [async example scan] */
	ret = ds->execute(ds, op, WT_AOP_SCAN, &cb);
	/*! [async example scan] */

	/*! [async example close] */
	ret = conn->close(conn, NULL);
	/*! [async example close] */

	return (ret);
}
