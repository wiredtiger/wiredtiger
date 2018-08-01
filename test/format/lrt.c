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

#include "format.h"

/*
 * lrt --
 *	Start a long-running transaction.
 */
WT_THREAD_RET
lrt(void *arg)
{
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_ITEM key, value;
	WT_SESSION *session;
	size_t buf_len, buf_size;
	uint64_t keyno, saved_keyno;
	uint8_t bitfield;
	u_int period;
	int pinned, ret;
	void *buf;

	(void)(arg);			/* Unused parameter */

	saved_keyno = 0;		/* [-Werror=maybe-uninitialized] */

	key_gen_init(&key);
	val_gen_init(&value);

	buf = NULL;
	buf_len = buf_size = 0;

	/* Open a session and cursor. */
	conn = g.wts_conn;
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	/*
	 * open_cursor can return EBUSY if concurrent with a metadata
	 * operation, retry in that case.
	 */
	while ((ret = session->open_cursor(
	    session, g.uri, NULL, NULL, &cursor)) == EBUSY)
		__wt_yield();
	testutil_check(ret);

	for (pinned = 0;;) {
		if (pinned) {
			/* Re-read the record at the end of the table. */
			while ((ret = read_row_worker(cursor,
			    saved_keyno, &key, &value, false)) == WT_ROLLBACK)
				;
			if (ret != 0)
				testutil_die(ret,
				    "read_row_worker %" PRIu64, saved_keyno);

			/* Compare the previous value with the current one. */
			if (g.type == FIX) {
				ret = cursor->get_value(cursor, &bitfield);
				value.data = &bitfield;
				value.size = 1;
			} else
				ret = cursor->get_value(cursor, &value);
			if (ret != 0)
				testutil_die(ret,
				    "cursor.get_value: %" PRIu64, saved_keyno);

			if (buf_size != value.size ||
			    memcmp(buf, value.data, value.size) != 0)
				testutil_die(0, "mismatched start/stop values");

			/* End the transaction. */
			testutil_check(
			    session->commit_transaction(session, NULL));

			/* Reset the cursor, releasing our pin. */
			testutil_check(cursor->reset(cursor));
			pinned = 0;
		} else {
			/*
			 * Test named snapshots: create a snapshot, wait to
			 * give the transaction state time to move forward,
			 * then start a transaction with the named snapshot,
			 * drop it, then commit the transaction. This exercises
			 * most of the named snapshot logic under load.
			 */
			testutil_check(session->snapshot(session, "name=test"));
			__wt_sleep(1, 0);
			/*
			 * Keep trying to start a new transaction if it's
			 * timing out - we know there aren't any resources
			 * pinned so it should succeed eventually.
			 */
			while ((ret = session->begin_transaction(
			    session, "snapshot=test")) == WT_CACHE_FULL)
				;
			testutil_check(ret);
			testutil_check(session->snapshot(
			    session, "drop=(all)"));
			testutil_check(session->commit_transaction(
			    session, NULL));

			/*
			 * Begin transaction: without an explicit transaction,
			 * the snapshot is only kept around while a cursor is
			 * positioned. As soon as the cursor loses its position
			 * a new snapshot will be allocated.
			 */
			while ((ret = session->begin_transaction(
			    session, "snapshot=snapshot")) == WT_CACHE_FULL)
				;
			testutil_check(ret);

			/* Read a record at the end of the table. */
			do {
				saved_keyno = mmrand(NULL,
				    (u_int)(g.key_cnt - g.key_cnt / 10),
				    (u_int)g.key_cnt);
				while ((ret = read_row_worker(cursor,
				    saved_keyno,
				    &key, &value, false)) == WT_ROLLBACK)
					;
			} while (ret == WT_NOTFOUND);
			if (ret != 0)
				testutil_die(ret,
				    "read_row_worker %" PRIu64, saved_keyno);

			/* Copy the cursor's value. */
			if (g.type == FIX) {
				ret = cursor->get_value(cursor, &bitfield);
				value.data = &bitfield;
				value.size = 1;
			} else
				ret = cursor->get_value(cursor, &value);
			if (ret != 0)
				testutil_die(ret,
				    "cursor.get_value: %" PRIu64, saved_keyno);
			if (buf_len < value.size)
				buf = drealloc(buf, buf_len = value.size);
			memcpy(buf, value.data, buf_size = value.size);

			/*
			 * Move the cursor to an early record in the table,
			 * hopefully allowing the page with the record just
			 * retrieved to be evicted from memory.
			 */
			do {
				keyno = mmrand(NULL, 1, (u_int)g.key_cnt / 5);
				while ((ret = read_row_worker(cursor,
				    keyno, &key, &value, false)) == WT_ROLLBACK)
					;
			} while (ret == WT_NOTFOUND);
			if (ret != 0)
				testutil_die(ret,
				    "read_row_worker %" PRIu64, keyno);

			pinned = 1;
		}

		/* Sleep for some number of seconds. */
		period = mmrand(NULL, 1, 10);

		/* Sleep for short periods so we don't make the run wait. */
		while (period > 0 && !g.workers_finished) {
			--period;
			__wt_sleep(1, 0);
		}
		if (g.workers_finished)
			break;
	}

	testutil_check(session->close(session, NULL));

	key_gen_teardown(&key);
	val_gen_teardown(&value);
	free(buf);

	return (WT_THREAD_RET_VALUE);
}
