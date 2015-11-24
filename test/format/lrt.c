/*-
 * Public Domain 2014-2015 MongoDB, Inc.
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
void *
lrt(void *arg)
{
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_ITEM key, value;
	WT_SESSION *session;
	size_t buf_len, buf_size;
	uint64_t keyno, saved_keyno;
	u_int period;
	int pinned, ret;
	uint8_t bitfield, *keybuf;
	void *buf;

	(void)(arg);			/* Unused parameter */

	saved_keyno = 0;		/* [-Werror=maybe-uninitialized] */

	key_gen_setup(&keybuf);
	memset(&key, 0, sizeof(key));
	key.data = keybuf;
	memset(&value, 0, sizeof(value));

	buf = NULL;
	buf_len = buf_size = 0;

	/* Open a session and cursor. */
	conn = g.wts_conn;
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");
	if ((ret = session->open_cursor(
	    session, g.uri, NULL, NULL, &cursor)) != 0)
		die(ret, "session.open_cursor");

	for (pinned = 0;;) {
		if (pinned) {
			/* Re-read the record at the end of the table. */
			while ((ret = read_row(cursor,
			    &key, saved_keyno, 1)) == WT_ROLLBACK)
				;
			if (ret != 0)
				die(ret, "read_row %" PRIu64, saved_keyno);

			/* Compare the previous value with the current one. */
			if (g.type == FIX) {
				ret = cursor->get_value(cursor, &bitfield);
				value.data = &bitfield;
				value.size = 1;
			} else
				ret = cursor->get_value(cursor, &value);
			if (ret != 0)
				die(ret,
				    "cursor.get_value: %" PRIu64, saved_keyno);

			if (buf_size != value.size ||
			    memcmp(buf, value.data, value.size) != 0)
				die(0, "mismatched start/stop values");

			/* End the transaction. */
			if ((ret =
			    session->commit_transaction(session, NULL)) != 0)
				die(ret, "session.commit_transaction");

			/* Reset the cursor, releasing our pin. */
			if ((ret = cursor->reset(cursor)) != 0)
				die(ret, "cursor.reset");
			pinned = 0;
		} else {
			/*
			 * Begin transaction: without an explicit transaction,
			 * the snapshot is only kept around while a cursor is
			 * positioned. As soon as the cursor loses its position
			 * a new snapshot will be allocated.
			 */
			if ((ret = session->begin_transaction(
			    session, "isolation=snapshot")) != 0)
				die(ret, "session.begin_transaction");

			/* Read a record at the end of the table. */
			do {
				saved_keyno = mmrand(NULL,
				    (u_int)(g.key_cnt - g.key_cnt / 10),
				    (u_int)g.key_cnt);
				while ((ret = read_row(cursor,
				    &key, saved_keyno, 1)) == WT_ROLLBACK)
					;
			} while (ret == WT_NOTFOUND);
			if (ret != 0)
				die(ret, "read_row %" PRIu64, saved_keyno);

			/* Copy the cursor's value. */
			if (g.type == FIX) {
				ret = cursor->get_value(cursor, &bitfield);
				value.data = &bitfield;
				value.size = 1;
			} else
				ret = cursor->get_value(cursor, &value);
			if (ret != 0)
				die(ret,
				    "cursor.get_value: %" PRIu64, saved_keyno);
			if (buf_len < value.size &&
			    (buf = realloc(buf, buf_len = value.size)) == NULL)
				die(errno, "malloc");
			memcpy(buf, value.data, buf_size = value.size);

			/*
			 * Move the cursor to an early record in the table,
			 * hopefully allowing the page with the record just
			 * retrieved to be evicted from memory.
			 */
			do {
				keyno = mmrand(NULL, 1, (u_int)g.key_cnt / 5);
				while ((ret = read_row(cursor,
				    &key, keyno, 1)) == WT_ROLLBACK)
					;
			} while (ret == WT_NOTFOUND);
			if (ret != 0)
				die(ret, "read_row %" PRIu64, keyno);

			pinned = 1;
		}

		/* Sleep for some number of seconds. */
		period = mmrand(NULL, 1, 10);

		/* Sleep for short periods so we don't make the run wait. */
		while (period > 0 && !g.workers_finished) {
			--period;
			sleep(1);
		}
		if (g.workers_finished)
			break;
	}

	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");

	free(keybuf);
	free(buf);

	return (NULL);
}
