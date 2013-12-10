/*-
 * Public Domain 2008-2013 WiredTiger, Inc.
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

static int   col_insert(WT_CURSOR *, WT_ITEM *, WT_ITEM *, uint64_t *);
static int   col_remove(WT_CURSOR *, WT_ITEM *, uint64_t, int *);
static int   col_update(WT_CURSOR *, WT_ITEM *, WT_ITEM *, uint64_t);
static int   nextprev(WT_CURSOR *, int, int *);
static int   notfound_chk(const char *, int, int, uint64_t);
static void *ops(void *);
static void  print_item(const char *, WT_ITEM *);
static int   read_row(WT_CURSOR *, WT_ITEM *, uint64_t);
static int   row_insert(WT_CURSOR *, WT_ITEM *, WT_ITEM *, uint64_t);
static int   row_remove(WT_CURSOR *, WT_ITEM *, uint64_t, int *);
static int   row_update(WT_CURSOR *, WT_ITEM *, WT_ITEM *, uint64_t);
static void  table_append_init(void);

/*
 * wts_ops --
 *	Perform a number of operations in a set of threads.
 */
void
wts_ops(void)
{
	TINFO *tinfo, total;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	pthread_t backup_tid, compact_tid;
	int ret, running;
	uint32_t i;

	conn = g.wts_conn;

	/*
	 * We support replay of threaded runs, but don't log random numbers
	 * after threaded operations start, there's no point.
	 */
	if (!SINGLETHREADED)
		g.rand_log_stop = 1;

	/* Initialize the table extension code. */
	table_append_init();

	/* Open a session. */
	if (g.logging != 0) {
		if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
			die(ret, "connection.open_session");
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== thread ops start ===============");
	}

	if (SINGLETHREADED) {
		memset(&total, 0, sizeof(total));
		total.id = 1;
		(void)ops(&total);
	} else {
		g.threads_finished = 0;

		/*
		 * Create thread structure; start worker, backup, compaction
		 * threads.
		 */
		if ((tinfo =
		    calloc((size_t)g.c_threads, sizeof(*tinfo))) == NULL)
			die(errno, "calloc");
		for (i = 0; i < g.c_threads; ++i) {
			tinfo[i].id = (int)i + 1;
			tinfo[i].state = TINFO_RUNNING;
			if ((ret = pthread_create(
			    &tinfo[i].tid, NULL, ops, &tinfo[i])) != 0)
				die(ret, "pthread_create");
		}
		if ((ret =
		    pthread_create(&backup_tid, NULL, hot_backup, NULL)) != 0)
			die(ret, "pthread_create");
		if (g.c_compact && (ret =
		    pthread_create(&compact_tid, NULL, compact, NULL)) != 0)
			die(ret, "pthread_create");

		/* Wait for the threads. */
		for (;;) {
			total.commit = total.deadlock = total.insert =
			    total.remove = total.rollback = total.search =
			    total.update = 0;
			for (i = 0, running = 0; i < g.c_threads; ++i) {
				total.commit += tinfo[i].commit;
				total.deadlock += tinfo[i].deadlock;
				total.insert += tinfo[i].insert;
				total.remove += tinfo[i].remove;
				total.rollback += tinfo[i].rollback;
				total.search += tinfo[i].search;
				total.update += tinfo[i].update;
				switch (tinfo[i].state) {
				case TINFO_RUNNING:
					running = 1;
					break;
				case TINFO_COMPLETE:
					tinfo[i].state = TINFO_JOINED;
					(void)pthread_join(tinfo[i].tid, NULL);
					break;
				case TINFO_JOINED:
					break;
				}
			}
			track("ops", 0ULL, &total);
			if (!running)
				break;
			(void)usleep(100000);		/* 1/10th of a second */
		}
		free(tinfo);

		/* Wait for the backup, compaction thread. */
		g.threads_finished = 1;
		(void)pthread_join(backup_tid, NULL);
		if (g.c_compact)
			(void)pthread_join(compact_tid, NULL);
	}

	if (g.logging != 0) {
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== thread ops stop ===============");
		if ((ret = session->close(session, NULL)) != 0)
			die(ret, "session.close");
	}
}

static void *
ops(void *arg)
{
	TINFO *tinfo;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor, *cursor_insert;
	WT_SESSION *session;
	WT_ITEM key, value;
	uint64_t cnt, keyno, ckpt_op, session_op, thread_ops;
	uint32_t op;
	uint8_t *keybuf, *valbuf;
	u_int np;
	int dir, insert, intxn, notfound, ret;
	char *ckpt_config, config[64];

	tinfo = arg;

	conn = g.wts_conn;
	keybuf = valbuf = NULL;

	/* Set up the default key and value buffers. */
	key_gen_setup(&keybuf);
	val_gen_setup(&valbuf);

	/*
	 * Each thread does its share of the total operations, and make sure
	 * that it's not 0 (testing runs: threads might be larger than ops).
	 */
	thread_ops = 100 + g.c_ops / g.c_threads;

	/*
	 * Select the first operation where we'll create sessions and cursors,
	 * perform checkpoint operations.
	 */
	ckpt_op = MMRAND(1, thread_ops);
	session_op = 0;

	session = NULL;
	cursor = cursor_insert = NULL;
	for (intxn = 0, cnt = 0; cnt < thread_ops; ++cnt) {
		if (SINGLETHREADED && cnt % 100 == 0)
			track("ops", 0ULL, tinfo);

		/*
		 * We can't checkpoint or swap sessions/cursors while in a
		 * transaction, resolve any running transaction.  Otherwise,
		 * reset the cursor: we may block waiting for a lock and there
		 * is no reason to keep pages pinned.
		 */
		if (cnt == ckpt_op || cnt == session_op) {
			if (intxn) {
				if ((ret = session->commit_transaction(
				    session, NULL)) != 0)
					die(ret, "session.commit_transaction");
				++tinfo->commit;
				intxn = 0;
			}
			else if (cursor != NULL &&
			    (ret = cursor->reset(cursor)) != 0)
				die(ret, "cursor.reset");
		}

		/* Open up a new session and cursors. */
		if (cnt == session_op || session == NULL || cursor == NULL) {
			if (session != NULL &&
			    (ret = session->close(session, NULL)) != 0)
				die(ret, "session.close");

			if ((ret = conn->open_session(
			    conn, NULL, NULL, &session)) != 0)
				die(ret, "connection.open_session");

			/*
			 * Open two cursors: one configured for overwriting and
			 * one configured for append if we're dealing with a
			 * column-store.
			 *
			 * The reason is when testing with existing records, we
			 * don't track if a record was deleted or not, which
			 * means we must use cursor->insert with overwriting
			 * configured.  But, in column-store files where we're
			 * testing with new, appended records, we don't want to
			 * have to specify the record number, which requires an
			 * append configuration.
			 */
			if ((ret = session->open_cursor(session,
			    g.uri, NULL, "overwrite", &cursor)) != 0)
				die(ret, "session.open_cursor");
			if ((g.type == FIX || g.type == VAR) &&
			    (ret = session->open_cursor(session,
			    g.uri, NULL, "append", &cursor_insert)) != 0)
				die(ret, "session.open_cursor");

			/* Pick the next session/cursor close/open. */
			session_op += SINGLETHREADED ?
			    MMRAND(1, thread_ops) : 100 * MMRAND(1, 50);
		}

		/* Checkpoint the database. */
		if (cnt == ckpt_op) {
			/*
			 * LSM and data-sources don't support named checkpoints,
			 * else 25% of the time we name the checkpoint.
			 */
			if (DATASOURCE("lsm") || DATASOURCE("kvsbdb") ||
			    DATASOURCE("memrata") || MMRAND(1, 4) == 1)
				ckpt_config = NULL;
			else {
				(void)snprintf(config, sizeof(config),
				    "name=thread-%d", tinfo->id);
				ckpt_config = config;
			}

			/* Named checkpoints lock out hot backups */
			if (ckpt_config != NULL &&
			    (ret = pthread_rwlock_wrlock(&g.backup_lock)) != 0)
				die(ret,
				    "pthread_rwlock_wrlock: hot-backup lock");

			if ((ret =
			    session->checkpoint(session, ckpt_config)) != 0)
				die(ret, "session.checkpoint%s%s",
				    ckpt_config == NULL ? "" : ": ",
				    ckpt_config == NULL ? "" : ckpt_config);

			if (ckpt_config != NULL &&
			    (ret = pthread_rwlock_unlock(&g.backup_lock)) != 0)
				die(ret,
				    "pthread_rwlock_wrlock: hot-backup lock");

			/*
			 * Pick the next checkpoint operation, try for roughly
			 * five checkpoint operations per thread run.
			 */
			ckpt_op += MMRAND(1, thread_ops) / 5;
		}

		/*
		 * If we're not single-threaded and we're not in a transaction,
		 * start a transaction 80% of the time.
		 */
		if (!SINGLETHREADED && !intxn && MMRAND(1, 10) >= 8) {
			if ((ret =
			    session->begin_transaction(session, NULL)) != 0)
				die(ret, "session.begin_transaction");
			intxn = 1;
		}

		insert = notfound = 0;

		keyno = MMRAND(1, g.rows);
		key.data = keybuf;
		value.data = valbuf;

		/*
		 * Perform some number of operations: the percentage of deletes,
		 * inserts and writes are specified, reads are the rest.  The
		 * percentages don't have to add up to 100, a high percentage
		 * of deletes will mean fewer inserts and writes.  Modifications
		 * are always followed by a read to confirm it worked.
		 */
		op = (uint32_t)(rng() % 100);
		if (op < g.c_delete_pct) {
			++tinfo->remove;
			switch (g.type) {
			case ROW:
				/*
				 * If deleting a non-existent record, the cursor
				 * won't be positioned, and so can't do a next.
				 */
				if (row_remove(cursor, &key, keyno, &notfound))
					goto deadlock;
				break;
			case FIX:
			case VAR:
				if (col_remove(cursor, &key, keyno, &notfound))
					goto deadlock;
				break;
			}
		} else if (op < g.c_delete_pct + g.c_insert_pct) {
			++tinfo->insert;
			switch (g.type) {
			case ROW:
				if (row_insert(cursor, &key, &value, keyno))
					goto deadlock;
				insert = 1;
				break;
			case FIX:
			case VAR:
				/*
				 * We can only append so many new records, if
				 * we've reached that limit, update a record
				 * instead of doing an insert.
				 */
				if (g.append_cnt >= g.append_max)
					goto skip_insert;

				/*
				 * Reset the standard cursor so it doesn't keep
				 * pages pinned.
				 */
				if ((ret = cursor->reset(cursor)) != 0)
					die(ret, "cursor.reset");

				/* Insert, then reset the insert cursor. */
				if (col_insert(
				    cursor_insert, &key, &value, &keyno))
					goto deadlock;
				if ((ret =
				    cursor_insert->reset(cursor_insert)) != 0)
					die(ret, "cursor.reset");

				insert = 1;
				break;
			}
		} else if (
		    op < g.c_delete_pct + g.c_insert_pct + g.c_write_pct) {
			++tinfo->update;
			switch (g.type) {
			case ROW:
				if (row_update(cursor, &key, &value, keyno))
					goto deadlock;
				break;
			case FIX:
			case VAR:
skip_insert:			if (col_update(cursor, &key, &value, keyno))
					goto deadlock;
				break;
			}
		} else {
			++tinfo->search;
			if (read_row(cursor, &key, keyno))
				goto deadlock;
			continue;
		}

		/*
		 * The cursor is positioned if we did any operation other than
		 * insert, do a small number of next/prev cursor operations in
		 * a random direction.
		 */
		if (!insert) {
			dir = (int)MMRAND(0, 1);
			for (np = 0; np < MMRAND(1, 8); ++np) {
				if (notfound)
					break;
				if (nextprev(cursor, dir, &notfound))
					goto deadlock;
			}
		}

		/* Read the value we modified to confirm the operation. */
		++tinfo->search;
		if (read_row(cursor, &key, keyno))
			goto deadlock;

		/*
		 * If we're in the transaction, commit 40% of the time and
		 * rollback 10% of the time.
		 */
		if (intxn)
			switch (MMRAND(1, 10)) {
			case 1: case 2: case 3: case 4:		/* 40% */
				if ((ret = session->commit_transaction(
				    session, NULL)) != 0)
					die(ret, "session.commit_transaction");
				++tinfo->commit;
				intxn = 0;
				break;
			case 5:					/* 10% */
				if (0) {
deadlock:				++tinfo->deadlock;
				}
				if ((ret = session->rollback_transaction(
				    session, NULL)) != 0)
					die(ret, "session.commit_transaction");
				++tinfo->rollback;
				intxn = 0;
				break;
			default:
				break;
			}
	}

	if (session != NULL && (ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");

	free(keybuf);
	free(valbuf);

	tinfo->state = TINFO_COMPLETE;
	return (NULL);
}

/*
 * wts_read_scan --
 *	Read and verify all elements in a file.
 */
void
wts_read_scan(void)
{
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_ITEM key;
	WT_SESSION *session;
	uint64_t cnt, last_cnt;
	uint8_t *keybuf;
	int ret;

	conn = g.wts_conn;

	/* Set up the default key buffer. */
	key_gen_setup(&keybuf);

	/* Open a session and cursor pair. */
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");
	if ((ret = session->open_cursor(
	    session, g.uri, NULL, NULL, &cursor)) != 0)
		die(ret, "session.open_cursor");

	/* Check a random subset of the records using the key. */
	for (last_cnt = cnt = 0; cnt < g.key_cnt;) {
		cnt += rng() % 17 + 1;
		if (cnt > g.rows)
			cnt = g.rows;
		if (cnt - last_cnt > 1000) {
			track("read row scan", cnt, NULL);
			last_cnt = cnt;
		}

		key.data = keybuf;
		if ((ret = read_row(cursor, &key, cnt)) != 0)
			die(ret, "read_scan");
	}

	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");

	free(keybuf);
}

/*
 * read_row --
 *	Read and verify a single element in a row- or column-store file.
 */
static int
read_row(WT_CURSOR *cursor, WT_ITEM *key, uint64_t keyno)
{
	WT_ITEM bdb_value, value;
	WT_SESSION *session;
	int notfound, ret;
	uint8_t bitfield;

	session = cursor->session;

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api,
		    session, "%-10s%" PRIu64, "read", keyno);

	/* Retrieve the key/value pair by key. */
	switch (g.type) {
	case FIX:
	case VAR:
		cursor->set_key(cursor, keyno);
		break;
	case ROW:
		key_gen((uint8_t *)key->data, &key->size, keyno, 0);
		cursor->set_key(cursor, key);
		break;
	}

	if ((ret = cursor->search(cursor)) == 0) {
		if (g.type == FIX) {
			ret = cursor->get_value(cursor, &bitfield);
			value.data = &bitfield;
			value.size = 1;
		} else {
			ret = cursor->get_value(cursor, &value);
		}
	}
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "read_row: read row %" PRIu64, keyno);
	/*
	 * In fixed length stores, zero values at the end of the key space are
	 * returned as not found.  Treat this the same as a zero value in the
	 * key space, to match BDB's behavior.
	 */
	if (ret == WT_NOTFOUND && g.type == FIX) {
		bitfield = 0;
		value.data = &bitfield;
		value.size = 1;
		ret = 0;
	}

	if (!SINGLETHREADED)
		return (0);

	/* Retrieve the BDB value. */
	bdb_read(keyno, &bdb_value.data, &bdb_value.size, &notfound);

	/* Check for not-found status. */
	if (notfound_chk("read_row", ret, notfound, keyno))
		return (0);

	/* Compare the two. */
	if (value.size != bdb_value.size ||
	    memcmp(value.data, bdb_value.data, value.size) != 0) {
		fprintf(stderr,
		    "read_row: read row value mismatch %" PRIu64 ":\n", keyno);
		print_item("bdb", &bdb_value);
		print_item(" wt", &value);
		die(0, NULL);
	}
	return (0);
}

/*
 * nextprev --
 *	Read and verify the next/prev element in a row- or column-store file.
 */
static int
nextprev(WT_CURSOR *cursor, int next, int *notfoundp)
{
	WT_ITEM key, value, bdb_key, bdb_value;
	WT_SESSION *session;
	uint64_t keyno;
	int notfound, ret;
	uint8_t bitfield;
	const char *which;
	char *p;

	session = cursor->session;
	which = next ? "next" : "prev";

	keyno = 0;
	ret = next ? cursor->next(cursor) : cursor->prev(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret == 0)
		switch (g.type) {
		case FIX:
			if ((ret = cursor->get_key(cursor, &keyno)) == 0 &&
			    (ret = cursor->get_value(cursor, &bitfield)) == 0) {
				value.data = &bitfield;
				value.size = 1;
			}
			break;
		case ROW:
			if ((ret = cursor->get_key(cursor, &key)) == 0)
				ret = cursor->get_value(cursor, &value);
			break;
		case VAR:
			if ((ret = cursor->get_key(cursor, &keyno)) == 0)
				ret = cursor->get_value(cursor, &value);
			break;
		}
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "%s", which);
	*notfoundp = (ret == WT_NOTFOUND);

	if (!SINGLETHREADED)
		return (0);

	/* Retrieve the BDB value. */
	bdb_np(next, &bdb_key.data, &bdb_key.size,
	    &bdb_value.data, &bdb_value.size, &notfound);
	if (notfound_chk(
	    next ? "nextprev(next)" : "nextprev(prev)", ret, notfound, keyno))
		return (0);

	/* Compare the two. */
	if (g.type == ROW) {
		if (key.size != bdb_key.size ||
		    memcmp(key.data, bdb_key.data, key.size) != 0) {
			fprintf(stderr, "nextprev: %s key mismatch:\n", which);
			print_item("bdb-key", &bdb_key);
			print_item(" wt-key", &key);
			die(0, NULL);
		}
	} else {
		if (keyno != (uint64_t)atoll(bdb_key.data)) {
			if ((p = strchr((char *)bdb_key.data, '.')) != NULL)
				*p = '\0';
			fprintf(stderr,
			    "nextprev: %s key mismatch: %.*s != %" PRIu64 "\n",
			    which,
			    (int)bdb_key.size, (char *)bdb_key.data, keyno);
			die(0, NULL);
		}
	}
	if (value.size != bdb_value.size ||
	    memcmp(value.data, bdb_value.data, value.size) != 0) {
		fprintf(stderr, "nextprev: %s value mismatch:\n", which);
		print_item("bdb-value", &bdb_value);
		print_item(" wt-value", &value);
		die(0, NULL);
	}

	if (g.logging == LOG_OPS)
		switch (g.type) {
		case FIX:
			(void)g.wt_api->msg_printf(g.wt_api,
			    session, "%-10s%" PRIu64 " {0x%02x}", which,
			    keyno, ((char *)value.data)[0]);
			break;
		case ROW:
			(void)g.wt_api->msg_printf(
			    g.wt_api, session, "%-10s{%.*s/%.*s}", which,
			    (int)key.size, (char *)key.data,
			    (int)value.size, (char *)value.data);
			break;
		case VAR:
			(void)g.wt_api->msg_printf(g.wt_api, session,
			    "%-10s%" PRIu64 " {%.*s}", which,
			    keyno, (int)value.size, (char *)value.data);
			break;
		}
	return (0);
}

/*
 * row_update --
 *	Update a row in a row-store file.
 */
static int
row_update(
    WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value, uint64_t keyno)
{
	WT_SESSION *session;
	int notfound, ret;

	session = cursor->session;

	key_gen((uint8_t *)key->data, &key->size, keyno, 0);
	value_gen((uint8_t *)value->data, &value->size, keyno);

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "%-10s{%.*s}\n%-10s{%.*s}",
		    "putK", (int)key->size, (char *)key->data,
		    "putV", (int)value->size, (char *)value->data);

	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	ret = cursor->update(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "row_update: update row %" PRIu64 " by key", keyno);

	if (!SINGLETHREADED)
		return (0);

	bdb_update(key->data, key->size, value->data, value->size, &notfound);
	(void)notfound_chk("row_update", ret, notfound, keyno);
	return (0);
}

/*
 * col_update --
 *	Update a row in a column-store file.
 */
static int
col_update(WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value, uint64_t keyno)
{
	WT_SESSION *session;
	int notfound, ret;

	session = cursor->session;

	value_gen((uint8_t *)value->data, &value->size, keyno);

	/* Log the operation */
	if (g.logging == LOG_OPS) {
		if (g.type == FIX)
			(void)g.wt_api->msg_printf(g.wt_api, session,
			    "%-10s%" PRIu64 " {0x%02" PRIx8 "}",
			    "update", keyno,
			    ((uint8_t *)value->data)[0]);
		else
			(void)g.wt_api->msg_printf(g.wt_api, session,
			    "%-10s%" PRIu64 " {%.*s}",
			    "update", keyno,
			    (int)value->size, (char *)value->data);
	}

	cursor->set_key(cursor, keyno);
	if (g.type == FIX)
		cursor->set_value(cursor, *(uint8_t *)value->data);
	else
		cursor->set_value(cursor, value);
	ret = cursor->update(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "col_update: %" PRIu64, keyno);

	if (!SINGLETHREADED)
		return (0);

	key_gen((uint8_t *)key->data, &key->size, keyno, 0);
	bdb_update(key->data, key->size, value->data, value->size, &notfound);
	(void)notfound_chk("col_update", ret, notfound, keyno);
	return (0);
}

/*
 * table_append_init --
 *	Re-initialize the appended records list.
 */
static void
table_append_init(void)
{
	/* Append up to 10 records per thread before waiting on resolution. */
	g.append_max = (size_t)g.c_threads * 10;
	g.append_cnt = 0;

	if (g.append != NULL) {
		free(g.append);
		g.append = NULL;
	}
	if ((g.append = calloc(g.append_max, sizeof(uint64_t))) == NULL)
		die(errno, "calloc");
}

/*
 * table_append --
 *	Resolve the appended records.
 */
static void
table_append(uint64_t keyno)
{
	uint64_t *p, *ep;
	int done, ret;

	ep = g.append + g.append_max;

	/*
	 * We don't want to ignore records we append, which requires we update
	 * the "last row" as we insert new records.   Threads allocating record
	 * numbers can race with other threads, so the thread allocating record
	 * N may return after the thread allocating N + 1.  We can't update a
	 * record before it's been inserted, and so we can't leave gaps when the
	 * count of records in the table is incremented.
	 *
	 * The solution is the append table, which contains an unsorted list of
	 * appended records.  Every time we finish appending a record, process
	 * the table, trying to update the total records in the object.
	 *
	 * First, enter the new key into the append list.
	 *
	 * It's technically possible to race: we allocated space for 10 records
	 * per thread, but the check for the maximum number of records being
	 * appended doesn't lock.  If a thread allocated a new record and went
	 * to sleep (so the append table fills up), then N threads of control
	 * used the same g.append_cnt value to decide there was an available
	 * slot in the append table and both allocated new records, we could run
	 * out of space in the table.   It's unfortunately not even unlikely in
	 * the case of a large number of threads all inserting as fast as they
	 * can and a single thread going to sleep for an unexpectedly long time.
	 * If it happens, sleep and retry until earlier records are resolved
	 * and we find a slot.
	 */
	for (done = 0;;) {
		if ((ret = pthread_rwlock_wrlock(&g.append_lock)) != 0)
			die(ret, "pthread_rwlock_wrlock: append_lock");

		/*
		 * If this is the thread we've been waiting for, and its record
		 * won't fit, we'd loop infinitely.  If there are many append
		 * operations and a thread goes to sleep for a little too long,
		 * it can happen.
		 */
		if (keyno == g.rows + 1) {
			g.rows = keyno;
			done = 1;

			/*
			 * Clean out the table, incrementing the total count of
			 * records until we don't find the next key.
			 */
			for (;;) {
				for (p = g.append; p < ep; ++p)
					if (*p == g.rows + 1) {
						g.rows = *p;
						*p = 0;
						--g.append_cnt;
						break;
					}
				if (p == ep)
					break;
			}
		} else
			/* Enter the key into the table. */
			for (p = g.append; p < ep; ++p)
				if (*p == 0) {
					*p = keyno;
					++g.append_cnt;
					done = 1;
					break;
				}

		if ((ret = pthread_rwlock_unlock(&g.append_lock)) != 0)
			die(ret, "pthread_rwlock_unlock: append_lock");

		if (done)
			break;
		sleep(1);
	}
}

/*
 * row_insert --
 *	Insert a row in a row-store file.
 */
static int
row_insert(
    WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value, uint64_t keyno)
{
	WT_SESSION *session;
	int notfound, ret;

	session = cursor->session;

	key_gen((uint8_t *)key->data, &key->size, keyno, 1);
	value_gen((uint8_t *)value->data, &value->size, keyno);

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "%-10s{%.*s}\n%-10s{%.*s}",
		    "insertK", (int)key->size, (char *)key->data,
		    "insertV", (int)value->size, (char *)value->data);

	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	ret = cursor->insert(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "row_insert: insert row %" PRIu64 " by key", keyno);

	if (!SINGLETHREADED)
		return (0);

	bdb_update(key->data, key->size, value->data, value->size, &notfound);
	(void)notfound_chk("row_insert", ret, notfound, keyno);
	return (0);
}

/*
 * col_insert --
 *	Insert an element in a column-store file.
 */
static int
col_insert(WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value, uint64_t *keynop)
{
	WT_SESSION *session;
	uint64_t keyno;
	int notfound, ret;

	session = cursor->session;

	value_gen((uint8_t *)value->data, &value->size, g.rows + 1);

	if (g.type == FIX)
		cursor->set_value(cursor, *(uint8_t *)value->data);
	else
		cursor->set_value(cursor, value);
	if ((ret = cursor->insert(cursor)) != 0) {
		if (ret == WT_DEADLOCK)
			return (WT_DEADLOCK);
		die(ret, "cursor.insert");
	}
	if ((ret = cursor->get_key(cursor, &keyno)) != 0)
		die(ret, "cursor.get_key");
	*keynop = (uint32_t)keyno;

	table_append(keyno);			/* Extend the object. */

	if (g.logging == LOG_OPS) {
		if (g.type == FIX)
			(void)g.wt_api->msg_printf(g.wt_api, session,
			    "%-10s%" PRIu64 " {0x%02" PRIx8 "}",
			    "insert", keyno,
			    ((uint8_t *)value->data)[0]);
		else
			(void)g.wt_api->msg_printf(g.wt_api, session,
			    "%-10s%" PRIu64 " {%.*s}",
			    "insert", keyno,
			    (int)value->size, (char *)value->data);
	}

	if (!SINGLETHREADED)
		return (0);

	key_gen((uint8_t *)key->data, &key->size, keyno, 0);
	bdb_update(key->data, key->size, value->data, value->size, &notfound);
	return (0);
}

/*
 * row_remove --
 *	Remove an row from a row-store file.
 */
static int
row_remove(WT_CURSOR *cursor, WT_ITEM *key, uint64_t keyno, int *notfoundp)
{
	WT_SESSION *session;
	int notfound, ret;

	session = cursor->session;

	key_gen((uint8_t *)key->data, &key->size, keyno, 0);

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(
		    g.wt_api, session, "%-10s%" PRIu64, "remove", keyno);

	cursor->set_key(cursor, key);
	/* We use the cursor in overwrite mode, check for existence. */
	if ((ret = cursor->search(cursor)) == 0)
		ret = cursor->remove(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "row_remove: remove %" PRIu64 " by key", keyno);
	*notfoundp = (ret == WT_NOTFOUND);

	if (!SINGLETHREADED)
		return (0);

	bdb_remove(keyno, &notfound);
	(void)notfound_chk("row_remove", ret, notfound, keyno);
	return (0);
}

/*
 * col_remove --
 *	Remove a row from a column-store file.
 */
static int
col_remove(WT_CURSOR *cursor, WT_ITEM *key, uint64_t keyno, int *notfoundp)
{
	WT_SESSION *session;
	int notfound, ret;

	session = cursor->session;

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(
		    g.wt_api, session, "%-10s%" PRIu64, "remove", keyno);

	cursor->set_key(cursor, keyno);
	/* We use the cursor in overwrite mode, check for existence. */
	if ((ret = cursor->search(cursor)) == 0)
		ret = cursor->remove(cursor);
	if (ret == WT_DEADLOCK)
		return (WT_DEADLOCK);
	if (ret != 0 && ret != WT_NOTFOUND)
		die(ret, "col_remove: remove %" PRIu64 " by key", keyno);
	*notfoundp = (ret == WT_NOTFOUND);

	if (!SINGLETHREADED)
		return (0);

	/*
	 * Deleting a fixed-length item is the same as setting the bits to 0;
	 * do the same thing for the BDB store.
	 */
	if (g.type == FIX) {
		key_gen((uint8_t *)key->data, &key->size, keyno, 0);
		bdb_update(key->data, key->size, "\0", 1, &notfound);
	} else
		bdb_remove(keyno, &notfound);
	(void)notfound_chk("col_remove", ret, notfound, keyno);
	return (0);
}

/*
 * notfound_chk --
 *	Compare notfound returns for consistency.
 */
static int
notfound_chk(const char *f, int wt_ret, int bdb_notfound, uint64_t keyno)
{
	/* Check for not found status. */
	if (bdb_notfound && wt_ret == WT_NOTFOUND)
		return (1);

	if (bdb_notfound) {
		fprintf(stderr, "%s: %s:", g.progname, f);
		if (keyno != 0)
			fprintf(stderr, " row %" PRIu64 ":", keyno);
		fprintf(stderr,
		    " not found in Berkeley DB, found in WiredTiger\n");
		die(0, NULL);
	}
	if (wt_ret == WT_NOTFOUND) {
		fprintf(stderr, "%s: %s:", g.progname, f);
		if (keyno != 0)
			fprintf(stderr, " row %" PRIu64 ":", keyno);
		fprintf(stderr,
		    " found in Berkeley DB, not found in WiredTiger\n");
		die(0, NULL);
	}
	return (0);
}

/*
 * print_item --
 *	Display a single data/size pair, with a tag.
 */
static void
print_item(const char *tag, WT_ITEM *item)
{
	static const char hex[] = "0123456789abcdef";
	const uint8_t *data;
	uint32_t size;
	int ch;

	data = item->data;
	size = item->size;

	fprintf(stderr, "\t%s {", tag);
	if (g.type == FIX)
		fprintf(stderr, "0x%02x", data[0]);
	else
		for (; size > 0; --size, ++data) {
			ch = data[0];
			if (isprint(ch))
				fprintf(stderr, "%c", ch);
			else
				fprintf(stderr, "%x%x",
				    hex[(data[0] & 0xf0) >> 4],
				    hex[data[0] & 0x0f]);
		}
	fprintf(stderr, "}\n");
}
