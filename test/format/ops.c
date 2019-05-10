/*-
 * Public Domain 2014-2019 MongoDB, Inc.
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

static int   col_insert(TINFO *, WT_CURSOR *);
static int   col_modify(TINFO *, WT_CURSOR *, bool);
static int   col_remove(TINFO *, WT_CURSOR *, bool);
static int   col_reserve(TINFO *, WT_CURSOR *, bool);
static int   col_truncate(TINFO *, WT_CURSOR *);
static int   col_update(TINFO *, WT_CURSOR *, bool);
static int   nextprev(TINFO *, WT_CURSOR *, bool);
static WT_THREAD_RET ops(void *);
static int   read_row(TINFO *, WT_CURSOR *);
static int   row_insert(TINFO *, WT_CURSOR *, bool);
static int   row_modify(TINFO *, WT_CURSOR *, bool);
static int   row_remove(TINFO *, WT_CURSOR *, bool);
static int   row_reserve(TINFO *, WT_CURSOR *, bool);
static int   row_truncate(TINFO *, WT_CURSOR *);
static int   row_update(TINFO *, WT_CURSOR *, bool);
static void  table_append_init(void);

#ifdef HAVE_BERKELEY_DB
static int   notfound_chk(const char *, int, int, uint64_t);
#endif

static char modify_repl[256];

/*
 * modify_repl_init --
 *	Initialize the replacement information.
 */
static void
modify_repl_init(void)
{
	size_t i;

	for (i = 0; i < sizeof(modify_repl); ++i)
		modify_repl[i] = "zyxwvutsrqponmlkjihgfedcba"[i % 26];
}

static void
set_alarm(void)
{
#ifdef HAVE_TIMER_CREATE
	struct itimerspec timer_val;
	timer_t timer_id;

	testutil_check(timer_create(CLOCK_REALTIME, NULL, &timer_id));
	memset(&timer_val, 0, sizeof(timer_val));
	timer_val.it_value.tv_sec = 60 * 2;
	timer_val.it_value.tv_nsec = 0;
	testutil_check(timer_settime(timer_id, 0, &timer_val, NULL));
#endif
}

/*
 * wts_ops --
 *	Perform a number of operations in a set of threads.
 */
void
wts_ops(int lastrun)
{
	TINFO **tinfo_list, *tinfo, total;
	WT_CONNECTION *conn;
	WT_SESSION *session;
	wt_thread_t alter_tid, backup_tid, checkpoint_tid, compact_tid, lrt_tid;
	wt_thread_t timestamp_tid;
	int64_t fourths, quit_fourths, thread_ops;
	uint32_t i;
	bool running;

	conn = g.wts_conn;

	session = NULL;			/* -Wconditional-uninitialized */
	memset(&alter_tid, 0, sizeof(alter_tid));
	memset(&backup_tid, 0, sizeof(backup_tid));
	memset(&checkpoint_tid, 0, sizeof(checkpoint_tid));
	memset(&compact_tid, 0, sizeof(compact_tid));
	memset(&lrt_tid, 0, sizeof(lrt_tid));
	memset(&timestamp_tid, 0, sizeof(timestamp_tid));

	modify_repl_init();

	/*
	 * There are two mechanisms to specify the length of the run, a number
	 * of operations and a timer, when either expire the run terminates.
	 *
	 * Each thread does an equal share of the total operations (and make
	 * sure that it's not 0).
	 *
	 * Calculate how many fourth-of-a-second sleeps until the timer expires.
	 * If the timer expires and threads don't return in 15 minutes, assume
	 * there is something hung, and force the quit.
	 */
	if (g.c_ops == 0)
		thread_ops = -1;
	else {
		if (g.c_ops < g.c_threads)
			g.c_ops = g.c_threads;
		thread_ops = g.c_ops / g.c_threads;
	}
	if (g.c_timer == 0)
		fourths = quit_fourths = -1;
	else {
		fourths = ((int64_t)g.c_timer * 4 * 60) / FORMAT_OPERATION_REPS;
		quit_fourths = fourths + 15 * 4 * 60;
	}

	/* Initialize the table extension code. */
	table_append_init();

	/*
	 * We support replay of threaded runs, but don't log random numbers
	 * after threaded operations start, there's no point.
	 */
	if (!SINGLETHREADED)
		g.rand_log_stop = true;

	/* Logging requires a session. */
	if (g.logging != 0) {
		testutil_check(conn->open_session(conn, NULL, NULL, &session));
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== thread ops start ===============");
	}

	/*
	 * Create the per-thread structures and start the worker threads.
	 * Allocate the thread structures separately to minimize false sharing.
	 */
	tinfo_list = dcalloc((size_t)g.c_threads, sizeof(TINFO *));
	for (i = 0; i < g.c_threads; ++i) {
		tinfo_list[i] = tinfo = dcalloc(1, sizeof(TINFO));

		tinfo->id = (int)i + 1;

		/*
		 * Characterize the per-thread random number generator. Normally
		 * we want independent behavior so threads start in different
		 * parts of the RNG space, but we've found bugs by having the
		 * threads pound on the same key/value pairs, that is, by making
		 * them traverse the same RNG space. 75% of the time we run in
		 * independent RNG space.
		 */
		if (g.c_independent_thread_rng)
			__wt_random_init_seed(
			    (WT_SESSION_IMPL *)session, &tinfo->rnd);
		else
			__wt_random_init(&tinfo->rnd);

		tinfo->state = TINFO_RUNNING;
		testutil_check(
		    __wt_thread_create(NULL, &tinfo->tid, ops, tinfo));
	}

	/*
	 * If a multi-threaded run, start optional backup, compaction and
	 * long-running reader threads.
	 */
	if (g.c_alter)
		testutil_check(
		    __wt_thread_create(NULL, &alter_tid, alter, NULL));
	if (g.c_backups)
		testutil_check(
		    __wt_thread_create(NULL, &backup_tid, backup, NULL));
	if (g.c_checkpoint_flag == CHECKPOINT_ON)
		testutil_check(__wt_thread_create(
		    NULL, &checkpoint_tid, checkpoint, NULL));
	if (g.c_compact)
		testutil_check(
		    __wt_thread_create(NULL, &compact_tid, compact, NULL));
	if (!SINGLETHREADED && g.c_long_running_txn)
		testutil_check(__wt_thread_create(NULL, &lrt_tid, lrt, NULL));
	if (g.c_txn_timestamps)
		testutil_check(__wt_thread_create(
		    NULL, &timestamp_tid, timestamp, tinfo_list));

	/* Spin on the threads, calculating the totals. */
	for (;;) {
		/* Clear out the totals each pass. */
		memset(&total, 0, sizeof(total));
		for (i = 0, running = false; i < g.c_threads; ++i) {
			tinfo = tinfo_list[i];
			total.commit += tinfo->commit;
			total.insert += tinfo->insert;
			total.prepare += tinfo->prepare;
			total.remove += tinfo->remove;
			total.rollback += tinfo->rollback;
			total.search += tinfo->search;
			total.truncate += tinfo->truncate;
			total.update += tinfo->update;

			switch (tinfo->state) {
			case TINFO_RUNNING:
				running = true;
				break;
			case TINFO_COMPLETE:
				tinfo->state = TINFO_JOINED;
				testutil_check(
				    __wt_thread_join(NULL, &tinfo->tid));
				break;
			case TINFO_JOINED:
				break;
			}

			/*
			 * If the timer has expired or this thread has completed
			 * its operations, notify the thread it should quit.
			 */
			if (fourths == 0 ||
			    (thread_ops != -1 &&
			    tinfo->ops >= (uint64_t)thread_ops)) {
				/*
				 * On the last execution, optionally drop core
				 * for recovery testing.
				 */
				if (lastrun && g.c_abort) {
					static char *core = NULL;
					*core = 0;
				}
				tinfo->quit = true;
			}
		}
		track("ops", 0ULL, &total);
		if (!running)
			break;
		__wt_sleep(0, 250000);		/* 1/4th of a second */
		if (fourths != -1)
			--fourths;
		if (quit_fourths != -1 && --quit_fourths == 0) {
			fprintf(stderr, "%s\n",
			    "format run more than 15 minutes past the maximum "
			    "time");
			fprintf(stderr, "%s\n",
			    "format run dumping cache and transaction state, "
			    "then aborting the process");

			/*
			 * If the library is deadlocked, we might just join the
			 * mess, set a timer to limit our exposure.
			 */
			set_alarm();

			(void)conn->debug_info(conn, "txn");
			(void)conn->debug_info(conn, "cache");

			__wt_abort(NULL);
		}
	}

	/* Wait for the other threads. */
	g.workers_finished = true;
	if (g.c_alter)
		testutil_check(__wt_thread_join(NULL, &alter_tid));
	if (g.c_backups)
		testutil_check(__wt_thread_join(NULL, &backup_tid));
	if (g.c_checkpoint_flag == CHECKPOINT_ON)
		testutil_check(__wt_thread_join(NULL, &checkpoint_tid));
	if (g.c_compact)
		testutil_check(__wt_thread_join(NULL, &compact_tid));
	if (!SINGLETHREADED && g.c_long_running_txn)
		testutil_check(__wt_thread_join(NULL, &lrt_tid));
	if (g.c_txn_timestamps)
		testutil_check(__wt_thread_join(NULL, &timestamp_tid));
	g.workers_finished = false;

	if (g.logging != 0) {
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== thread ops stop ===============");
		testutil_check(session->close(session, NULL));
	}

	for (i = 0; i < g.c_threads; ++i)
		free(tinfo_list[i]);
	free(tinfo_list);
}

typedef enum { NEXT, PREV, SEARCH, SEARCH_NEAR } read_operation;

/*
 * read_op --
 *	Perform a read operation, waiting out prepare conflicts.
 */
static inline int
read_op(WT_CURSOR *cursor, read_operation op, int *exactp)
{
	WT_DECL_RET;

	/*
	 * Read operations wait out prepare-conflicts. (As part of the snapshot
	 * isolation checks, we repeat reads that succeeded before, they should
	 * be repeatable.)
	 */
	switch (op) {
	case NEXT:
		while ((ret = cursor->next(cursor)) == WT_PREPARE_CONFLICT)
			__wt_yield();
		break;
	case PREV:
		while ((ret = cursor->prev(cursor)) == WT_PREPARE_CONFLICT)
			__wt_yield();
		break;
	case SEARCH:
		while ((ret = cursor->search(cursor)) == WT_PREPARE_CONFLICT)
			__wt_yield();
		break;
	case SEARCH_NEAR:
		while ((ret =
		    cursor->search_near(cursor, exactp)) == WT_PREPARE_CONFLICT)
			__wt_yield();
		break;
	}
	return (ret);
}

typedef enum { INSERT, MODIFY, READ, REMOVE, TRUNCATE, UPDATE } thread_op;
typedef struct {
	thread_op op;			/* Operation */
	uint64_t  keyno;		/* Row number */
	uint64_t  last;			/* Inclusive end of a truncate range */

	void    *kdata;			/* If an insert, the generated key */
	size_t   ksize;
	size_t   kmemsize;

	void    *vdata;			/* If not a delete, the value */
	size_t   vsize;
	size_t   vmemsize;
} SNAP_OPS;

#define	SNAP_TRACK(op, tinfo) do {					\
	if (snap != NULL &&						\
	    (size_t)(snap - snap_list) < WT_ELEMENTS(snap_list))	\
		snap_track(snap++, op, tinfo);				\
} while (0)

/*
 * snap_track --
 *     Add a single snapshot isolation returned value to the list.
 */
static void
snap_track(SNAP_OPS *snap, thread_op op, TINFO *tinfo)
{
	WT_ITEM *ip;

	snap->op = op;
	snap->keyno = tinfo->keyno;
	snap->last = op == TRUNCATE ? tinfo->last : 0;

	if (op == INSERT && g.type == ROW) {
		ip = tinfo->key;
		if (snap->kmemsize < ip->size) {
			snap->kdata = drealloc(snap->kdata, ip->size);
			snap->kmemsize = ip->size;
		}
		memcpy(snap->kdata, ip->data, snap->ksize = ip->size);
	}

	if (op != REMOVE && op != TRUNCATE)  {
		ip = tinfo->value;
		if (snap->vmemsize < ip->size) {
			snap->vdata = drealloc(snap->vdata, ip->size);
			snap->vmemsize = ip->size;
		}
		memcpy(snap->vdata, ip->data, snap->vsize = ip->size);
	}
}

/*
 * snap_check --
 *	Check snapshot isolation operations are repeatable.
 */
static int
snap_check(WT_CURSOR *cursor,
    SNAP_OPS *start, SNAP_OPS *stop, WT_ITEM *key, WT_ITEM *value)
{
	WT_DECL_RET;
	SNAP_OPS *p;
	uint8_t bitfield;

	for (; start < stop; ++start) {
		/*
		 * We don't test all of the records in a truncate range, only
		 * the first because that matches the rest of the isolation
		 * checks. If a truncate range was from the start of the table,
		 * switch to the record at the end.
		 */
		if (start->op == TRUNCATE && start->keyno == 0) {
			start->keyno = start->last;
			testutil_assert(start->keyno != 0);
		}

		/*
		 * Check for subsequent changes to this record. If we find a
		 * read, don't treat it was a subsequent change, that way we
		 * verify the results of the change as well as the results of
		 * the read.
		 */
		for (p = start + 1; p < stop; ++p) {
			if (p->op == READ)
				continue;
			if (p->keyno == start->keyno)
				break;

			if (p->op != TRUNCATE)
				continue;
			if (g.c_reverse &&
			    (p->keyno == 0 || p->keyno >= start->keyno) &&
			    (p->last == 0 || p->last <= start->keyno))
				break;
			if (!g.c_reverse &&
			    (p->keyno == 0 || p->keyno <= start->keyno) &&
			    (p->last == 0 || p->last >= start->keyno))
				break;
		}
		if (p != stop)
			continue;

		/*
		 * Retrieve the key/value pair by key. Row-store inserts have a
		 * unique generated key we saved, else generate the key from the
		 * key number.
		 */
		if (start->op == INSERT && g.type == ROW) {
			key->data = start->kdata;
			key->size = start->ksize;
			cursor->set_key(cursor, key);
		} else {
			switch (g.type) {
			case FIX:
			case VAR:
				cursor->set_key(cursor, start->keyno);
				break;
			case ROW:
				key_gen(key, start->keyno);
				cursor->set_key(cursor, key);
				break;
			}
		}

		switch (ret = read_op(cursor, SEARCH, NULL)) {
		case 0:
			if (g.type == FIX) {
				testutil_check(
				    cursor->get_value(cursor, &bitfield));
				*(uint8_t *)(value->data) = bitfield;
				value->size = 1;
			} else
				testutil_check(
				    cursor->get_value(cursor, value));
			break;
		case WT_NOTFOUND:
			break;
		default:
			return (ret);
		}

		/* Check for simple matches. */
		if (ret == 0 &&
		    start->op != REMOVE && start->op != TRUNCATE &&
		    value->size == start->vsize &&
		    memcmp(value->data, start->vdata, value->size) == 0)
			continue;
		if (ret == WT_NOTFOUND &&
		    (start->op == REMOVE || start->op == TRUNCATE))
			continue;

		/*
		 * In fixed length stores, zero values at the end of the key
		 * space are returned as not-found, and not-found row reads
		 * are saved as zero values. Map back-and-forth for simplicity.
		 */
		if (g.type == FIX) {
			if (ret == WT_NOTFOUND &&
			    start->vsize == 1 && *(uint8_t *)start->vdata == 0)
				continue;
			if ((start->op == REMOVE || start->op == TRUNCATE) &&
			    value->size == 1 && *(uint8_t *)value->data == 0)
				continue;
		}

		/* Things went pear-shaped. */
		switch (g.type) {
		case FIX:
			testutil_die(ret,
			    "snapshot-isolation: %" PRIu64 " search: "
			    "expected {0x%02x}, found {0x%02x}",
			    start->keyno,
			    start->op == REMOVE ? 0 : *(uint8_t *)start->vdata,
			    ret == WT_NOTFOUND ? 0 : *(uint8_t *)value->data);
			/* NOTREACHED */
		case ROW:
			fprintf(stderr,
			    "snapshot-isolation %.*s search mismatch\n",
			    (int)key->size, (const char *)key->data);

			if (start->op == REMOVE)
				fprintf(stderr, "expected {deleted}\n");
			else
				print_item_data(
				    "expected", start->vdata, start->vsize);
			if (ret == WT_NOTFOUND)
				fprintf(stderr, "found {deleted}\n");
			else
				print_item_data(
				    "found", value->data, value->size);

			testutil_die(ret,
			    "snapshot-isolation: %.*s search mismatch",
			    (int)key->size, key->data);
			/* NOTREACHED */
		case VAR:
			fprintf(stderr,
			    "snapshot-isolation %" PRIu64 " search mismatch\n",
			    start->keyno);

			if (start->op == REMOVE)
				fprintf(stderr, "expected {deleted}\n");
			else
				print_item_data(
				    "expected", start->vdata, start->vsize);
			if (ret == WT_NOTFOUND)
				fprintf(stderr, "found {deleted}\n");
			else
				print_item_data(
				    "found", value->data, value->size);

			testutil_die(ret,
			    "snapshot-isolation: %" PRIu64 " search mismatch",
			    start->keyno);
			/* NOTREACHED */
		}
	}
	return (0);
}

/*
 * begin_transaction --
 *	Choose an isolation configuration and begin a transaction.
 */
static void
begin_transaction(TINFO *tinfo, WT_SESSION *session, u_int *iso_configp)
{
	WT_DECL_RET;
	u_int v;
	char buf[64];
	const char *config;

	if ((v = g.c_isolation_flag) == ISOLATION_RANDOM)
		v = mmrand(&tinfo->rnd, 1, 3);
	switch (v) {
	case 1:
		v = ISOLATION_READ_UNCOMMITTED;
		config = "isolation=read-uncommitted";
		break;
	case 2:
		v = ISOLATION_READ_COMMITTED;
		config = "isolation=read-committed";
		break;
	case 3:
	default:
		v = ISOLATION_SNAPSHOT;
		config = "isolation=snapshot";
		break;
	}
	*iso_configp = v;

	/*
	 * Keep trying to start a new transaction if it's timing out - we know
	 * there aren't any resources pinned so it should succeed eventually.
	 */
	while ((ret =
	    session->begin_transaction(session, config)) == WT_CACHE_FULL)
		;
	testutil_check(ret);

	if (v == ISOLATION_SNAPSHOT && g.c_txn_timestamps) {
		/*
		 * Prepare returns an error if the prepare timestamp is less
		 * than any active read timestamp, single-thread transaction
		 * prepare and begin.
		 *
		 * Lock out the oldest timestamp update.
		 */
		testutil_check(pthread_rwlock_wrlock(&g.ts_lock));

		testutil_check(__wt_snprintf(buf, sizeof(buf),
		    "read_timestamp=%" PRIx64,
		    __wt_atomic_addv64(&g.timestamp, 1)));
		testutil_check(session->timestamp_transaction(session, buf));

		testutil_check(pthread_rwlock_unlock(&g.ts_lock));
	}
}

/*
 * commit_transaction --
 *     Commit a transaction.
 */
static void
commit_transaction(TINFO *tinfo, WT_SESSION *session)
{
	uint64_t ts;
	char buf[64];

	++tinfo->commit;

	if (g.c_txn_timestamps) {
		/* Lock out the oldest timestamp update. */
		testutil_check(pthread_rwlock_wrlock(&g.ts_lock));

		ts = __wt_atomic_addv64(&g.timestamp, 1);
		testutil_check(__wt_snprintf(
		    buf, sizeof(buf), "commit_timestamp=%" PRIx64, ts));
		testutil_check(session->timestamp_transaction(session, buf));

		if (tinfo->prepare_txn) {
			testutil_check(__wt_snprintf(buf, sizeof(buf),
			    "durable_timestamp=%" PRIx64, ts));
			testutil_check(
			    session->timestamp_transaction(session, buf));
		}

		testutil_check(pthread_rwlock_unlock(&g.ts_lock));
	}
	testutil_check(session->commit_transaction(session, NULL));

	tinfo->prepare_txn = false;
}

/*
 * rollback_transaction --
 *     Rollback a transaction.
 */
static void
rollback_transaction(TINFO *tinfo, WT_SESSION *session)
{
	++tinfo->rollback;

	testutil_check(session->rollback_transaction(session, NULL));

	tinfo->prepare_txn = false;
}

/*
 * prepare_transaction --
 *     Prepare a transaction if timestamps are in use.
 */
static int
prepare_transaction(TINFO *tinfo, WT_SESSION *session)
{
	WT_DECL_RET;
	uint64_t ts;
	char buf[64];

	++tinfo->prepare;

	/*
	 * Prepare timestamps must be less than or equal to the eventual commit
	 * timestamp. Set the prepare timestamp to whatever the global value is
	 * now. The subsequent commit will increment it, ensuring correctness.
	 *
	 * Prepare returns an error if the prepare timestamp is less than any
	 * active read timestamp, single-thread transaction prepare and begin.
	 *
	 * Lock out the oldest timestamp update.
	 */
	testutil_check(pthread_rwlock_wrlock(&g.ts_lock));

	ts = __wt_atomic_addv64(&g.timestamp, 1);
	testutil_check(__wt_snprintf(
	    buf, sizeof(buf), "prepare_timestamp=%" PRIx64, ts));
	ret = session->prepare_transaction(session, buf);

	testutil_check(pthread_rwlock_unlock(&g.ts_lock));

	tinfo->prepare_txn = true;
	return (ret);
}

/*
 * OP_FAILED --
 *	General error handling.
 */
#define	OP_FAILED(notfound_ok) do {					\
	positioned = false;						\
	if (intxn && (ret == WT_CACHE_FULL || ret == WT_ROLLBACK))	\
		goto rollback;						\
	testutil_assert((notfound_ok && ret == WT_NOTFOUND) ||		\
	    ret == WT_CACHE_FULL || ret == WT_ROLLBACK);		\
} while (0)

/*
 * Rollback updates returning prepare-conflict, they're unlikely to succeed
 * unless the prepare aborts. Reads wait out the error, so it's unexpected.
 */
#define	READ_OP_FAILED(notfound_ok)					\
	OP_FAILED(notfound_ok)
#define	WRITE_OP_FAILED(notfound_ok) do {				\
	if (ret == WT_PREPARE_CONFLICT)					\
		ret = WT_ROLLBACK;					\
	OP_FAILED(notfound_ok);						\
} while (0)

/*
 * ops --
 *     Per-thread operations.
 */
static WT_THREAD_RET
ops(void *arg)
{
	SNAP_OPS *snap, snap_list[128];
	TINFO *tinfo;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION *session;
	thread_op op;
	uint64_t reset_op, session_op, truncate_op;
	uint32_t range, rnd;
	u_int i, j, iso_config;
	bool greater_than, intxn, next, positioned, readonly;

	tinfo = arg;

	conn = g.wts_conn;
	readonly = false;		/* -Wconditional-uninitialized */

	/* Initialize tracking of snapshot isolation transaction returns. */
	snap = NULL;
	iso_config = 0;
	memset(snap_list, 0, sizeof(snap_list));

	/* Set up the default key and value buffers. */
	tinfo->key = &tinfo->_key;
	key_gen_init(tinfo->key);
	tinfo->value = &tinfo->_value;
	val_gen_init(tinfo->value);
	tinfo->lastkey = &tinfo->_lastkey;
	key_gen_init(tinfo->lastkey);
	tinfo->tbuf = &tinfo->_tbuf;

	/* Set the first operation where we'll create sessions and cursors. */
	cursor = NULL;
	session = NULL;
	session_op = 0;

	/* Set the first operation where we'll reset the session. */
	reset_op = mmrand(&tinfo->rnd, 100, 10000);
	/* Set the first operation where we'll truncate a range. */
	truncate_op = g.c_truncate == 0 ?
	    UINT64_MAX : mmrand(&tinfo->rnd, 100, 10000);

	for (intxn = false; !tinfo->quit; ++tinfo->ops) {
		/* Periodically open up a new session and cursors. */
		if (tinfo->ops > session_op ||
		    session == NULL || cursor == NULL) {
			/*
			 * We can't swap sessions/cursors if in a transaction,
			 * resolve any running transaction.
			 */
			if (intxn) {
				commit_transaction(tinfo, session);
				intxn = false;
			}

			if (session != NULL)
				testutil_check(session->close(session, NULL));
			testutil_check(
			    conn->open_session(conn, NULL, NULL, &session));

			/* Pick the next session/cursor close/open. */
			session_op += mmrand(&tinfo->rnd, 100, 5000);

			/*
			 * 10% of the time, perform some read-only operations
			 * from a checkpoint.
			 *
			 * Skip if single-threaded and doing checks against a
			 * Berkeley DB database, that won't work because the
			 * Berkeley DB database won't match the checkpoint.
			 *
			 * Skip if we are using data-sources or LSM, they don't
			 * support reading from checkpoints.
			 */
			if (!SINGLETHREADED &&
			    !DATASOURCE("kvsbdb") && !DATASOURCE("lsm") &&
			    mmrand(&tinfo->rnd, 1, 10) == 1) {
				/*
				 * open_cursor can return EBUSY if concurrent
				 * with a metadata operation, retry.
				 */
				while ((ret = session->open_cursor(session,
				    g.uri, NULL,
				    "checkpoint=WiredTigerCheckpoint",
				    &cursor)) == EBUSY)
					__wt_yield();
				/*
				 * If the checkpoint hasn't been created yet,
				 * ignore the error.
				 */
				if (ret == ENOENT)
					continue;
				testutil_check(ret);

				/* Checkpoints are read-only. */
				readonly = true;
			} else {
				/*
				 * Configure "append", in the case of column
				 * stores, we append when inserting new rows.
				 * open_cursor can return EBUSY if concurrent
				 * with a metadata operation, retry.
				 */
				while ((ret = session->open_cursor(session,
				    g.uri, NULL, "append", &cursor)) == EBUSY)
					__wt_yield();
				testutil_check(ret);

				/* Updates supported. */
				readonly = false;
			}
		}

		/*
		 * Reset the session every now and then, just to make sure that
		 * operation gets tested. Note the test is not for equality, we
		 * have to do the reset outside of a transaction.
		 */
		if (tinfo->ops > reset_op && !intxn) {
			testutil_check(session->reset(session));

			/* Pick the next reset operation. */
			reset_op += mmrand(&tinfo->rnd, 20000, 50000);
		}

		/*
		 * If we're not single-threaded and not in a transaction, choose
		 * an isolation level and start a transaction some percentage of
		 * the time.
		 */
		if (!SINGLETHREADED &&
		    !intxn && mmrand(&tinfo->rnd, 1, 100) <= g.c_txn_freq) {
			begin_transaction(tinfo, session, &iso_config);
			snap =
			    iso_config == ISOLATION_SNAPSHOT ? snap_list : NULL;
			intxn = true;
		}

		/* Select a row. */
		tinfo->keyno = mmrand(&tinfo->rnd, 1, (u_int)g.rows);

		/* Select an operation. */
		op = READ;
		if (!readonly) {
			i = mmrand(&tinfo->rnd, 1, 100);
			if (i < g.c_delete_pct && tinfo->ops > truncate_op) {
				op = TRUNCATE;

				/* Pick the next truncate operation. */
				truncate_op +=
				    mmrand(&tinfo->rnd, 20000, 100000);
			} else if (i < g.c_delete_pct)
				op = REMOVE;
			else if (i < g.c_delete_pct + g.c_insert_pct)
				op = INSERT;
			else if (i < g.c_delete_pct +
			    g.c_insert_pct + g.c_modify_pct)
				op = MODIFY;
			else if (i < g.c_delete_pct +
			    g.c_insert_pct + g.c_modify_pct + g.c_write_pct)
				op = UPDATE;
		}

		/*
		 * Inserts, removes and updates can be done following a cursor
		 * set-key, or based on a cursor position taken from a previous
		 * search. If not already doing a read, position the cursor at
		 * an existing point in the tree 20% of the time.
		 */
		positioned = false;
		if (op != READ && mmrand(&tinfo->rnd, 1, 5) == 1) {
			++tinfo->search;
			ret = read_row(tinfo, cursor);
			if (ret == 0) {
				positioned = true;
				SNAP_TRACK(READ, tinfo);
			} else
				READ_OP_FAILED(true);
		}

		/* Optionally reserve a row. */
		if (!readonly && intxn && mmrand(&tinfo->rnd, 0, 20) == 1) {
			switch (g.type) {
			case ROW:
				ret = row_reserve(tinfo, cursor, positioned);
				break;
			case FIX:
			case VAR:
				ret = col_reserve(tinfo, cursor, positioned);
				break;
			}
			if (ret == 0) {
				positioned = true;

				__wt_yield();	/* Let other threads proceed. */
			} else
				WRITE_OP_FAILED(true);
		}

		/* Perform the operation. */
		switch (op) {
		case INSERT:
			switch (g.type) {
			case ROW:
				ret = row_insert(tinfo, cursor, positioned);
				break;
			case FIX:
			case VAR:
				/*
				 * We can only append so many new records, once
				 * we reach that limit, update a record instead
				 * of inserting.
				 */
				if (g.append_cnt >= g.append_max)
					goto update_instead_of_chosen_op;

				ret = col_insert(tinfo, cursor);
				break;
			}

			/* Insert never leaves the cursor positioned. */
			positioned = false;
			if (ret == 0) {
				++tinfo->insert;
				SNAP_TRACK(INSERT, tinfo);
			} else
				WRITE_OP_FAILED(false);
			break;
		case MODIFY:
			/*
			 * Change modify into update if not in a transaction
			 * or in a read-uncommitted transaction, modify isn't
			 * supported in those cases.
			 */
			if (!intxn || iso_config == ISOLATION_READ_UNCOMMITTED)
				goto update_instead_of_chosen_op;

			++tinfo->update;
			switch (g.type) {
			case ROW:
				ret = row_modify(tinfo, cursor, positioned);
				break;
			case VAR:
				ret = col_modify(tinfo, cursor, positioned);
				break;
			}
			if (ret == 0) {
				positioned = true;
				SNAP_TRACK(MODIFY, tinfo);
			} else
				WRITE_OP_FAILED(true);
			break;
		case READ:
			++tinfo->search;
			ret = read_row(tinfo, cursor);
			if (ret == 0) {
				positioned = true;
				SNAP_TRACK(READ, tinfo);
			} else
				READ_OP_FAILED(true);
			break;
		case REMOVE:
remove_instead_of_truncate:
			switch (g.type) {
			case ROW:
				ret = row_remove(tinfo, cursor, positioned);
				break;
			case FIX:
			case VAR:
				ret = col_remove(tinfo, cursor, positioned);
				break;
			}
			if (ret == 0) {
				++tinfo->remove;
				/*
				 * Don't set positioned: it's unchanged from the
				 * previous state, but not necessarily set.
				 */
				SNAP_TRACK(REMOVE, tinfo);
			} else
				WRITE_OP_FAILED(true);
			break;
		case TRUNCATE:
			/*
			 * A maximum of 2 truncation operations at a time, more
			 * than that can lead to serious thrashing.
			 */
			if (__wt_atomic_addv64(&g.truncate_cnt, 1) > 2) {
				(void)__wt_atomic_subv64(&g.truncate_cnt, 1);
				goto remove_instead_of_truncate;
			}

			if (!positioned)
				tinfo->keyno =
				    mmrand(&tinfo->rnd, 1, (u_int)g.rows);

			/*
			 * Truncate up to 5% of the table. If the range overlaps
			 * the beginning/end of the table, set the key to 0 (the
			 * truncate function then sets a cursor to NULL so that
			 * code is tested).
			 *
			 * This gets tricky: there are 2 directions (truncating
			 * from lower keys to the current position or from
			 * the current position to higher keys), and collation
			 * order (truncating from lower keys to higher keys or
			 * vice-versa).
			 */
			greater_than = mmrand(&tinfo->rnd, 0, 1) == 1;
			range = g.rows < 20 ?
			    1 : mmrand(&tinfo->rnd, 1, (u_int)g.rows / 20);
			tinfo->last = tinfo->keyno;
			if (greater_than) {
				if (g.c_reverse) {
					if (tinfo->keyno <= range)
						tinfo->last = 0;
					else
						tinfo->last -= range;
				} else {
					tinfo->last += range;
					if (tinfo->last > g.rows)
						tinfo->last = 0;
				}
			} else {
				if (g.c_reverse) {
					tinfo->keyno += range;
					if (tinfo->keyno > g.rows)
						tinfo->keyno = 0;
				} else {
					if (tinfo->keyno <= range)
						tinfo->keyno = 0;
					else
						tinfo->keyno -= range;
				}
			}
			switch (g.type) {
			case ROW:
				ret = row_truncate(tinfo, cursor);
				break;
			case FIX:
			case VAR:
				ret = col_truncate(tinfo, cursor);
				break;
			}
			(void)__wt_atomic_subv64(&g.truncate_cnt, 1);

			/* Truncate never leaves the cursor positioned. */
			positioned = false;
			if (ret == 0) {
				++tinfo->truncate;
				SNAP_TRACK(TRUNCATE, tinfo);
			} else
				WRITE_OP_FAILED(false);
			break;
		case UPDATE:
update_instead_of_chosen_op:
			++tinfo->update;
			switch (g.type) {
			case ROW:
				ret = row_update(tinfo, cursor, positioned);
				break;
			case FIX:
			case VAR:
				ret = col_update(tinfo, cursor, positioned);
				break;
			}
			if (ret == 0) {
				positioned = true;
				SNAP_TRACK(UPDATE, tinfo);
			} else
				WRITE_OP_FAILED(false);
			break;
		}

		/*
		 * The cursor is positioned if we did any operation other than
		 * insert, do a small number of next/prev cursor operations in
		 * a random direction.
		 */
		if (positioned) {
			next = mmrand(&tinfo->rnd, 0, 1) == 1;
			j = mmrand(&tinfo->rnd, 1, 100);
			for (i = 0; i < j; ++i) {
				if ((ret = nextprev(tinfo, cursor, next)) == 0)
					continue;

				READ_OP_FAILED(true);
				break;
			}
		}

		/* Reset the cursor: there is no reason to keep pages pinned. */
		testutil_check(cursor->reset(cursor));

		/*
		 * Continue if not in a transaction, else add more operations
		 * to the transaction half the time.
		 */
		if (!intxn || (rnd = mmrand(&tinfo->rnd, 1, 10)) > 5)
			continue;

		/*
		 * Ending the transaction. If in snapshot isolation, repeat the
		 * operations and confirm they're unchanged.
		 */
		if (snap != NULL) {
			ret = snap_check(
			    cursor, snap_list, snap, tinfo->key, tinfo->value);
			testutil_assert(ret == 0 || ret == WT_ROLLBACK);
			if (ret == WT_ROLLBACK)
				goto rollback;
		}

		/*
		 * If prepare configured, prepare the transaction 10% of the
		 * time.
		 */
		if (g.c_prepare && mmrand(&tinfo->rnd, 1, 10) == 1) {
			ret = prepare_transaction(tinfo, session);
			if (ret != 0)
				WRITE_OP_FAILED(false);

			__wt_yield();		/* Let other threads proceed. */
		}

		/*
		 * If we're in a transaction, commit 40% of the time and
		 * rollback 10% of the time.
		 */
		switch (rnd) {
		case 1: case 2: case 3: case 4:			/* 40% */
			commit_transaction(tinfo, session);
			break;
		case 5:						/* 10% */
rollback:		rollback_transaction(tinfo, session);
			break;
		}

		intxn = false;
		snap = NULL;
	}

	if (session != NULL)
		testutil_check(session->close(session, NULL));

	for (i = 0; i < WT_ELEMENTS(snap_list); ++i) {
		free(snap_list[i].kdata);
		free(snap_list[i].vdata);
	}
	key_gen_teardown(tinfo->key);
	val_gen_teardown(tinfo->value);
	key_gen_teardown(tinfo->lastkey);
	free(tinfo->tbuf->mem);

	tinfo->state = TINFO_COMPLETE;
	return (WT_THREAD_RET_VALUE);
}

/*
 * wts_read_scan --
 *	Read and verify a subset of the elements in a file.
 */
void
wts_read_scan(void)
{
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_ITEM key, value;
	WT_SESSION *session;
	uint64_t keyno, last_keyno;

	conn = g.wts_conn;

	/* Set up the default key/value buffers. */
	key_gen_init(&key);
	val_gen_init(&value);

	/* Open a session and cursor pair. */
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	/*
	 * open_cursor can return EBUSY if concurrent with a metadata
	 * operation, retry in that case.
	 */
	while ((ret = session->open_cursor(
	    session, g.uri, NULL, NULL, &cursor)) == EBUSY)
		__wt_yield();
	testutil_check(ret);

	/* Check a random subset of the records using the key. */
	for (last_keyno = keyno = 0; keyno < g.key_cnt;) {
		keyno += mmrand(NULL, 1, 17);
		if (keyno > g.rows)
			keyno = g.rows;
		if (keyno - last_keyno > 1000) {
			track("read row scan", keyno, NULL);
			last_keyno = keyno;
		}

		switch (ret = read_row_worker(
		    cursor, keyno, &key, &value, false)) {
		case 0:
		case WT_NOTFOUND:
		case WT_ROLLBACK:
		case WT_PREPARE_CONFLICT:
			break;
		default:
			testutil_die(
			    ret, "wts_read_scan: read row %" PRIu64, keyno);
		}
	}

	testutil_check(session->close(session, NULL));

	key_gen_teardown(&key);
	val_gen_teardown(&value);
}

/*
 * read_row_worker --
 *	Read and verify a single element in a row- or column-store file.
 */
int
read_row_worker(
    WT_CURSOR *cursor, uint64_t keyno, WT_ITEM *key, WT_ITEM *value, bool sn)
{
	WT_SESSION *session;
	uint8_t bitfield;
	int exact, ret;

	session = cursor->session;

	/* Retrieve the key/value pair by key. */
	switch (g.type) {
	case FIX:
	case VAR:
		cursor->set_key(cursor, keyno);
		break;
	case ROW:
		key_gen(key, keyno);
		cursor->set_key(cursor, key);
		break;
	}

	if (sn) {
		ret = read_op(cursor, SEARCH_NEAR, &exact);
		if (ret == 0 && exact != 0)
			ret = WT_NOTFOUND;
	} else
		ret = read_op(cursor, SEARCH, NULL);
	switch (ret) {
	case 0:
		if (g.type == FIX) {
			testutil_check(cursor->get_value(cursor, &bitfield));
			*(uint8_t *)(value->data) = bitfield;
			value->size = 1;
		} else
			testutil_check(cursor->get_value(cursor, value));
		break;
	case WT_NOTFOUND:
		/*
		 * In fixed length stores, zero values at the end of the key
		 * space are returned as not-found. Treat this the same as
		 * a zero value in the key space, to match BDB's behavior.
		 * The WiredTiger cursor has lost its position though, so
		 * we return not-found, the cursor movement can't continue.
		 */
		if (g.type == FIX) {
			*(uint8_t *)(value->data) = 0;
			value->size = 1;
		}
		break;
	default:
		return (ret);
	}

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api,
		    session, "%-10s%" PRIu64, "read", keyno);

#ifdef HAVE_BERKELEY_DB
	if (!SINGLETHREADED)
		return (ret);

	/* Retrieve the BDB value. */
	{
	WT_ITEM bdb_value;
	int notfound;

	bdb_read(keyno, &bdb_value.data, &bdb_value.size, &notfound);

	/* Check for not-found status. */
	if (notfound_chk("read_row", ret, notfound, keyno))
		return (ret);

	/* Compare the two. */
	if (value->size != bdb_value.size ||
	    memcmp(value->data, bdb_value.data, value->size) != 0) {
		fprintf(stderr,
		    "read_row: value mismatch %" PRIu64 ":\n", keyno);
		print_item("bdb", &bdb_value);
		print_item(" wt", value);
		testutil_die(0, NULL);
	}
	}
#endif
	return (ret);
}

/*
 * read_row --
 *	Read and verify a single element in a row- or column-store file.
 */
static int
read_row(TINFO *tinfo, WT_CURSOR *cursor)
{
	/* 25% of the time we call search-near. */
	return (read_row_worker(cursor, tinfo->keyno,
	    tinfo->key, tinfo->value, mmrand(&tinfo->rnd, 0, 3) == 1));
}

/*
 * nextprev --
 *	Read and verify the next/prev element in a row- or column-store file.
 */
static int
nextprev(TINFO *tinfo, WT_CURSOR *cursor, bool next)
{
	WT_DECL_RET;
	WT_ITEM key, value;
	uint64_t keyno, keyno_prev;
	uint8_t bitfield;
	int cmp;
	const char *which;
	bool incrementing, record_gaps;

	keyno = 0;
	which = next ? "WT_CURSOR.next" : "WT_CURSOR.prev";

	switch (ret = read_op(cursor, next ? NEXT : PREV, NULL)) {
	case 0:
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
		if (ret != 0)
			testutil_die(ret, "nextprev: get_key/get_value");

		/* Check that keys are never returned out-of-order. */
		/*
		 * XXX
		 * WT-3889
		 * LSM has a bug that prevents cursor order checks from
		 * working, skip the test for now.
		 */
		if (DATASOURCE("lsm"))
			break;

		/*
		 * Compare the returned key with the previously returned key,
		 * and assert the order is correct. If not deleting keys, and
		 * the rows aren't in the column-store insert name space, also
		 * assert we don't skip groups of records (that's a page-split
		 * bug symptom).
		 */
		record_gaps = g.c_delete_pct != 0;
		switch (g.type) {
		case FIX:
		case VAR:
			if (tinfo->keyno > g.c_rows || keyno > g.c_rows)
				record_gaps = true;
			if (!next) {
				if (tinfo->keyno < keyno ||
				    (!record_gaps && keyno != tinfo->keyno - 1))
					goto order_error_col;
			} else
				if (tinfo->keyno > keyno ||
				    (!record_gaps && keyno != tinfo->keyno + 1))
					goto order_error_col;
			if (0) {
order_error_col:
				testutil_die(0,
				    "%s returned %" PRIu64 " then %" PRIu64,
				    which, tinfo->keyno, keyno);
			}

			tinfo->keyno = keyno;
			break;
		case ROW:
			incrementing =
			    (next && !g.c_reverse) || (!next && g.c_reverse);
			cmp = memcmp(tinfo->key->data, key.data,
			    WT_MIN(tinfo->key->size, key.size));
			if (incrementing) {
				if (cmp > 0 ||
				    (cmp == 0 && tinfo->key->size < key.size))
					goto order_error_row;
			} else
				if (cmp < 0 ||
				    (cmp == 0 && tinfo->key->size > key.size))
					goto order_error_row;
			if (!record_gaps) {
				/*
				 * Convert the keys to record numbers and then
				 * compare less-than-or-equal. (Not less-than,
				 * row-store inserts new rows in-between rows
				 * by append a new suffix to the row's key.)
				 */
				testutil_check(__wt_buf_fmt(
				    (WT_SESSION_IMPL *)cursor->session,
				    tinfo->tbuf, "%.*s",
				    (int)tinfo->key->size,
				    (char *)tinfo->key->data));
				keyno_prev =
				    strtoul(tinfo->tbuf->data, NULL, 10);
				testutil_check(__wt_buf_fmt(
				    (WT_SESSION_IMPL *)cursor->session,
				    tinfo->tbuf, "%.*s",
				    (int)key.size, (char *)key.data));
				keyno = strtoul(tinfo->tbuf->data, NULL, 10);
				if (incrementing) {
					if (keyno_prev != keyno &&
					    keyno_prev + 1 != keyno)
						goto order_error_row;
				} else
					if (keyno_prev != keyno &&
					    keyno_prev - 1 != keyno)
						goto order_error_row;
			}
			if (0) {
order_error_row:
				testutil_die(0,
				    "%s returned {%.*s} then {%.*s}",
				    which,
				    (int)tinfo->key->size, tinfo->key->data,
				    (int)key.size, key.data);
			}

			testutil_check(__wt_buf_set((WT_SESSION_IMPL *)
			    cursor->session, tinfo->key, key.data, key.size));
			break;
		}
		break;
	case WT_NOTFOUND:
		break;
	default:
		return (ret);
	}

	if (ret == 0 && g.logging == LOG_OPS)
		switch (g.type) {
		case FIX:
			(void)g.wt_api->msg_printf(g.wt_api,
			    cursor->session, "%-10s%" PRIu64 " {0x%02x}",
			    which, keyno, ((char *)value.data)[0]);
			break;
		case ROW:
			(void)g.wt_api->msg_printf(g.wt_api,
			    cursor->session, "%-10s{%.*s}, {%.*s}",
			    which, (int)key.size, (char *)key.data,
			    (int)value.size, (char *)value.data);
			break;
		case VAR:
			(void)g.wt_api->msg_printf(g.wt_api,
			    cursor->session, "%-10s%" PRIu64 " {%.*s}",
			    which, keyno, (int)value.size, (char *)value.data);
			break;
		}

#ifdef HAVE_BERKELEY_DB
	if (!SINGLETHREADED)
		return (ret);

	{
	WT_ITEM bdb_key, bdb_value;
	int notfound;
	char *p;

	/* Retrieve the BDB key/value. */
	bdb_np(next, &bdb_key.data, &bdb_key.size,
	    &bdb_value.data, &bdb_value.size, &notfound);
	if (notfound_chk(
	    next ? "nextprev(next)" : "nextprev(prev)", ret, notfound, keyno))
		return (ret);

	/* Compare the two. */
	if ((g.type == ROW &&
	    (key.size != bdb_key.size ||
	    memcmp(key.data, bdb_key.data, key.size) != 0)) ||
	    (g.type != ROW && keyno != (uint64_t)atoll(bdb_key.data))) {
		fprintf(stderr, "nextprev: %s KEY mismatch:\n", which);
		goto mismatch;
	}
	if (value.size != bdb_value.size ||
	    memcmp(value.data, bdb_value.data, value.size) != 0) {
		fprintf(stderr, "nextprev: %s VALUE mismatch:\n", which);
mismatch:	if (g.type == ROW) {
			print_item("bdb-key", &bdb_key);
			print_item(" wt-key", &key);
		} else {
			if ((p = (char *)strchr(bdb_key.data, '.')) != NULL)
				*p = '\0';
			fprintf(stderr,
			    "\t" "bdb-key %.*s != wt-key %" PRIu64 "\n",
			    (int)bdb_key.size, (char *)bdb_key.data, keyno);
		}
		print_item("bdb-value", &bdb_value);
		print_item(" wt-value", &value);
		testutil_die(0, NULL);
	}
	}
#endif
	return (ret);
}

/*
 * row_reserve --
 *	Reserve a row in a row-store file.
 */
static int
row_reserve(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned) {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
	}

	if ((ret = cursor->reserve(cursor)) != 0)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s{%.*s}", "reserve",
		    (int)tinfo->key->size, tinfo->key->data);

	return (0);
}

/*
 * col_reserve --
 *	Reserve a row in a column-store file.
 */
static int
col_reserve(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned)
		cursor->set_key(cursor, tinfo->keyno);

	if ((ret = cursor->reserve(cursor)) != 0)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s%" PRIu64, "reserve", tinfo->keyno);

	return (0);
}

/*
 * modify_build --
 *	Generate a set of modify vectors.
 */
static void
modify_build(TINFO *tinfo, WT_MODIFY *entries, int *nentriesp)
{
	int i, nentries;

	/* Randomly select a number of byte changes, offsets and lengths. */
	nentries = (int)mmrand(&tinfo->rnd, 1, MAX_MODIFY_ENTRIES);
	for (i = 0; i < nentries; ++i) {
		entries[i].data.data = modify_repl +
		    mmrand(&tinfo->rnd, 1, sizeof(modify_repl) - 10);
		entries[i].data.size = (size_t)mmrand(&tinfo->rnd, 0, 10);
		/*
		 * Start at least 11 bytes into the buffer so we skip leading
		 * key information.
		 */
		entries[i].offset = (size_t)mmrand(&tinfo->rnd, 20, 40);
		entries[i].size = (size_t)mmrand(&tinfo->rnd, 0, 10);
	}

	*nentriesp = (int)nentries;
}

/*
 * row_modify --
 *	Modify a row in a row-store file.
 */
static int
row_modify(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;
	WT_MODIFY entries[MAX_MODIFY_ENTRIES];
	int nentries;

	if (!positioned) {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
	}

	modify_build(tinfo, entries, &nentries);
	if ((ret = cursor->modify(cursor, entries, nentries)) != 0)
		return (ret);

	testutil_check(cursor->get_value(cursor, tinfo->value));

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s{%.*s}, {%.*s}",
		    "modify",
		    (int)tinfo->key->size, tinfo->key->data,
		    (int)tinfo->value->size, tinfo->value->data);

#ifdef HAVE_BERKELEY_DB
	if (!SINGLETHREADED)
		return (0);

	bdb_update(
	    tinfo->key->data, tinfo->key->size,
	    tinfo->value->data, tinfo->value->size);
#endif
	return (0);
}

/*
 * col_modify --
 *	Modify a row in a column-store file.
 */
static int
col_modify(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;
	WT_MODIFY entries[MAX_MODIFY_ENTRIES];
	int nentries;

	if (!positioned)
		cursor->set_key(cursor, tinfo->keyno);

	modify_build(tinfo, entries, &nentries);
	if ((ret = cursor->modify(cursor, entries, nentries)) != 0)
		return (ret);

	testutil_check(cursor->get_value(cursor, tinfo->value));

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s%" PRIu64 ", {%.*s}",
		    "modify",
		    tinfo->keyno,
		    (int)tinfo->value->size, tinfo->value->data);

#ifdef HAVE_BERKELEY_DB
	if (!SINGLETHREADED)
		return (0);

	key_gen(tinfo->key, tinfo->keyno);
	bdb_update(
	    tinfo->key->data, tinfo->key->size,
	    tinfo->value->data, tinfo->value->size);
#endif
	return (0);
}

/*
 * row_truncate --
 *	Truncate rows in a row-store file.
 */
static int
row_truncate(TINFO *tinfo, WT_CURSOR *cursor)
{
	WT_CURSOR *c2;
	WT_DECL_RET;
	WT_SESSION *session;

	session = cursor->session;

	/*
	 * The code assumes we're never truncating the entire object, assert
	 * that fact.
	 */
	testutil_assert(tinfo->keyno != 0 || tinfo->last != 0);

	c2 = NULL;
	if (tinfo->keyno == 0) {
		key_gen(tinfo->key, tinfo->last);
		cursor->set_key(cursor, tinfo->key);
		ret = session->truncate(session, NULL, NULL, cursor, NULL);
	} else if (tinfo->last == 0) {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
		ret = session->truncate(session, NULL, cursor, NULL, NULL);
	} else {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);

		testutil_check(
		    session->open_cursor(session, g.uri, NULL, NULL, &c2));
		key_gen(tinfo->lastkey, tinfo->last);
		cursor->set_key(c2, tinfo->lastkey);

		ret = session->truncate(session, NULL, cursor, c2, NULL);
		testutil_check(c2->close(c2));
	}

	if (ret != 0)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "%-10s%" PRIu64 ", %" PRIu64,
		    "truncate",
		    tinfo->keyno, tinfo->last);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED)
		bdb_truncate(tinfo->keyno, tinfo->last);
#endif
	return (0);
}

/*
 * col_truncate --
 *	Truncate rows in a column-store file.
 */
static int
col_truncate(TINFO *tinfo, WT_CURSOR *cursor)
{
	WT_CURSOR *c2;
	WT_DECL_RET;
	WT_SESSION *session;

	session = cursor->session;

	/*
	 * The code assumes we're never truncating the entire object, assert
	 * that fact.
	 */
	testutil_assert(tinfo->keyno != 0 || tinfo->last != 0);

	c2 = NULL;
	if (tinfo->keyno == 0) {
		cursor->set_key(cursor, tinfo->last);
		ret = session->truncate(session, NULL, NULL, cursor, NULL);
	} else if (tinfo->last == 0) {
		cursor->set_key(cursor, tinfo->keyno);
		ret = session->truncate(session, NULL, cursor, NULL, NULL);
	} else {
		cursor->set_key(cursor, tinfo->keyno);

		testutil_check(
		    session->open_cursor(session, g.uri, NULL, NULL, &c2));
		cursor->set_key(c2, tinfo->last);

		ret = session->truncate(session, NULL, cursor, c2, NULL);
		testutil_check(c2->close(c2));
	}
	if (ret != 0)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "%-10s%" PRIu64 "-%" PRIu64,
		    "truncate",
		    tinfo->keyno, tinfo->last);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED)
		bdb_truncate(tinfo->keyno, tinfo->last);
#endif
	return (0);
}

/*
 * row_update --
 *	Update a row in a row-store file.
 */
static int
row_update(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned) {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
	}
	val_gen(&tinfo->rnd, tinfo->value, tinfo->keyno);
	cursor->set_value(cursor, tinfo->value);

	if ((ret = cursor->update(cursor)) != 0)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s{%.*s}, {%.*s}",
		    "put",
		    (int)tinfo->key->size, tinfo->key->data,
		    (int)tinfo->value->size, tinfo->value->data);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED)
		bdb_update(
		    tinfo->key->data, tinfo->key->size,
		    tinfo->value->data, tinfo->value->size);
#endif
	return (0);
}

/*
 * col_update --
 *	Update a row in a column-store file.
 */
static int
col_update(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned)
		cursor->set_key(cursor, tinfo->keyno);
	val_gen(&tinfo->rnd, tinfo->value, tinfo->keyno);
	if (g.type == FIX)
		cursor->set_value(cursor, *(uint8_t *)tinfo->value->data);
	else
		cursor->set_value(cursor, tinfo->value);

	if ((ret = cursor->update(cursor)) != 0)
		return (ret);

	if (g.logging == LOG_OPS) {
		if (g.type == FIX)
			(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
			    "%-10s%" PRIu64 " {0x%02" PRIx8 "}",
			    "update", tinfo->keyno,
			    ((uint8_t *)tinfo->value->data)[0]);
		else
			(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
			    "%-10s%" PRIu64 " {%.*s}",
			    "update", tinfo->keyno,
			    (int)tinfo->value->size,
			    (char *)tinfo->value->data);
	}

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED) {
		key_gen(tinfo->key, tinfo->keyno);
		bdb_update(
		    tinfo->key->data, tinfo->key->size,
		    tinfo->value->data, tinfo->value->size);
	}
#endif
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

	free(g.append);
	g.append = dcalloc(g.append_max, sizeof(uint64_t));
}

/*
 * table_append --
 *	Resolve the appended records.
 */
static void
table_append(uint64_t keyno)
{
	uint64_t *ep, *p;
	int done;

	ep = g.append + g.append_max;

	/*
	 * We don't want to ignore records we append, which requires we update
	 * the "last row" as we insert new records. Threads allocating record
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
	 * out of space in the table. It's unfortunately not even unlikely in
	 * the case of a large number of threads all inserting as fast as they
	 * can and a single thread going to sleep for an unexpectedly long time.
	 * If it happens, sleep and retry until earlier records are resolved
	 * and we find a slot.
	 */
	for (done = 0;;) {
		testutil_check(pthread_rwlock_wrlock(&g.append_lock));

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

		testutil_check(pthread_rwlock_unlock(&g.append_lock));

		if (done)
			break;
		__wt_sleep(1, 0);
	}
}

/*
 * row_insert --
 *	Insert a row in a row-store file.
 */
static int
row_insert(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	/*
	 * If we positioned the cursor already, it's a test of an update using
	 * the insert method. Otherwise, generate a unique key and insert.
	 */
	if (!positioned) {
		key_gen_insert(&tinfo->rnd, tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
	}
	val_gen(&tinfo->rnd, tinfo->value, tinfo->keyno);
	cursor->set_value(cursor, tinfo->value);

	if ((ret = cursor->insert(cursor)) != 0)
		return (ret);

	/* Log the operation */
	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
		    "%-10s{%.*s}, {%.*s}",
		    "insert",
		    (int)tinfo->key->size, tinfo->key->data,
		    (int)tinfo->value->size, tinfo->value->data);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED)
		bdb_update(
		    tinfo->key->data, tinfo->key->size,
		    tinfo->value->data, tinfo->value->size);
#endif
	return (0);
}

/*
 * col_insert --
 *	Insert an element in a column-store file.
 */
static int
col_insert(TINFO *tinfo, WT_CURSOR *cursor)
{
	WT_DECL_RET;

	val_gen(&tinfo->rnd, tinfo->value, g.rows + 1);
	if (g.type == FIX)
		cursor->set_value(cursor, *(uint8_t *)tinfo->value->data);
	else
		cursor->set_value(cursor, tinfo->value);

	if ((ret = cursor->insert(cursor)) != 0)
		return (ret);

	testutil_check(cursor->get_key(cursor, &tinfo->keyno));

	table_append(tinfo->keyno);			/* Extend the object. */

	if (g.logging == LOG_OPS) {
		if (g.type == FIX)
			(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
			    "%-10s%" PRIu64 " {0x%02" PRIx8 "}",
			    "insert", tinfo->keyno,
			    ((uint8_t *)tinfo->value->data)[0]);
		else
			(void)g.wt_api->msg_printf(g.wt_api, cursor->session,
			    "%-10s%" PRIu64 " {%.*s}",
			    "insert", tinfo->keyno,
			    (int)tinfo->value->size,
			    (char *)tinfo->value->data);
	}

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED) {
		key_gen(tinfo->key, tinfo->keyno);
		bdb_update(
		    tinfo->key->data, tinfo->key->size,
		    tinfo->value->data, tinfo->value->size);
	}
#endif
	return (0);
}

/*
 * row_remove --
 *	Remove an row from a row-store file.
 */
static int
row_remove(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned) {
		key_gen(tinfo->key, tinfo->keyno);
		cursor->set_key(cursor, tinfo->key);
	}

	/* We use the cursor in overwrite mode, check for existence. */
	if ((ret = read_op(cursor, SEARCH, NULL)) == 0)
		ret = cursor->remove(cursor);

	if (ret != 0 && ret != WT_NOTFOUND)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api,
		    cursor->session, "%-10s%" PRIu64, "remove", tinfo->keyno);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED) {
		int notfound;

		bdb_remove(tinfo->keyno, &notfound);
		(void)notfound_chk("row_remove", ret, notfound, tinfo->keyno);
	}
#endif
	return (ret);
}

/*
 * col_remove --
 *	Remove a row from a column-store file.
 */
static int
col_remove(TINFO *tinfo, WT_CURSOR *cursor, bool positioned)
{
	WT_DECL_RET;

	if (!positioned)
		cursor->set_key(cursor, tinfo->keyno);

	/* We use the cursor in overwrite mode, check for existence. */
	if ((ret = read_op(cursor, SEARCH, NULL)) == 0)
		ret = cursor->remove(cursor);

	if (ret != 0 && ret != WT_NOTFOUND)
		return (ret);

	if (g.logging == LOG_OPS)
		(void)g.wt_api->msg_printf(g.wt_api,
		    cursor->session, "%-10s%" PRIu64, "remove", tinfo->keyno);

#ifdef HAVE_BERKELEY_DB
	if (SINGLETHREADED) {
		int notfound;

		bdb_remove(tinfo->keyno, &notfound);
		(void)notfound_chk("col_remove", ret, notfound, tinfo->keyno);
	}
#endif
	return (ret);
}

#ifdef HAVE_BERKELEY_DB
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
		fprintf(stderr, "%s: %s:", progname, f);
		if (keyno != 0)
			fprintf(stderr, " row %" PRIu64 ":", keyno);
		fprintf(stderr,
		    " not found in Berkeley DB, found in WiredTiger\n");
		testutil_die(0, NULL);
	}
	if (wt_ret == WT_NOTFOUND) {
		fprintf(stderr, "%s: %s:", progname, f);
		if (keyno != 0)
			fprintf(stderr, " row %" PRIu64 ":", keyno);
		fprintf(stderr,
		    " found in Berkeley DB, not found in WiredTiger\n");
		testutil_die(0, NULL);
	}
	return (0);
}
#endif
