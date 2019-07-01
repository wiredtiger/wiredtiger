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

/*
 * snap_track --
 *     Add a single snapshot isolation returned value to the list.
 */
void
snap_track(TINFO *tinfo, thread_op op)
{
	WT_ITEM *ip;
	SNAP_OPS *snap;

	snap = tinfo->snap;
	snap->op = op;
	snap->keyno = tinfo->keyno;
	snap->ts = WT_TS_NONE;
	snap->repeatable = false;
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

	/*
	 * Move to the next slot, wrap at the end of the circular buffer.
	 *
	 * It's possible to pass this transaction's buffer starting point and
	 * start replacing our own entries. That's OK, we just skip earlier
	 * operations when we check.
	 */
	if (++tinfo->snap >= &tinfo->snap_list[WT_ELEMENTS(tinfo->snap_list)])
		tinfo->snap = tinfo->snap_list;
}

/*
 * snap_verify --
 *	Repeat a read and verify the contents.
 */
static int
snap_verify(WT_CURSOR *cursor, TINFO *tinfo, SNAP_OPS *snap)
{
	WT_DECL_RET;
	WT_ITEM *key, *value;
	uint8_t bitfield;

	key = tinfo->key;
	value = tinfo->value;

	/*
	 * Retrieve the key/value pair by key. Row-store inserts have a unique
	 * generated key we saved, else generate the key from the key number.
	 */
	if (snap->op == INSERT && g.type == ROW) {
		key->data = snap->kdata;
		key->size = snap->ksize;
		cursor->set_key(cursor, key);
	} else {
		switch (g.type) {
		case FIX:
		case VAR:
			cursor->set_key(cursor, snap->keyno);
			break;
		case ROW:
			key_gen(key, snap->keyno);
			cursor->set_key(cursor, key);
			break;
		}
	}

	switch (ret = read_op(cursor, SEARCH, NULL)) {
	case 0:
		if (g.type == FIX) {
			testutil_check(cursor->get_value(cursor, &bitfield));
			*(uint8_t *)(value->data) = bitfield;
			value->size = 1;
		} else
			testutil_check(cursor->get_value(cursor, value));
		break;
	case WT_NOTFOUND:
		break;
	default:
		return (ret);
	}

	/* Check for simple matches. */
	if (ret == 0 &&
	    snap->op != REMOVE && snap->op != TRUNCATE &&
	    value->size == snap->vsize &&
	    memcmp(value->data, snap->vdata, value->size) == 0)
		return (0);
	if (ret == WT_NOTFOUND && (snap->op == REMOVE || snap->op == TRUNCATE))
		return (0);

	/*
	 * In fixed length stores, zero values at the end of the key space are
	 * returned as not-found, and not-found row reads are saved as zero
	 * values. Map back-and-forth for simplicity.
	 */
	if (g.type == FIX) {
		if (ret == WT_NOTFOUND &&
		    snap->vsize == 1 && *(uint8_t *)snap->vdata == 0)
			return (0);
		if ((snap->op == REMOVE || snap->op == TRUNCATE) &&
		    value->size == 1 && *(uint8_t *)value->data == 0)
			return (0);
	}

	/* Things went pear-shaped. */
	switch (g.type) {
	case FIX:
		testutil_die(ret,
		    "snapshot-isolation: %" PRIu64 " search: "
		    "expected {0x%02x}, found {0x%02x}",
		    snap->keyno,
		    snap->op == REMOVE ? 0 : *(uint8_t *)snap->vdata,
		    ret == WT_NOTFOUND ? 0 : *(uint8_t *)value->data);
		/* NOTREACHED */
	case ROW:
		fprintf(stderr,
		    "snapshot-isolation %.*s search mismatch\n",
		    (int)key->size, (char *)key->data);

		if (snap->op == REMOVE)
			fprintf(stderr, "expected {deleted}\n");
		else
			print_item_data("expected", snap->vdata, snap->vsize);
		if (ret == WT_NOTFOUND)
			fprintf(stderr, "   found {deleted}\n");
		else
			print_item_data("   found", value->data, value->size);

		testutil_die(ret,
		    "snapshot-isolation: %.*s search mismatch",
		    (int)key->size, (char *)key->data);
		/* NOTREACHED */
	case VAR:
		fprintf(stderr,
		    "snapshot-isolation %" PRIu64 " search mismatch\n",
		    snap->keyno);

		if (snap->op == REMOVE)
			fprintf(stderr, "expected {deleted}\n");
		else
			print_item_data("expected", snap->vdata, snap->vsize);
		if (ret == WT_NOTFOUND)
			fprintf(stderr, "   found {deleted}\n");
		else
			print_item_data("   found", value->data, value->size);

		testutil_die(ret,
		    "snapshot-isolation: %" PRIu64 " search mismatch",
		    snap->keyno);
		/* NOTREACHED */
	}

	/* NOTREACHED */
	return (1);
}

/*
 * snap_ts_clear --
 *	Clear snapshots at or before a specified timestamp.
 */
static void
snap_ts_clear(TINFO *tinfo, uint64_t ts)
{
	SNAP_OPS *snap;
	int count;

	/* Check from the first operation to the last. */
	for (snap = tinfo->snap_list,
	    count = WT_ELEMENTS(tinfo->snap_list); count > 0; --count, ++snap)
		if (snap->repeatable && snap->ts <= ts)
			snap->repeatable = false;
}

/*
 * snap_repeat_ok_match --
 *	Compare two operations and see if they modified the same record.
 */
static bool
snap_repeat_ok_match(SNAP_OPS *current, SNAP_OPS *a)
{
	/* Reads are never a problem, there's no modification. */
	if (a->op == READ)
		return (true);

	/* Check for a matching single record modification. */
	if (a->keyno == current->keyno)
		return (false);

	/* Truncates are slightly harder, make sure the ranges don't overlap. */
	if (a->op == TRUNCATE) {
		if (g.c_reverse &&
		    (a->keyno == 0 || a->keyno >= current->keyno) &&
		    (a->last == 0 || a->last <= current->keyno))
			return (false);
		if (!g.c_reverse &&
		    (a->keyno == 0 || a->keyno <= current->keyno) &&
		    (a->last == 0 || a->last >= current->keyno))
			return (false);
	}

	return (true);
}

/*
 * snap_repeat_ok_commit --
 *	Return if an operation in the transaction can be repeated, where the
 * transaction isn't yet committed (so all locks are in place), or has already
 * committed successfully.
 */
static bool
snap_repeat_ok_commit(
    TINFO *tinfo, SNAP_OPS *current, SNAP_OPS *first, SNAP_OPS *last)
{
	SNAP_OPS *p;

	/*
	 * For updates, check for subsequent changes to the record and don't
	 * repeat the read. For reads, check for either subsequent or previous
	 * changes to the record and don't repeat the read. (The reads are
	 * repeatable, but only at the commit timestamp, and the update will
	 * do the repeatable read in that case.)
	 */
	for (p = current;;) {
		/*
		 * Wrap at the end of the circular buffer; "last" is the element
		 * after the last element we want to test.
		 */
		if (++p >= &tinfo->snap_list[WT_ELEMENTS(tinfo->snap_list)])
			p = tinfo->snap_list;
		if (p == last)
			break;

		if (!snap_repeat_ok_match(current, p))
			return (false);
	}

	if (current->op != READ)
		return (true);
	for (p = current;;) {
		/*
		 * Wrap at the beginning of the circular buffer; "first" is the
		 * last element we want to test.
		 */
		if (p == first)
			return (true);
		if (--p < tinfo->snap_list)
			p = &tinfo->snap_list[
			    WT_ELEMENTS(tinfo->snap_list) - 1];

		if (!snap_repeat_ok_match(current, p))
			return (false);

	}
	/* NOTREACHED */
}

/*
 * snap_repeat_ok_rollback --
 *	Return if an operation in the transaction can be repeated, after a
 * transaction has rolled back.
 */
static bool
snap_repeat_ok_rollback(TINFO *tinfo, SNAP_OPS *current, SNAP_OPS *first)
{
	SNAP_OPS *p;

	/* Ignore update operations, they can't be repeated after rollback. */
	if (current->op != READ)
		return (false);

	/*
	 * Check for previous changes to the record and don't attempt to repeat
	 * the read in that case.
	 */
	for (p = current;;) {
		/*
		 * Wrap at the beginning of the circular buffer; "first" is the
		 * last element we want to test.
		 */
		if (p == first)
			return (true);
		if (--p < tinfo->snap_list)
			p = &tinfo->snap_list[
			    WT_ELEMENTS(tinfo->snap_list) - 1];

		if (!snap_repeat_ok_match(current, p))
			return (false);

	}
	/* NOTREACHED */
}

/*
 * snap_repeat_txn --
 *	Repeat each operation done within a snapshot isolation transaction.
 */
int
snap_repeat_txn(WT_CURSOR *cursor, TINFO *tinfo)
{
	SNAP_OPS *current, *stop;

	/* Check from the first operation we saved to the last. */
	for (current = tinfo->snap_first, stop = tinfo->snap;; ++current) {
		/* Wrap at the end of the circular buffer. */
		if (current >= &tinfo->snap_list[WT_ELEMENTS(tinfo->snap_list)])
			current = tinfo->snap_list;
		if (current == stop)
			break;

		/*
		 * We don't test all of the records in a truncate range, only
		 * the first because that matches the rest of the isolation
		 * checks. If a truncate range was from the start of the table,
		 * switch to the record at the end. This is done in the first
		 * routine that considers if operations are repeatable, and the
		 * rest of those functions depend on it already being done.
		 */
		if (current->op == TRUNCATE && current->keyno == 0) {
			current->keyno = current->last;
			testutil_assert(current->keyno != 0);
		}

		if (snap_repeat_ok_commit(
		    tinfo, current, tinfo->snap_first, stop))
			WT_RET(snap_verify(cursor, tinfo, current));
	}

	return (0);
}

/*
 * snap_repeat_update --
 *	Update the list of snapshot operations based on final transaction
 * resolution.
 */
void
snap_repeat_update(TINFO *tinfo, bool committed)
{
	SNAP_OPS *start, *stop;

	/*
	 * Check from the first operation we saved to the last. It's possible
	 * to update none at all if we did exactly the number of operations
	 * in the circular buffer, it will look like we didn't do any. That's
	 * OK, it's a big enough buffer that it's not going to matter.
	 */
	for (start = tinfo->snap_first, stop = tinfo->snap;; ++start) {
		/* Wrap at the end of the circular buffer. */
		if (start >= &tinfo->snap_list[WT_ELEMENTS(tinfo->snap_list)])
			start = tinfo->snap_list;
		if (start == stop)
			break;

		/*
		 * First, reads may simply not be repeatable because the read
		 * timestamp chosen wasn't older than all concurrently running
		 * uncommitted updates.
		 */
		if (!tinfo->repeatable_reads && start->op == READ)
			continue;

		/*
		 * Second, check based on the transaction resolution (the rules
		 * are different if the transaction committed or rolled back).
		 */
		start->repeatable = committed ? snap_repeat_ok_commit(
		    tinfo, start, tinfo->snap_first, stop) :
		    snap_repeat_ok_rollback(tinfo, start, tinfo->snap_first);

		/*
		 * Repeat reads at the transaction's read timestamp and updates
		 * at the commit timestamp.
		 */
		if (start->repeatable)
			start->ts = start->op == READ ?
			    tinfo->read_ts : tinfo->commit_ts;
	}
}

/*
 * snap_repeat_single --
 *	Repeat an historic operation.
 */
void
snap_repeat_single(WT_CURSOR *cursor, TINFO *tinfo)
{
	SNAP_OPS *snap;
	WT_DECL_RET;
	WT_SESSION *session;
	int count;
	u_int v;
	char buf[64];

	session = cursor->session;

	/*
	 * Start at a random spot in the list of operations and look for a read
	 * to retry. Stop when we've walked the entire list or found one.
	 */
	v = mmrand(&tinfo->rnd, 1, WT_ELEMENTS(tinfo->snap_list)) - 1;
	for (snap = &tinfo->snap_list[v],
	    count = WT_ELEMENTS(tinfo->snap_list); count > 0; --count, ++snap) {
		/* Wrap at the end of the circular buffer. */
		if (snap >= &tinfo->snap_list[WT_ELEMENTS(tinfo->snap_list)])
			snap = tinfo->snap_list;

		if (snap->repeatable)
			break;
	}

	if (count == 0)
		return;

	/*
	 * Start a new transaction.
	 * Set the read timestamp.
	 * Verify the record.
	 * Discard the transaction.
	 */
	while ((ret = session->begin_transaction(
	    session, "isolation=snapshot")) == WT_CACHE_FULL)
		__wt_yield();
	testutil_check(ret);

	/*
	 * If the timestamp has aged out of the system, we'll get EINVAL when we
	 * try and set it.
	 */
	testutil_check(__wt_snprintf(
	    buf, sizeof(buf), "read_timestamp=%" PRIx64, snap->ts));

	ret = session->timestamp_transaction(session, buf);
	if (ret == 0) {
		logop(session, "%-10s%" PRIu64 " ts=%" PRIu64 " {%.*s}",
		    "repeat", snap->keyno, snap->ts,
		    (int)snap->vsize, (char *)snap->vdata);

		/* The only expected error is rollback. */
		ret = snap_verify(cursor, tinfo, snap);

		if (ret != 0 && ret != WT_ROLLBACK)
			testutil_check(ret);
	} else if (ret == EINVAL)
		snap_ts_clear(tinfo, snap->ts);
	else
		testutil_check(ret);

	/* Discard the transaction. */
	testutil_check(session->rollback_transaction(session, NULL));
}
