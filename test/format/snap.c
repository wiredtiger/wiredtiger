/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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
 * Increment the snap pointer, wrapping around as needed.
 */
#define SNAP_NEXT(tinfo, snap) (((snap) + 1 >= (tinfo)->snap_end) ? (tinfo)->snap_list : (snap) + 1)

/*
 * Issue a warning when there enough consecutive unsuccessful checks for rollback to stable.
 */
#define WARN_RTS_NO_CHECK 5

/*
 * snap_init --
 *     Initialize the repeatable operation tracking.
 */
void
snap_init(TINFO *tinfo)
{
    /*
     * We maintain two snap lists. The current one is indicated by tinfo->s, and keeps the most
     * recent operations. The other one is used when we are running with rollback_to_stable. When
     * each thread notices that the stable timestamp has changed, it stashes the current snap list
     * and starts fresh with the other snap list. After we've completed a rollback_to_stable, we can
     * the secondary snap list to see the state of keys/values seen and updated at the time of the
     * rollback.
     */
    if (g.c_txn_rollback_to_stable) {
        tinfo->s = &tinfo->snap_states[1];
        tinfo->snap_list = dcalloc(SNAP_LIST_SIZE, sizeof(SNAP_OPS));
        tinfo->snap_end = &tinfo->snap_list[SNAP_LIST_SIZE];
    }
    tinfo->s = &tinfo->snap_states[0];
    tinfo->snap_list = dcalloc(SNAP_LIST_SIZE, sizeof(SNAP_OPS));
    tinfo->snap_end = &tinfo->snap_list[SNAP_LIST_SIZE];
    tinfo->snap_current = tinfo->snap_list;
}

/*
 * snap_teardown --
 *     Tear down the repeatable operation tracking structures.
 */
void
snap_teardown(TINFO *tinfo)
{
    SNAP_OPS *snaplist;
    u_int i, snap_index;

    for (snap_index = 0; snap_index < WT_ELEMENTS(tinfo->snap_states); snap_index++)
        if ((snaplist = tinfo->snap_states[snap_index].snap_state_list) != NULL) {
            for (i = 0; i < SNAP_LIST_SIZE; ++i) {
                free(snaplist[i].kdata);
                free(snaplist[i].vdata);
            }
            free(snaplist);
        }
}

/*
 * snap_clear --
 *     Clear the snap list.
 */
static void
snap_clear_one(SNAP_OPS *snap, bool free_data)
{
    WT_ITEM *ksave, *vsave;

    if (free_data) {
        free(snap->kdata);
        free(snap->vdata);
        WT_CLEAR(*snap);
    } else {
        /* Preserve allocated memory */
        ksave = snap->kdata;
        vsave = snap->vdata;
        WT_CLEAR(*snap);
        snap->kdata = ksave;
        snap->vdata = vsave;
    }
}

/*
 * snap_clear --
 *     Clear the snap list.
 */
static void
snap_clear(TINFO *tinfo)
{
    SNAP_OPS *snap;

    for (snap = tinfo->snap_list; snap < tinfo->snap_end; ++snap)
        snap_clear_one(snap, false);
}

/*
 * snap_clear_range --
 *     Clear a portion of the snap list.
 */
static void
snap_clear_range(TINFO *tinfo, SNAP_OPS *begin, SNAP_OPS *end)
{
    SNAP_OPS *snap;

    for (snap = begin; snap != end; snap = SNAP_NEXT(tinfo, snap))
        snap_clear_one(snap, false);
}

/*
 * snap_op_end --
 *     Finish a set of repeatable operations (transaction).
 */
void
snap_op_end(TINFO *tinfo, bool committed)
{
    SNAP_OPS *snap;

    /*
     * There's some extra work we need to do that's only applicable to rollback_to_stable checking.
     */
    if (g.c_txn_rollback_to_stable) {
        /*
         * If we wrapped the buffer, clear it out, it won't be useful for rollback checking.
         */
        if (tinfo->repeatable_wrap)
            snap_clear(tinfo);
        else if (!committed) {
            snap_clear_range(tinfo, tinfo->snap_first, tinfo->snap_current);
            tinfo->snap_current = tinfo->snap_first;
        } else
            /*
             * For write operations in this transaction, set the timestamp to be the commit
             * timestamp.
             */
            for (snap = tinfo->snap_first; snap != tinfo->snap_current;
                 snap = SNAP_NEXT(tinfo, snap)) {
                testutil_assert(snap->opid == tinfo->opid);
                if (snap->op != READ)
                    snap->ts = tinfo->commit_ts;
            }
    }
}

/*
 * snap_op_init --
 *     Initialize the repeatable operation tracking for each new operation.
 */
void
snap_op_init(TINFO *tinfo, uint64_t read_ts, bool repeatable_reads)
{
    uint64_t stable_ts;

    ++tinfo->opid;
    tinfo->op_order = 0;

    if (g.c_txn_rollback_to_stable) {
        /*
         * If the stable timestamp has changed and we've advanced beyond it, preserve the current
         * snapshot history up to this point, we'll use it verify rollback_to_stable. Switch our
         * tracking to the other snap list.
         */
        stable_ts = __wt_atomic_addv64(&g.stable_timestamp, 0);
        if (stable_ts != tinfo->stable_ts && read_ts > stable_ts) {
            tinfo->stable_ts = stable_ts;
            if (tinfo->s == &tinfo->snap_states[0])
                tinfo->s = &tinfo->snap_states[1];
            else
                tinfo->s = &tinfo->snap_states[0];
            tinfo->snap_current = tinfo->snap_list;

            /* Clear out older info from the snap list. */
            snap_clear(tinfo);
        }
    }

    tinfo->snap_first = tinfo->snap_current;

    tinfo->read_ts = read_ts;
    tinfo->repeatable_reads = repeatable_reads;
    tinfo->repeatable_wrap = false;
}

/*
 * snap_track --
 *     Add a single snapshot isolation returned value to the list.
 */
void
snap_track(TINFO *tinfo, thread_op op)
{
    WT_ITEM *ip;
    SNAP_OPS *snap;

    snap = tinfo->snap_current;
    snap->op = op;
    snap->opid = tinfo->opid;
    snap->op_order = tinfo->op_order++;
    snap->keyno = tinfo->keyno;
    snap->ts = WT_TS_NONE;
    snap->repeatable = false;
    snap->last = op == TRUNCATE ? tinfo->last : 0;
    snap->ksize = snap->vsize = 0;

    if (op == INSERT && g.type == ROW) {
        ip = tinfo->key;
        if (snap->kmemsize < ip->size) {
            snap->kdata = drealloc(snap->kdata, ip->size);
            snap->kmemsize = ip->size;
        }
        memcpy(snap->kdata, ip->data, snap->ksize = ip->size);
    }

    if (op != REMOVE && op != TRUNCATE) {
        ip = tinfo->value;
        if (snap->vmemsize < ip->size) {
            snap->vdata = drealloc(snap->vdata, ip->size);
            snap->vmemsize = ip->size;
        }
        memcpy(snap->vdata, ip->data, snap->vsize = ip->size);
    }

    /* Move to the next slot, wrap at the end of the circular buffer. */
    if (++tinfo->snap_current >= tinfo->snap_end)
        tinfo->snap_current = tinfo->snap_list;

    /*
     * It's possible to pass this transaction's buffer starting point and start replacing our own
     * entries. If that happens, we can't repeat operations because we don't know which ones were
     * previously modified.
     */
    if (tinfo->snap_current->opid == tinfo->opid)
        tinfo->repeatable_wrap = true;
}

/*
 * print_item_data --
 *     Display a single data/size pair, with a tag.
 */
static void
print_item_data(const char *tag, const uint8_t *data, size_t size)
{
    WT_ITEM tmp;

    if (g.type == FIX) {
        fprintf(stderr, "%s {0x%02x}\n", tag, data[0]);
        return;
    }

    memset(&tmp, 0, sizeof(tmp));
    testutil_check(__wt_raw_to_esc_hex(NULL, data, size, &tmp));
    fprintf(stderr, "%s {%s}\n", tag, (char *)tmp.mem);
    __wt_buf_free(NULL, &tmp);
}

/*
 * snap_verify --
 *     Repeat a read and verify the contents.
 */
static int
snap_verify(WT_CURSOR *cursor, TINFO *tinfo, SNAP_OPS *snap)
{
    WT_DECL_RET;
    WT_ITEM *key, *value;
    uint64_t keyno;
    uint8_t bitfield;

    testutil_assert(snap->op != TRUNCATE);

    key = tinfo->key;
    value = tinfo->value;
    keyno = snap->keyno;

    /*
     * Retrieve the key/value pair by key. Row-store inserts have a unique generated key we saved,
     * else generate the key from the key number.
     */
    if (snap->op == INSERT && g.type == ROW) {
        key->data = snap->kdata;
        key->size = snap->ksize;
        cursor->set_key(cursor, key);
    } else {
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
    if (ret == 0 && snap->op != REMOVE && value->size == snap->vsize &&
      memcmp(value->data, snap->vdata, value->size) == 0)
        return (0);
    if (ret == WT_NOTFOUND && snap->op == REMOVE)
        return (0);

    /*
     * In fixed length stores, zero values at the end of the key space are returned as not-found,
     * and not-found row reads are saved as zero values. Map back-and-forth for simplicity.
     */
    if (g.type == FIX) {
        if (ret == WT_NOTFOUND && snap->vsize == 1 && *(uint8_t *)snap->vdata == 0)
            return (0);
        if (snap->op == REMOVE && value->size == 1 && *(uint8_t *)value->data == 0)
            return (0);
    }

    /* Things went pear-shaped. */
    switch (g.type) {
    case FIX:
        fprintf(stderr,
          "snapshot-isolation: %" PRIu64 " search: expected {0x%02x}, found {0x%02x}\n", keyno,
          snap->op == REMOVE ? 0U : *(uint8_t *)snap->vdata,
          ret == WT_NOTFOUND ? 0U : *(uint8_t *)value->data);
        break;
    case ROW:
        fprintf(
          stderr, "snapshot-isolation %.*s search mismatch\n", (int)key->size, (char *)key->data);

        if (snap->op == REMOVE)
            fprintf(stderr, "expected {deleted}\n");
        else
            print_item_data("expected", snap->vdata, snap->vsize);
        if (ret == WT_NOTFOUND)
            fprintf(stderr, "   found {deleted}\n");
        else
            print_item_data("   found", value->data, value->size);
        break;
    case VAR:
        fprintf(stderr, "snapshot-isolation %" PRIu64 " search mismatch\n", keyno);

        if (snap->op == REMOVE)
            fprintf(stderr, "expected {deleted}\n");
        else
            print_item_data("expected", snap->vdata, snap->vsize);
        if (ret == WT_NOTFOUND)
            fprintf(stderr, "   found {deleted}\n");
        else
            print_item_data("   found", value->data, value->size);
        break;
    }

    g.page_dump_cursor = cursor;
    testutil_assert(0);

    /* NOTREACHED */
    return (1);
}

/*
 * snap_ts_clear --
 *     Clear snapshots at or before a specified timestamp.
 */
static void
snap_ts_clear(TINFO *tinfo, uint64_t ts)
{
    SNAP_OPS *snap;

    /* Check from the first slot to the last. */
    for (snap = tinfo->snap_list; snap < tinfo->snap_end; ++snap)
        if (snap->repeatable && snap->ts <= ts)
            snap->repeatable = false;
}

/*
 * snap_repeat_ok_match --
 *     Compare two operations and see if they modified the same record.
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
        if (g.c_reverse && (a->keyno == 0 || a->keyno >= current->keyno) &&
          (a->last == 0 || a->last <= current->keyno))
            return (false);
        if (!g.c_reverse && (a->keyno == 0 || a->keyno <= current->keyno) &&
          (a->last == 0 || a->last >= current->keyno))
            return (false);
    }

    return (true);
}

/*
 * snap_repeat_ok_commit --
 *     Return if an operation in the transaction can be repeated, where the transaction isn't yet
 *     committed (so all locks are in place), or has already committed successfully.
 */
static bool
snap_repeat_ok_commit(TINFO *tinfo, SNAP_OPS *current)
{
    SNAP_OPS *p;

    /*
     * Truncates can't be repeated, we don't know the exact range of records that were removed (if
     * any).
     */
    if (current->op == TRUNCATE)
        return (false);

    /*
     * For updates, check for subsequent changes to the record and don't repeat the read. For reads,
     * check for either subsequent or previous changes to the record and don't repeat the read. (The
     * reads are repeatable, but only at the commit timestamp, and the update will do the repeatable
     * read in that case.)
     */
    for (p = current;;) {
        /* Wrap at the end of the circular buffer. */
        if (++p >= tinfo->snap_end)
            p = tinfo->snap_list;
        if (p->opid != tinfo->opid)
            break;

        if (!snap_repeat_ok_match(current, p))
            return (false);
    }

    if (current->op != READ)
        return (true);
    for (p = current;;) {
        /* Wrap at the beginning of the circular buffer. */
        if (--p < tinfo->snap_list)
            p = &tinfo->snap_list[SNAP_LIST_SIZE - 1];
        if (p->opid != tinfo->opid)
            break;

        if (!snap_repeat_ok_match(current, p))
            return (false);
    }
    return (true);
}

/*
 * snap_repeat_ok_rollback --
 *     Return if an operation in the transaction can be repeated, after a transaction has rolled
 *     back.
 */
static bool
snap_repeat_ok_rollback(TINFO *tinfo, SNAP_OPS *current)
{
    SNAP_OPS *p;

    /* Ignore update operations, they can't be repeated after rollback. */
    if (current->op != READ)
        return (false);

    /*
     * Check for previous changes to the record and don't attempt to repeat the read in that case.
     */
    for (p = current;;) {
        /* Wrap at the beginning of the circular buffer. */
        if (--p < tinfo->snap_list)
            p = &tinfo->snap_list[SNAP_LIST_SIZE - 1];
        if (p->opid != tinfo->opid)
            break;

        if (!snap_repeat_ok_match(current, p))
            return (false);
    }
    return (true);
}

/*
 * snap_repeat_txn --
 *     Repeat each operation done within a snapshot isolation transaction.
 */
int
snap_repeat_txn(WT_CURSOR *cursor, TINFO *tinfo)
{
    SNAP_OPS *current;

    /* If we wrapped the buffer, we can't repeat operations. */
    if (tinfo->repeatable_wrap)
        return (0);

    /* Check from the first operation we saved to the last. */
    for (current = tinfo->snap_first;; ++current) {
        /* Wrap at the end of the circular buffer. */
        if (current >= tinfo->snap_end)
            current = tinfo->snap_list;
        if (current->opid != tinfo->opid)
            break;

        /*
         * The transaction is not yet resolved, so the rules are as if the transaction has
         * committed. Note we are NOT checking if reads are repeatable based on the chosen
         * timestamp. This is because we expect snapshot isolation to work even in the presence of
         * other threads of control committing in our past, until the transaction resolves.
         */
        if (snap_repeat_ok_commit(tinfo, current))
            WT_RET(snap_verify(cursor, tinfo, current));
    }

    return (0);
}

/*
 * snap_repeat_update --
 *     Update the list of snapshot operations based on final transaction resolution.
 */
void
snap_repeat_update(TINFO *tinfo, bool committed)
{
    SNAP_OPS *current;

    /* If we wrapped the buffer, we can't repeat operations. */
    if (tinfo->repeatable_wrap)
        return;

    /* Check from the first operation we saved to the last. */
    for (current = tinfo->snap_first;; ++current) {
        /* Wrap at the end of the circular buffer. */
        if (current >= tinfo->snap_end)
            current = tinfo->snap_list;
        if (current->opid != tinfo->opid)
            break;

        /*
         * First, reads may simply not be repeatable because the read timestamp chosen wasn't older
         * than all concurrently running uncommitted updates.
         */
        if (!tinfo->repeatable_reads && current->op == READ)
            continue;

        /*
         * Second, check based on the transaction resolution (the rules are different if the
         * transaction committed or rolled back).
         */
        current->repeatable = committed ? snap_repeat_ok_commit(tinfo, current) :
                                          snap_repeat_ok_rollback(tinfo, current);

        /*
         * Repeat reads at the transaction's read timestamp and updates at the commit timestamp.
         */
        if (current->repeatable)
            current->ts = current->op == READ ? tinfo->read_ts : tinfo->commit_ts;
    }
}

/*
 * snap_repeat_single --
 *     Repeat an historic operation.
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
     * Start at a random spot in the list of operations and look for a read to retry. Stop when
     * we've walked the entire list or found one.
     */
    v = mmrand(&tinfo->rnd, 1, SNAP_LIST_SIZE) - 1;
    for (snap = &tinfo->snap_list[v], count = SNAP_LIST_SIZE; count > 0; --count, ++snap) {
        /* Wrap at the end of the circular buffer. */
        if (snap >= tinfo->snap_end)
            snap = tinfo->snap_list;

        if (snap->repeatable)
            break;
    }

    if (count == 0)
        return;

    /*
     * Start a new transaction. Set the read timestamp. Verify the record. Discard the transaction.
     */
    while ((ret = session->begin_transaction(session, "isolation=snapshot")) == WT_CACHE_FULL)
        __wt_yield();
    testutil_check(ret);

    /*
     * If the timestamp has aged out of the system, we'll get EINVAL when we try and set it.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "read_timestamp=%" PRIx64, snap->ts));

    ret = session->timestamp_transaction(session, buf);
    if (ret == 0) {
        trace_op(tinfo, "repeat %" PRIu64 " ts=%" PRIu64 " {%s}", snap->keyno, snap->ts,
          trace_bytes(tinfo, snap->vdata, snap->vsize));

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

/*
 * compare_snap_ts --
 *     Compare for qsort.
 */
static int
compare_snap_ts(const void *a, const void *b)
{
    SNAP_OPS *snap_a, *snap_b;

    snap_a = *(SNAP_OPS **)a;
    snap_b = *(SNAP_OPS **)b;

    /*
     * Compare so that the highest timestamp sorts first. If timestamps are equal, the operations
     * are from the same transaction, and since it's possible that both operations modified the same
     * record, choose the latest.
     */
    if (snap_a->ts > snap_b->ts)
        return (-1);
    else if (snap_a->ts < snap_b->ts)
        return (1);
    else if (snap_a->op_order > snap_b->op_order)
        return (-1);
    else if (snap_a->op_order < snap_b->op_order)
        return (1);
    else
        return (0);
}

/*
 * snap_repeat_rollback --
 *     Repeat all known operations after a rollback.
 */
void
snap_repeat_rollback(WT_CURSOR *cursor, TINFO **tinfo_array, size_t tinfo_count)
{
    SNAP_STATE *state;
    SNAP_OPS **psnap, *snap, **sorted_end, **sorted_snaps;
    TINFO *first_tinfo, *tinfo, **tinfop;
    WT_CURSOR *seen_cursor;
    WT_DECL_RET;
    WT_ITEM null_value, value;
    WT_SESSION *session;
    u_int64_t keyno, last_keyno, newest_oldest_ts, oldest_ts;
    u_int count;
    size_t i, statenum;
    char buf[100];

    count = 0;
    session = cursor->session;
    first_tinfo = *tinfo_array;

    track("rollback_to_stable: checking", 0ULL, NULL);
    /*
     * Since rolling back to stable effects all changes made, we need to look at changes made by all
     * threads collectively. We'll work backwards from the most recent operations since rollback to
     * stable, repeating each one. To do this, we first collect all snap operations from all threads
     * that may be relevant.
     *
     * We need to limit how far back we can examine. For example, if we see a modification for key X
     * in thread T, there may in fact have been a more recent modification for key X in thread U,
     * but we can't see it because the snap list for U has wrapped past the modification for key X.
     * We need to look at the oldest timestamp recorded in the snaps for each thread, and use the
     * maximum of all of these for our limit. That's the newest, oldest timestamp.
     */
    newest_oldest_ts = 0ULL;
    sorted_end = sorted_snaps = dcalloc(SNAP_LIST_SIZE * tinfo_count, sizeof(SNAP_OPS *));

    for (i = 0, tinfop = tinfo_array; i < tinfo_count; ++i, ++tinfop) {
        tinfo = *tinfop;
        /*
         * If this thread has knowledge of the current stable timestamp, that means its "other" snap
         * list stores up to the stable timestamp, it's the one we want to use. If this thread
         * doesn't yet have knowledge of the current stable, that means the current snap list is the
         * one we want.
         */
        if (tinfo->stable_ts != g.stable_timestamp)
            state = tinfo->s;
        else if (tinfo->s == &tinfo->snap_states[0])
            state = &tinfo->snap_states[1];
        else
            state = &tinfo->snap_states[0];
        oldest_ts = UINT64_MAX;
        for (snap = state->snap_state_list; snap < state->snap_state_end; ++snap) {
            /*
             * Only keep entries that aren't cleared out and may have relevant timestamps. We don't
             * fully know which timestamps are relevant, since we haven't computed the newest oldest
             * yet. We do keep entries that are not marked repeatable, we won't retry unrepeatable
             * reads, but we need them to invalidate keys that we shouldn't check.
             */
            if (snap->op != 0 && snap->ts != 0 && snap->ts <= g.stable_timestamp) {
                oldest_ts = WT_MIN(oldest_ts, snap->ts);
                *sorted_end++ = snap;
            }
        }

        /*
         * If there aren't any entries older than the stable timestamp, we've wrapped around. This
         * thread may have made changes to any key right up to the stable time that have now been
         * overwritten. There's no way to get an accurate accounting, so we skip checking for this
         * run.
         */
        if (oldest_ts == UINT64_MAX)
            goto cleanup;
        newest_oldest_ts = WT_MAX(newest_oldest_ts, oldest_ts);
    }

    __wt_qsort(
      sorted_snaps, (size_t)(sorted_end - sorted_snaps), sizeof(SNAP_OPS *), compare_snap_ts);

    /*
     * Start a new transaction. Verify all repeatable records. Discard the transaction.
     */
    while ((ret = session->begin_transaction(session, "isolation=snapshot")) == WT_CACHE_FULL)
        __wt_yield();
    testutil_check(ret);

    if (g.c_assert_read_timestamp) {
        testutil_check(
          __wt_snprintf(buf, sizeof(buf), "read_timestamp=%" PRIx64, g.stable_timestamp));
        testutil_check(session->timestamp_transaction(session, buf));
    }

    testutil_check(session->create(session, "table:wt_snap_keys", "key_format=u,value_format=u"));
    testutil_check(session->truncate(session, "table:wt_snap_keys", NULL, NULL, NULL));
    testutil_check(session->open_cursor(session, "table:wt_snap_keys", NULL, NULL, &seen_cursor));

    /*
     * Now apply them, taking note of what keys have been seen. If we've seen a key previously,
     * we've already checked it, against a more recent value.
     */
    null_value.data = NULL;
    null_value.size = 0;
    for (psnap = sorted_snaps; psnap < sorted_end; ++psnap) {
        snap = *psnap;
        if (snap->ts < newest_oldest_ts)
            continue;
        last_keyno = (snap->op == TRUNCATE) ? snap->last : snap->keyno;
        for (keyno = snap->keyno; keyno <= last_keyno; ++keyno) {
            key_gen(first_tinfo->key, keyno);
            seen_cursor->set_key(seen_cursor, first_tinfo->key);
            if ((ret = seen_cursor->search(seen_cursor)) == 0)
                continue;
            if (ret != WT_NOTFOUND)
                testutil_check(ret);
            seen_cursor->set_value(seen_cursor, &null_value);
            testutil_check(seen_cursor->insert(seen_cursor));

            if (snap->op == TRUNCATE) {
                cursor->set_key(cursor, first_tinfo->key);
                if ((ret = read_op(cursor, SEARCH, NULL)) != WT_NOTFOUND) {
                    testutil_check(ret);

                    /* A truncated record was unexpectedly found. */
                    seen_cursor->get_value(seen_cursor, &value);
                    if (g.type == FIX) {
                        /* A zero value is equivalent to not found for fixed length stores. */
                        if (value.size == 1 && *(uint8_t *)value.data == 0)
                            continue;
                        fprintf(stderr, "snapshot-isolation: %" PRIu64
                                        " search: expected {0x00}, found {0x%02x}\n",
                          keyno, *(uint8_t *)value.data);
                    } else {
                        fprintf(stderr, "snapshot-isolation %.*s search mismatch\n",
                          (int)first_tinfo->key->size, (char *)first_tinfo->key->data);
                        fprintf(stderr, "expected {deleted}\n");
                        print_item_data("   found", value.data, value.size);
                    }
                    testutil_assert(0);
                }
            } else if (snap->repeatable || snap->op != READ)
                /*
                 * The tinfo argument is used to stash the key value pair found during checking.
                 */
                testutil_check(snap_verify(cursor, first_tinfo, snap));

            ++count;
            if (count % 100 == 0) {
                testutil_check(__wt_snprintf(buf, sizeof(buf),
                    "rollback_to_stable: %" PRIu32 " ops repeated", count));
                track(buf, 0ULL, NULL);
            }
        }
    }
    /*
     * Show the final result and check that we're accomplishing some checking.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf),
        "rollback_to_stable: %" PRIu32 " ops repeated", count));
    track(buf, 0ULL, NULL);
    if (count == 0) {
        if (++g.rts_no_check >= WARN_RTS_NO_CHECK)
            fprintf(stderr, "Warning: %d consecutive runs with no rollback_to_stable checking\n",
              count);
    } else
            g.rts_no_check = 0;

    testutil_check(seen_cursor->close(seen_cursor));

    /* Discard the transaction. */
    testutil_check(session->rollback_transaction(session, NULL));

cleanup:
    /*
     * After a rollback_to_stable, we can't trust some of our snap data. Rather than figure out what
     * is good or bad, we'll invalidate it all.
     */
    for (i = 0, tinfop = tinfo_array; i < tinfo_count; ++i, ++tinfop) {
        tinfo = *tinfop;
        for (statenum = 0; statenum < WT_ELEMENTS(tinfo->snap_states); statenum++) {
            state = &tinfo->snap_states[statenum];
            for (snap = state->snap_state_list; snap < state->snap_state_end; ++snap)
                snap_clear_one(snap, true);
        }
    }
}
