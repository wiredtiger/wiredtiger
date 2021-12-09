/*-
 * Public Domain 2014-present MongoDB, Inc.
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
 * table_verify --
 *     Verify a single table.
 */
void
table_verify(TABLE *table, void *arg)
{
    SAP sap;
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_SESSION *session;
    int i;

    conn = (WT_CONNECTION *)arg;
    testutil_assert(table != NULL);

    /*
     * Verify can return EBUSY if the handle isn't available. Eventually quit trying, the handle may
     * not be available for a long time in the case of LSM.
     */
    memset(&sap, 0, sizeof(sap));
    wiredtiger_open_session(conn, &sap, table->track_prefix, &session);
    for (i = 0; ++i <= 5; __wt_sleep(1, 0)) {
        if ((ret = session->verify(session, table->uri, "strict")) == 0)
            break;
        testutil_assert(ret == EBUSY);
    }
    if (ret == EBUSY)
        WARN("table.%u skipped verify because of EBUSY", table->id);
    wiredtiger_close_session(session);
}

/*
 * table_mirror_row_next --
 *     Move to the next row-store original record.
 */
static int
table_mirror_row_next(TABLE *table, WT_CURSOR *cursor, WT_ITEM *key, uint64_t *keynop)
{
    WT_DECL_RET;
    const char *p;

    /* RS tables insert records in the table, skip to the next original key/value pair. */
    for (;;) {
        if ((ret = read_op(cursor, NEXT, NULL)) == WT_NOTFOUND)
            return (WT_NOTFOUND);
        /* WT_ROLLBACK isn't illegal, but it would mean restarting the verify somehow. */
        testutil_assert(ret == 0);

        /* The original keys are either short or have ".00" as a suffix. */
        testutil_check(cursor->get_key(cursor, key));
        testutil_assert((p = strchr(key->data, '.')) != NULL);
        testutil_assert(key->size - WT_PTRDIFF(p, key->data) >= 3);
        if (p[1] == '0' && p[2] == '0')
            break;
    }

    /* There may be a common key prefix, skip over it. */
    *keynop = atou32("mirror-verify", (char *)key->data + NTV(table, BTREE_PREFIX_LEN), '.');
    return (0);
}

/*
 * table_verify_mirror --
 *     Verify a mirrored pair.
 */
static void
table_verify_mirror(WT_CONNECTION *conn, TABLE *base, TABLE *table, const char *checkpoint)
{
    SAP sap;
    WT_CURSOR *base_cursor, *table_cursor;
    WT_ITEM base_key, base_value, table_key, table_value;
    WT_SESSION *session;
    uint64_t base_keyno, table_keyno, rows;
    uint64_t fail_cmp_count, fail_size_count;
    uint8_t base_bitv, table_bitv;
    int base_ret, table_ret;
    char buf[256];

    base_keyno = table_keyno = 0;             /* -Wconditional-uninitialized */
    base_bitv = table_bitv = FIX_VALUE_WRONG; /* -Wconditional-uninitialized */
    base_ret = table_ret = 0;

    memset(&sap, 0, sizeof(sap));
    wiredtiger_open_session(conn, &sap, NULL, &session);

    /* Optionally open a checkpoint to verify. */
    if (checkpoint != NULL)
        testutil_check(__wt_snprintf(buf, sizeof(buf), "checkpoint=%s", checkpoint));
    wiredtiger_open_cursor(session, base->uri, checkpoint == NULL ? NULL : buf, &base_cursor);
    wiredtiger_open_cursor(session, table->uri, checkpoint == NULL ? NULL : buf, &table_cursor);

    testutil_check(__wt_snprintf(buf, sizeof(buf),
      "table %u %s%s"
      "mirror verify",
      table->id, checkpoint == NULL ? "" : checkpoint, checkpoint == NULL ? "" : "checkpoint "));
    trace_msg(session, "%s: start", buf);

    fail_cmp_count = fail_size_count = 0;
    for (rows = 1; rows <= TV(RUNS_ROWS); ++rows) {
        switch (base->type) {
        case FIX:
            testutil_assert(base->type != FIX);
            break;
        case VAR:
            base_ret = read_op(base_cursor, NEXT, NULL);
            testutil_assert(base_ret == 0 || base_ret == WT_NOTFOUND);
            if (base_ret == 0)
                testutil_check(base_cursor->get_key(base_cursor, &base_keyno));
            break;
        case ROW:
            base_ret = table_mirror_row_next(base, base_cursor, &base_key, &base_keyno);
            break;
        }

        switch (table->type) {
        case FIX:
            /*
             * RS and VLCS skip over removed entries, FLCS returns a value of 0. Skip to the next
             * matching key number, asserting intermediate records have values of 0.
             */
            for (;;) {
                table_ret = read_op(table_cursor, NEXT, NULL);
                testutil_assert(table_ret == 0 || table_ret == WT_NOTFOUND);
                if (table_ret != 0)
                    break;
                testutil_check(table_cursor->get_key(table_cursor, &table_keyno));
                if (table_keyno >= base_keyno)
                    break;
                testutil_check(table_cursor->get_value(table_cursor, &table_bitv));
                testutil_assert(table_bitv == 0);
            }
            break;
        case VAR:
            table_ret = read_op(table_cursor, NEXT, NULL);
            testutil_assert(table_ret == 0 || table_ret == WT_NOTFOUND);
            if (table_ret == 0)
                testutil_check(table_cursor->get_key(table_cursor, &table_keyno));
            break;
        case ROW:
            table_ret = table_mirror_row_next(table, table_cursor, &table_key, &table_keyno);
            break;
        }

        /*
         * Tables run out of keys at different times as RS inserts between the initial table rows
         * and VLCS/FLCS inserts after the initial table rows. There's not much to say about the
         * relationships between them (especially as we skip deleted rows in RS and VLCS, so our
         * last successful check can be before the end of the original rows). If we run out of keys,
         * we're done. If both keys are past the end of the original keys, we're done. There are
         * some potential problems we're not going to catch at the end of the original rows, but
         * those problems should also appear in the middle of the tree.
         *
         * If we have two key/value pairs from the original rows, assert the keys have the same key
         * number (the keys themselves won't match), and keys are larger than or equal to the
         * counter. If the counter is smaller than the keys, that means rows were deleted, which is
         * expected.
         */
        if (base_ret == WT_NOTFOUND || table_ret == WT_NOTFOUND)
            break;
        if (base_keyno > TV(RUNS_ROWS) && table_keyno > TV(RUNS_ROWS))
            break;
        testutil_assert(base_keyno == table_keyno);
        testutil_assert(rows <= base_keyno);
        rows = base_keyno;

        testutil_check(base_cursor->get_value(base_cursor, &base_value));
        if (table->type == FIX) {
            val_to_flcs(table, &base_value, &base_bitv);
            testutil_check(table_cursor->get_value(table_cursor, &table_bitv));
            testutil_assert(base_bitv == table_bitv);
        } else {
            testutil_check(table_cursor->get_value(table_cursor, &table_value));
#if 0
            testutil_assert(base_value.size == table_value.size &&
              (base_value.size == 0 ||
                memcmp(base_value.data, table_value.data, base_value.size) == 0));
#else
            if (base_value.size != table_value.size) {
                fail_size_count++;
                trace_msg(session,
                  "VERIFY_MIRROR: fail_size %" PRIu64 " Row %" PRIu64 " of %" PRIu32 ":",
                  fail_size_count, rows, TV(RUNS_ROWS));
                trace_msg(session, "VERIFY_MIRROR: key %" PRIu64 " base size %d table size %d",
                  base_keyno, (int)base_value.size, (int)table_value.size);
                trace_msg(session, "VERIFY_MIRROR: key %" PRIu64 " base: %.*s", base_keyno,
                  (int)base_value.size, base_value.data);
                trace_msg(session, "VERIFY_MIRROR: key %" PRIu64 " table: %.*s", table_keyno,
                  (int)table_value.size, table_value.data);
            } else if (memcmp(base_value.data, table_value.data, base_value.size) != 0) {
                fail_cmp_count++;
                trace_msg(session,
                  "VERIFY_MIRROR: key %" PRIu64 " fail_cmp %" PRIu64 " Row %" PRIu64 " of %" PRIu32,
                  base_keyno, fail_cmp_count, rows, TV(RUNS_ROWS));
                trace_msg(session, "VERIFY_MIRROR: key %" PRIu64 " base: %.*s", base_keyno,
                  (int)base_value.size, base_value.data);
                trace_msg(session, "VERIFY_MIRROR: key %" PRIu64 " table: %.*s", table_keyno,
                  (int)table_value.size, table_value.data);
            }
#endif
        }

        /* Report progress (unless verifying checkpoints which happens during live operations). */
        if (checkpoint == NULL && ((rows < 5000 && rows % 10 == 0) || rows % 5000 == 0))
            track(buf, rows);
    }
    if (fail_size_count != 0 || fail_cmp_count != 0)
        trace_msg(session,
          "VERIFY_MIRROR FAILURE: fail_cmp %" PRIu64 " fail_size %" PRIu64 " Rows %" PRIu32,
          fail_cmp_count, fail_size_count, TV(RUNS_ROWS));
    testutil_assert(fail_size_count == 0);
    testutil_assert(fail_cmp_count == 0);

    trace_msg(session, "%s: stop", buf);
    wiredtiger_close_session(session);
}

/*
 * wts_verify --
 *     Verify the database tables.
 */
void
wts_verify(WT_CONNECTION *conn, bool mirror_check)
{
    u_int i;

    if (GV(OPS_VERIFY) == 0)
        return;

    /* Individual object verification. */
    tables_apply(table_verify, conn);

    /*
     * Optionally compare any mirrored objects. If this is a reopen, check and see if salvage was
     * tested on the database. In that case, we can't do mirror verification because salvage will
     * have modified some rows leading to failure.
     */
    if (!mirror_check || g.base_mirror == NULL)
        return;

    if (g.reopen && GV(OPS_SALVAGE)) {
        WARN("%s", "skipping mirror verify on reopen because salvage testing was done");
        return;
    }

    for (i = 1; i <= ntables; ++i)
        if (tables[i]->mirror && tables[i] != g.base_mirror)
            table_verify_mirror(conn, g.base_mirror, tables[i], NULL);
}

/*
 * wts_verify_checkpoint --
 *     Verify the database tables at a checkpoint.
 */
void
wts_verify_checkpoint(WT_CONNECTION *conn, const char *checkpoint)
{
    u_int i;

    /* XXX checkpoint cursors don't work. Don't use for now. */
    return;

    if (GV(OPS_VERIFY) == 0)
        return;

    if (g.base_mirror == NULL)
        return;

    for (i = 1; i <= ntables; ++i)
        if (tables[i]->mirror && tables[i] != g.base_mirror)
            table_verify_mirror(conn, g.base_mirror, tables[i], checkpoint);
}
