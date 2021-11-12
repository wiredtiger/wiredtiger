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
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_SESSION *session;
    int i;

    conn = (WT_CONNECTION *)arg;

    /*
     * Verify can return EBUSY if the handle isn't available. Don't yield and retry, in the case of
     * LSM, the handle may not be available for a long time.
     */
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    session->app_private = table->track_prefix;
    for (i = 0; ++i <= 5; __wt_sleep(1, 0)) {
        if ((ret = session->verify(session, table->uri, "strict")) == 0)
            break;
        testutil_assert(ret == EBUSY);
    }
    if (ret == EBUSY)
        WARN("table.%d skipped verify because of EBUSY", table->id);
    testutil_check(session->close(session, NULL));
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
table_verify_mirror(WT_CONNECTION *conn, TABLE *base, TABLE *table)
{
    WT_CURSOR *base_cursor, *table_cursor;
    WT_ITEM base_key, base_value, table_key, table_value;
    WT_SESSION *session;
    uint64_t base_keyno, table_keyno, rows;
    uint8_t bitv;
    int base_ret, table_ret;
    char track_buf[128];

    base_keyno = table_keyno = 0; /* -Wconditional-uninitialized */
    base_ret = table_ret = 0;

    testutil_check(
      __wt_snprintf(track_buf, sizeof(track_buf), "table %d mirror verify", table->id));

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    wiredtiger_open_cursor(session, base->uri, NULL, &base_cursor);
    wiredtiger_open_cursor(session, table->uri, NULL, &table_cursor);

    for (rows = 1; rows <= TV(RUNS_ROWS); ++rows) {
        switch (base->type) {
        case FIX:
            testutil_assert(base->type != FIX);
            break;
        case VAR:
            testutil_assert(read_op(base_cursor, NEXT, NULL) == 0);
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
                testutil_assert(read_op(table_cursor, NEXT, NULL) == 0);
                testutil_check(table_cursor->get_key(table_cursor, &table_keyno));
                if (table_keyno >= base_keyno)
                    break;
                testutil_check(table_cursor->get_value(table_cursor, &table_value));
                testutil_assert(*(uint8_t *)table_value.data == 0);
            }
            break;
        case VAR:
            testutil_assert(read_op(table_cursor, NEXT, NULL) == 0);
            testutil_check(table_cursor->get_key(table_cursor, &table_keyno));
            break;
        case ROW:
            table_ret = table_mirror_row_next(table, table_cursor, &table_key, &table_keyno);
            break;
        }

        /*
         * Tables can run out of keys at different times as RS inserts between table rows and
         * VLCS/FLCS insert after the initial table rows. There's not much to say about the
         * relationships between them (especially as we skip rows that are removed, so our last
         * successful check may have been before the end of the original rows).
         */
        if (base_ret == WT_NOTFOUND || table_ret == WT_NOTFOUND)
            break;

        /*
         * Otherwise, assert mirrors are larger than or equal to the counter and have the same key
         * number (the keys themselves won't match). If the counter is smaller than the mirrors key,
         * it means a row was deleted, which is expected.
         */
        testutil_assert(rows <= base_keyno && base_keyno == table_keyno);
        rows = base_keyno;

        testutil_check(base_cursor->get_value(base_cursor, &base_value));
        testutil_check(table_cursor->get_value(table_cursor, &table_value));
        if (table->type == FIX) {
            val_to_flcs(&base_value, &bitv);
            testutil_assert(*(uint8_t *)table_value.data == bitv);
        } else
            testutil_assert(base_value.size == table_value.size &&
              memcmp(base_value.data, table_value.data, base_value.size) == 0);

        /* Report on progress. */
        if ((rows < 5000 && rows % 10 == 0) || rows % 5000 == 0)
            track(track_buf, rows);
    }

    testutil_check(session->close(session, NULL));
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
            table_verify_mirror(conn, g.base_mirror, tables[i]);
}
