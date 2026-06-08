/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/* Field-level descriptions for the btree usage stats, generated from the single source list. */
#define WT_BTREE_USAGE_DESC_ENTRY(name, desc) desc,
static const char *const __curstat_usage_field_desc[] = {
  WT_BTREE_USAGE_STATS_LIST(WT_BTREE_USAGE_DESC_ENTRY)};
#undef WT_BTREE_USAGE_DESC_ENTRY

#define WT_BTREE_USAGE_DESC_BUF_SIZE 320

/*
 * __usage_sanitize --
 *     Reduce a btree URI to a single key token: drop the "file:"/"table:" scheme and turn every
 *     character that is not an ASCII letter, digit, or underscore into '_', so the result has no
 *     space, dot, or colon. MongoDB groups WT stats by the token before the first ": " when it
 *     reports them, treating those characters as nesting separators, so a clean token keeps each
 *     btree in one subsection.
 */
static void
__usage_sanitize(const char *uri, char *buf, size_t len)
{
    size_t i;
    char c;
    const char *p;

    p = strchr(uri, ':');
    p = (p != NULL) ? p + 1 : uri;
    for (i = 0; *p != '\0' && i + 1 < len; ++p) {
        c = *p;
        buf[i++] =
          ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) ? c : '_';
    }
    buf[i] = '\0';
}

/*
 * __curstat_usage_conn_desc --
 *     Description callback for the connection statistics cursor. Normal connection stats defer to
 *     the generated description; the appended btree-usage entries are a rank leaderboard
 *     ("usage_rank_01" .. "_16") followed by per-btree detail keyed by identity, whose slot prefix
 *     ("usage_(id=N)_<uri>" / "usage_hs" / "usage_sample") was assembled at init and stashed per
 *     slot.
 */
static int
__curstat_usage_conn_desc(WT_CURSOR_STAT *cst, int offset, const char **descp)
{
    WT_SESSION_IMPL *session;
    int conn_count, field, rel;
    char rank_prefix[24];
    const char *fdesc, *prefix;

    conn_count = (int)(sizeof(WT_CONNECTION_STATS) / sizeof(int64_t));
    if (offset < conn_count)
        return (__wt_stat_connection_desc(cst, offset, descp));

    session = (WT_SESSION_IMPL *)cst->iface.session;
    rel = offset - conn_count;

    if (rel < WT_BTREE_USAGE_SUMMARY_COUNT) {
        /* Connection-level summary: schema version and the count of active btrees. */
        if (rel == WT_BTREE_USAGE_SUMMARY_VERSION) {
            prefix = "usage_version";
            fdesc = "number";
        } else {
            prefix = "usage_active_btrees";
            fdesc = "count";
        }
    } else if ((rel -= WT_BTREE_USAGE_SUMMARY_COUNT) < WT_BTREE_USAGE_LEADERBOARD_COUNT) {
        /* Rank slot, 1-based for display; the top-N is sorted by score at publish. */
        WT_RET(__wt_snprintf(rank_prefix, sizeof(rank_prefix), "usage_rank_%02d",
          rel / WT_BTREE_USAGE_RANK_FIELDS + 1));
        prefix = rank_prefix;
        fdesc = (rel % WT_BTREE_USAGE_RANK_FIELDS == WT_BTREE_USAGE_RANK_BTREE_ID) ? "btree id" :
                                                                                     "access total";
    } else {
        /* Per-btree detail: prefix is the per-slot identity token built in conn_init. */
        rel -= WT_BTREE_USAGE_LEADERBOARD_COUNT;
        prefix =
          cst->usage_uris + (size_t)(rel / WT_BTREE_USAGE_DETAIL_FIELDS) * WT_BTREE_USAGE_URI_MAX;
        field = rel % WT_BTREE_USAGE_DETAIL_FIELDS;
        fdesc = (field == WT_BTREE_USAGE_DETAIL_STREAK) ?
          "consecutive intervals in top set" :
          ((field == WT_BTREE_USAGE_DETAIL_TYPE) ?
              "btree type (1 fixed-length column, 2 variable-length column, 3 row)" :
              __curstat_usage_field_desc[field]);
    }

    if (cst->desc_buf == NULL)
        WT_RET(__wt_calloc_def(session, WT_BTREE_USAGE_DESC_BUF_SIZE, &cst->desc_buf));
    WT_RET(__wt_snprintf(cst->desc_buf, WT_BTREE_USAGE_DESC_BUF_SIZE, "%s: %s", prefix, fdesc));
    *descp = cst->desc_buf;
    return (0);
}

/*
 * __wti_curstat_usage_conn_init --
 *     Append the per-btree usage snapshot to the connection statistics cursor as virtual entries:
 *     the aggregated connection stats, then a rank leaderboard (btree id + access total per rank),
 *     then each slot's detail block. The snapshot is a connection-level value maintained by the
 *     sweep, so it is read here (not aggregated across session buckets). Reallocates on re-init.
 */
int
__wti_curstat_usage_conn_init(WT_SESSION_IMPL *session, WT_CURSOR_STAT *cst)
{
    WT_BTREE_USAGE_SNAPSHOT *snap;
    WT_CONNECTION_IMPL *conn;
    int64_t *buf;
    int conn_count, dbase, f, lbase, rbase, s;
    char token[WT_BTREE_USAGE_URI_MAX], *prefix;

    conn = S2C(session);
    conn_count = (int)(sizeof(WT_CONNECTION_STATS) / sizeof(int64_t));

    __wt_free(session, cst->usage_uris);
    WT_RET(__wt_calloc_def(
      session, (size_t)WT_BTREE_USAGE_SLOT_COUNT * WT_BTREE_USAGE_URI_MAX, &cst->usage_uris));
    __wt_free(session, cst->stats_alloc);
    WT_RET(__wt_calloc_def(session, (size_t)conn_count + WT_BTREE_USAGE_VIRTUAL_COUNT, &buf));
    memcpy(buf, &cst->u.conn_stats, sizeof(WT_CONNECTION_STATS));

    lbase = conn_count + WT_BTREE_USAGE_SUMMARY_COUNT; /* leaderboard block */
    dbase = lbase + WT_BTREE_USAGE_LEADERBOARD_COUNT;  /* detail block */

    buf[conn_count + WT_BTREE_USAGE_SUMMARY_VERSION] = WT_BTREE_USAGE_VERSION;
    __wt_readlock(session, &conn->btree_usage_lock);
    buf[conn_count + WT_BTREE_USAGE_SUMMARY_ACTIVE] = conn->btree_usage_active;
    for (s = 0; s < WT_BTREE_USAGE_SLOT_COUNT; ++s) {
        snap = &conn->btree_usage[s];

        /* Detail block for the slot (invalid slots stay zero-filled). */
        if (snap->valid) {
            rbase = dbase + s * WT_BTREE_USAGE_DETAIL_FIELDS;
            for (f = 0; f < WT_BTREE_USAGE_STAT_COUNT; ++f)
                buf[rbase + f] = snap->stats.v[f];
            buf[rbase + WT_BTREE_USAGE_DETAIL_STREAK] = snap->streak;
            buf[rbase + WT_BTREE_USAGE_DETAIL_TYPE] = snap->type;
        }

        /* Per-slot identity prefix for the detail keys. */
        prefix = cst->usage_uris + (size_t)s * WT_BTREE_USAGE_URI_MAX;
        if (s == WT_BTREE_USAGE_SLOT_PIN_HS)
            WT_IGNORE_RET(__wt_snprintf(prefix, WT_BTREE_USAGE_URI_MAX, "usage_hs"));
        else if (s == WT_BTREE_USAGE_SLOT_SAMPLE)
            WT_IGNORE_RET(__wt_snprintf(prefix, WT_BTREE_USAGE_URI_MAX, "usage_sample"));
        else if (snap->valid) {
            __usage_sanitize(snap->uri, token, sizeof(token));
            WT_IGNORE_RET(__wt_snprintf(
              prefix, WT_BTREE_USAGE_URI_MAX, "usage_(id=%" PRIu32 ")_%s", snap->btree_id, token));
        } else
            /* Empty rank slot: a unique, stable placeholder so the key space stays distinct. */
            WT_IGNORE_RET(
              __wt_snprintf(prefix, WT_BTREE_USAGE_URI_MAX, "usage_unused_%02d", s + 1));
    }

    /* The top-N slots are already score-sorted by the sweep, so rank == slot. */
    for (s = 0; s < WT_BTREE_USAGE_TOP_N; ++s) {
        snap = &conn->btree_usage[s];
        if (!snap->valid)
            continue;
        rbase = lbase + s * WT_BTREE_USAGE_RANK_FIELDS;
        buf[rbase + WT_BTREE_USAGE_RANK_BTREE_ID] = snap->btree_id;
        buf[rbase + WT_BTREE_USAGE_RANK_ACCESS_TOTAL] = snap->score;
    }
    __wt_readunlock(session, &conn->btree_usage_lock);

    cst->stats_alloc = buf;
    cst->stats = buf;
    cst->stats_base = WT_CONNECTION_STATS_BASE;
    cst->stats_count = conn_count + WT_BTREE_USAGE_VIRTUAL_COUNT;
    cst->stats_desc = __curstat_usage_conn_desc;
    return (0);
}

/*
 * __wti_curstat_usage_close --
 *     Free the heap state owned by the connection usage virtual stats. Safe to call on any stat
 *     cursor; the fields are NULL unless this cursor exposed the usage snapshot.
 */
void
__wti_curstat_usage_close(WT_SESSION_IMPL *session, WT_CURSOR_STAT *cst)
{
    __wt_free(session, cst->stats_alloc);
    __wt_free(session, cst->usage_uris);
}
