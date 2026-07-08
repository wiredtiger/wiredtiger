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

#include "schema_disagg_leader_abort.h"

/* Last schema operation on one URI slot within the epoch cutoff. */
typedef struct {
    uint64_t epoch;
    uint64_t commit_ts; /* data commit timestamp; 0 for DROP */
    bool is_create;
    bool valid;
} SLOT_STATE;

/*
 * parse_schema_records --
 *     Scan one thread's schema record file up to cutoff, filling the per-slot state array with the
 *     last operation seen per URI slot. Only records whose URI belongs to thread t are kept.
 */
static void
parse_schema_records(const char *fname, uint32_t t, uint64_t cutoff,
  SLOT_STATE states[MAX_POOL_SIZE], uint32_t pool_size)
{
    FILE *fp;
    uint64_t commit_ts, entry_epoch;
    uint32_t s, t2;
    char op[16], rec_uri[128];

    for (s = 0; s < pool_size; s++) {
        states[s].epoch = 0;
        states[s].commit_ts = 0;
        states[s].is_create = false;
        states[s].valid = false;
    }

    if ((fp = fopen(fname, "r")) == NULL)
        return;

    while (fscanf(fp, "%15s %" SCNu64 " %" SCNu64 " %127s", op, &entry_epoch, &commit_ts,
             rec_uri) == 4) {
        if (entry_epoch > cutoff)
            continue;
        if (sscanf(rec_uri, "table:schema_%u_%u", &t2, &s) != 2 || t2 != t || s >= pool_size)
            continue;
        if (entry_epoch > states[s].epoch) {
            states[s].epoch = entry_epoch;
            states[s].commit_ts = commit_ts;
            states[s].is_create = (strcmp(op, "CREATE") == 0);
            states[s].valid = true;
        }
    }
    (void)fclose(fp);
}

/*
 * check_schema_presence --
 *     For each slot with a valid record, assert that tables created before the cutoff exist and
 *     tables dropped before the cutoff are absent.
 */
static void
check_schema_presence(WT_SESSION *session, uint32_t t, const SLOT_STATE states[MAX_POOL_SIZE],
  uint32_t pool_size, bool *fatal)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    uint32_t s;
    char uri[64];

    for (s = 0; s < pool_size; s++) {
        if (!states[s].valid)
            continue;

        testutil_snprintf(uri, sizeof(uri), SCHEMA_TABLE_FMT, t, s);
        ret = session->open_cursor(session, uri, NULL, NULL, &cursor);

        if (states[s].is_create) {
            if (ret == WT_NOTFOUND || ret == ENOENT) {
                printf("SCHEMA FAIL: %s missing after recovery (CREATE at epoch %" PRIu64 ")\n",
                  uri, states[s].epoch);
                *fatal = true;
            } else if (ret != 0) {
                printf("SCHEMA FAIL: error opening %s: %s\n", uri, wiredtiger_strerror(ret));
                *fatal = true;
            } else
                testutil_check(cursor->close(cursor));
        } else {
            if (ret == 0)
                testutil_check(cursor->close(cursor));
            else if (ret != WT_NOTFOUND && ret != ENOENT) {
                printf("SCHEMA FAIL: error checking %s: %s\n", uri, wiredtiger_strerror(ret));
                *fatal = true;
            }
        }
    }
}

/*
 * check_data_rows --
 *     For each slot whose last checkpointed operation was a CREATE and whose data commit timestamp
 *     is at or below the last checkpoint timestamp, confirm all DATA_NROWS rows are present. Slots
 *     whose data commit timestamp exceeds last_ckpt_ts are skipped: the data was committed after
 *     the last checkpoint and is not durable.
 */
static void
check_data_rows(WT_SESSION *session, uint32_t t, const SLOT_STATE states[MAX_POOL_SIZE],
  uint32_t pool_size, uint64_t last_ckpt_ts, bool *fatal)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    uint32_t r, s;
    char expected_val[32], key_buf[16], uri[64];
    const char *actual_val;

    for (s = 0; s < pool_size; s++) {
        if (!states[s].valid || !states[s].is_create)
            continue;
        if (last_ckpt_ts > 0 && states[s].commit_ts > last_ckpt_ts)
            continue;

        testutil_snprintf(uri, sizeof(uri), SCHEMA_TABLE_FMT, t, s);
        ret = session->open_cursor(session, uri, NULL, NULL, &cursor);
        if (ret != 0)
            continue; /* Cursor open failed - schema check already caught this. */

        testutil_snprintf(expected_val, sizeof(expected_val), "%" PRIu64, states[s].epoch);
        for (r = 0; r < DATA_NROWS; r++) {
            testutil_snprintf(key_buf, sizeof(key_buf), "%" PRIu32, r);
            cursor->set_key(cursor, key_buf);
            ret = cursor->search(cursor);
            if (ret == 0) {
                testutil_check(cursor->get_value(cursor, &actual_val));
                if (strcmp(actual_val, expected_val) != 0) {
                    printf("DATA FAIL: %s key %s: got %s want %s\n", uri, key_buf, actual_val,
                      expected_val);
                    *fatal = true;
                }
            } else if (ret != WT_NOTFOUND) {
                printf("DATA FAIL: error reading %s key %s: %s\n", uri, key_buf,
                  wiredtiger_strerror(ret));
                *fatal = true;
            } else {
                printf("DATA FAIL: %s missing key %s (epoch %" PRIu64 ")\n", uri, key_buf,
                  states[s].epoch);
                *fatal = true;
            }
        }
        testutil_check(cursor->close(cursor));
    }
}

/*
 * verify_schema_state --
 *     Verify schema and data state after recovery.
 *
 * Reads per-thread schema record files, uses last_disaggregated_schema_epoch as the epoch cutoff,
 *     and asserts that every table whose final pre-cutoff operation was a CREATE exists and
 *     contains correct data rows. Returns true if a fatal error is found.
 */
bool
verify_schema_state(WT_CONNECTION *conn, TEST_CONFIG *cfg)
{
    SLOT_STATE states[MAX_POOL_SIZE];
    WT_SESSION *session;
    uint64_t cutoff, last_ckpt_ts;
    bool fatal;
    char fname[128], ts_buf[64];
    uint32_t t;

    fatal = false;
    cutoff = 0;
    testutil_check(conn->query_timestamp(conn, ts_buf, "get=last_disaggregated_schema_epoch"));
    (void)sscanf(ts_buf, "%" SCNx64, &cutoff);
    printf("Schema verify: last_disaggregated_schema_epoch = %" PRIu64 "\n", cutoff);
    if (cutoff == 0) {
        printf("Schema verify: no schema epoch checkpointed, skipping.\n");
        return (false);
    }

    last_ckpt_ts = 0;
    (void)conn->query_timestamp(conn, ts_buf, "get=last_checkpoint");
    (void)sscanf(ts_buf, "%" SCNx64, &last_ckpt_ts);
    printf("Schema verify: last_checkpoint_timestamp = %" PRIu64 "\n", last_ckpt_ts);

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    for (t = 0; t < cfg->nth; t++) {
        testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, t);
        parse_schema_records(fname, t, cutoff, states, cfg->pool_size);
        check_schema_presence(session, t, states, cfg->pool_size, &fatal);
        check_data_rows(session, t, states, cfg->pool_size, last_ckpt_ts, &fatal);
    }

    testutil_check(session->close(session, NULL));
    return (fatal);
}
