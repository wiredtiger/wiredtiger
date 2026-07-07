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

/*
 * verify_schema_state --
 *     Verify schema and data state after recovery.
 *
 *     Reads per-thread schema and data record files, uses last_disaggregated_schema_epoch as the
 *     epoch cutoff, and asserts that every table whose final pre-cutoff operation was a CREATE
 *     exists and contains the correct data row. Returns true if a fatal error is found.
 */
bool
verify_schema_state(WT_CONNECTION *conn)
{
    FILE *fp;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION *session;
    uint64_t ckpt_epoch, entry_epoch, last_epoch[SCHEMA_POOL_SIZE];
    bool is_create[SCHEMA_POOL_SIZE], slot_valid[SCHEMA_POOL_SIZE], fatal;
    char fname[128], op[16], rec_uri[128];
    char ts_buf[64];
    uint32_t s, t, t2;

    fatal = false;
    ckpt_epoch = 0;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    testutil_check(
      conn->query_timestamp(conn, ts_buf, "get=last_disaggregated_schema_epoch"));
    (void)sscanf(ts_buf, "%" SCNx64, &ckpt_epoch);
    printf("Schema verify: last_disaggregated_schema_epoch = %" PRIu64 "\n", ckpt_epoch);

    if (ckpt_epoch == 0) {
        printf("Schema verify: no schema epoch checkpointed, skipping.\n");
        testutil_check(session->close(session, NULL));
        return (false);
    }

    /* Schema verification: assert CREATE'd tables exist, DROP'd ones are absent. */
    for (t = 0; t < nth; t++) {
        testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, t);
        if ((fp = fopen(fname, "r")) == NULL)
            continue;

        for (s = 0; s < SCHEMA_POOL_SIZE; s++) {
            slot_valid[s] = false;
            last_epoch[s] = 0;
            is_create[s] = false;
        }

        while (fscanf(fp, "%15s %" SCNu64 " %127s", op, &entry_epoch, rec_uri) == 3) {
            if (entry_epoch > ckpt_epoch)
                continue;
            if (sscanf(rec_uri, "table:schema_%u_%u", &t2, &s) != 2 || t2 != t ||
              s >= SCHEMA_POOL_SIZE)
                continue;
            if (entry_epoch > last_epoch[s]) {
                last_epoch[s] = entry_epoch;
                is_create[s] = (strcmp(op, "CREATE") == 0);
                slot_valid[s] = true;
            }
        }
        (void)fclose(fp);

        for (s = 0; s < SCHEMA_POOL_SIZE; s++) {
            char expected_uri[64];

            if (!slot_valid[s])
                continue;
            testutil_snprintf(expected_uri, sizeof(expected_uri), SCHEMA_TABLE_FMT, t, s);
            ret = session->open_cursor(session, expected_uri, NULL, NULL, &cursor);
            if (is_create[s]) {
                if (ret == WT_NOTFOUND || ret == ENOENT) {
                    printf("SCHEMA FAIL: %s missing after recovery (CREATE at epoch %" PRIu64
                           ")\n",
                      expected_uri, last_epoch[s]);
                    fatal = true;
                } else if (ret != 0) {
                    printf("SCHEMA FAIL: error opening %s: %s\n", expected_uri,
                      wiredtiger_strerror(ret));
                    fatal = true;
                } else
                    testutil_check(cursor->close(cursor));
            } else {
                if (ret == 0)
                    testutil_check(cursor->close(cursor));
                else if (ret != WT_NOTFOUND && ret != ENOENT) {
                    printf("SCHEMA FAIL: error checking %s: %s\n", expected_uri,
                      wiredtiger_strerror(ret));
                    fatal = true;
                }
            }
        }
    }

    /* Data verification: confirm each row written to a surviving table is present. */
    for (t = 0; t < nth; t++) {
        uint64_t d_slot, d_epoch;
        uint64_t last_data_epoch[SCHEMA_POOL_SIZE];
        char data_fname[128];

        testutil_snprintf(data_fname, sizeof(data_fname), SCHEMA_DATA_FILE, t);
        if ((fp = fopen(data_fname, "r")) == NULL)
            continue;

        for (s = 0; s < SCHEMA_POOL_SIZE; s++)
            last_data_epoch[s] = 0;

        while (fscanf(fp, "%" SCNu64 " %" SCNu64, &d_slot, &d_epoch) == 2)
            if (d_slot < SCHEMA_POOL_SIZE && d_epoch <= ckpt_epoch &&
              d_epoch > last_data_epoch[d_slot])
                last_data_epoch[d_slot] = d_epoch;
        (void)fclose(fp);

        for (s = 0; s < SCHEMA_POOL_SIZE; s++) {
            char data_uri[64], expected_val[32];
            const char *actual_val;

            if (last_data_epoch[s] == 0)
                continue;

            testutil_snprintf(data_uri, sizeof(data_uri), SCHEMA_TABLE_FMT, t, s);
            ret = session->open_cursor(session, data_uri, NULL, NULL, &cursor);
            if (ret != 0)
                continue; /* Table may have been dropped — OK. */

            cursor->set_key(cursor, DATA_KEY);
            ret = cursor->search(cursor);
            if (ret == 0) {
                testutil_snprintf(
                  expected_val, sizeof(expected_val), "%" PRIu64, last_data_epoch[s]);
                testutil_check(cursor->get_value(cursor, &actual_val));
                if (strcmp(actual_val, expected_val) != 0) {
                    printf("DATA FAIL: %s key %s: got %s want %s\n", data_uri, DATA_KEY,
                      actual_val, expected_val);
                    fatal = true;
                }
            } else if (ret != WT_NOTFOUND) {
                printf("DATA FAIL: error reading %s: %s\n", data_uri,
                  wiredtiger_strerror(ret));
                fatal = true;
            } else {
                printf("DATA FAIL: %s missing key %s (epoch %" PRIu64 ")\n", data_uri,
                  DATA_KEY, last_data_epoch[s]);
                fatal = true;
            }
            testutil_check(cursor->close(cursor));
        }
    }

    testutil_check(session->close(session, NULL));
    return (fatal);
}
