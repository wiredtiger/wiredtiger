#include "format.h"

#define FNV_PRIME 0x00000100000001b3

struct checksum_table_arg {
    WT_SESSION *session;
    uint64_t *hash;
};

/*
 * fnv1a_init --
 *     Initialize an incremental FNV-1A hash with a seed value.
 */
static void
fnv1a_init(uint64_t *hash)
{
    *hash = 0xcbf29ce484222325;
}

/*
 * fnv1a_add_bytes --
 *     Update an incremental FNV-1A hash using an arbitrary run of bytes.
 */
static uint64_t
fnv1a_add_bytes(uint64_t cur_hash, const uint8_t *data, size_t sz)
{
    for (size_t i = 0; i < sz; i++) {
        cur_hash ^= data[i];
        cur_hash *= FNV_PRIME;
    }

    return (cur_hash);
}

/*
 * fnv1a_add_u32 --
 *     Update an incremental FNV-1A hash using the four bytes of a u32.
 */
static uint64_t
fnv1a_add_u32(uint64_t cur_hash, uint32_t data)
{
    for (int i = 0; i < 4; i++) {
        cur_hash ^= data & 0xff;
        cur_hash *= FNV_PRIME;

        data >>= 8;
    }

    return (cur_hash);
}

/*
 * checksum_key --
 *     Update an incremental checksum with the key part of a key/value pair.
 */
static void
checksum_key(uint64_t *hash, TABLE *table, WT_ITEM *key)
{
    uint32_t keyno = atou32("checksum-key", (char *)key->data + NTV(table, BTREE_PREFIX_LEN), '.');
    *hash = fnv1a_add_u32(*hash, keyno);
}

/*
 * checksum_value --
 *     Update an incremental checksum with the value part of a key/value pair.
 */
static void
checksum_value(uint64_t *hash, WT_ITEM *value)
{
    *hash = fnv1a_add_bytes(*hash, value->data, value->size);
}

/*
 * checksum_table --
 *     Update an incremental checksum with the contents of a table.
 */
static void
checksum_table(TABLE *t, void *arg)
{
    struct checksum_table_arg *args = arg;

    uint64_t *hash = args->hash;
    WT_SESSION *session = args->session;
    const char *uri = t->uri;

    wt_wrap_begin_transaction(session, NULL);
    testutil_check(
      session->timestamp_transaction_uint(session, WT_TS_TXN_TYPE_READ, g.stable_timestamp));

    WT_CURSOR *cursor;
    wt_wrap_open_cursor(session, uri, NULL, &cursor);

    int ret = 0;
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ITEM key;
        testutil_check(cursor->get_key(cursor, &key));
        checksum_key(hash, t, &key);

        WT_ITEM value;
        testutil_check(cursor->get_value(cursor, &value));
        checksum_value(hash, &value);
    }
    if (ret == WT_NOTFOUND)
        ret = 0;
    testutil_check(ret);

    testutil_check(cursor->close(cursor));
    testutil_check(session->rollback_transaction(session, NULL));
}

/*
 * checksum_database --
 *     Calculate and report a checksum over every table we know about. Eventually we should compare
 *     this checksum against one reported on the follower node, but that's future work.
 */
void
checksum_database(void)
{
    uint64_t hash;
    fnv1a_init(&hash);

    WT_CONNECTION *conn = g.wts_conn;
    WT_SESSION *session;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    struct checksum_table_arg arg = {
      .session = session,
      .hash = &hash,
    };

    tables_apply(checksum_table, &arg);

    testutil_check(session->close(session, NULL));

    trace_msg(session, "Hashed entire DB, checksum is %lu\n", hash);
}
