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

#include "../../../third_party/openldap_liblmdb/lmdb.h"

/*
 * This include file creates a tiny bit of abstraction for the KV database used, in case we want to
 * ever change to a different implementation.
 *
 * At the moment we use LMDB, which is very similar to Berkeley DB. LMDB often uses MDB as a prefix.
 */
typedef struct PALM_KV_ENV {
    MDB_env *lmdb_env;
    MDB_txn *lmdb_txn;
    MDB_dbi lmdb_globals_dbi;
    MDB_dbi lmdb_tables_dbi;
    MDB_dbi lmdb_pages_dbi;
} PALM_KV_ENV;

typedef struct PALM_KV_PAGE_MATCHES {
    MDB_cursor *lmdb_cursor;
    size_t size;
    void *data;
    int error;
    bool first;

    uint64_t table_id;
    uint64_t page_id;
    uint64_t checkpoint_id;
} PALM_KV_PAGE_MATCHES;

int palm_kv_env_create(PALM_KV_ENV **env);
int palm_kv_env_open(PALM_KV_ENV *env, const char *homedir);
void palm_kv_env_close(PALM_KV_ENV *env);

int palm_kv_begin_transaction(PALM_KV_ENV *env, bool readonly);
int palm_kv_commit_transaction(PALM_KV_ENV *env);
void palm_kv_rollback_transaction(PALM_KV_ENV *env);

typedef enum PALM_KV_GLOBAL_KEY {
    PALM_KV_GLOBAL_REVISION = 0,
    PALM_KV_GLOBAL_CHECKPOINT_COMPLETED = 1,
    PALM_KV_GLOBAL_CHECKPOINT_STARTED = 2,
} PALM_KV_GLOBAL_KEY;

int palm_kv_put_global(PALM_KV_ENV *env, PALM_KV_GLOBAL_KEY key, uint64_t value);
int palm_kv_get_global(PALM_KV_ENV *env, PALM_KV_GLOBAL_KEY key, uint64_t *valuep);
int palm_kv_put_page(PALM_KV_ENV *env, uint64_t table_id, uint64_t page_id, uint64_t checkpoint_id,
  uint64_t revision, bool is_delta, const WT_ITEM *buf);
int palm_kv_get_page_matches(PALM_KV_ENV *env, uint64_t table_id, uint64_t page_id,
  uint64_t checkpoint_id, PALM_KV_PAGE_MATCHES *matchesp);
bool palm_kv_next_page_match(PALM_KV_PAGE_MATCHES *matches);
