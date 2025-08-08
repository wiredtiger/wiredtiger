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

#pragma once

#include "../../../third_party/sqlite3/sqlite3.h"
#include <pthread.h>
#include <semaphore.h>

/*
 * Both PALITE and PALITE_KV need these, there's no other convenient place for them.
 */
#ifndef WT_THOUSAND
#define WT_THOUSAND 1000
#define WT_MILLION 1000000
#endif

/*
 * PALITE flags start at the 16th bit (0x10000u) to avoid conflicts with __wt_page_log_put_args flags.
 */
#define WT_PALITE_KV_TOMBSTONE 0x10000u

/*
 * This include file creates a tiny bit of abstraction for the KV database used, in case we want to
 * ever change to a different implementation.
 *
 * This version uses SQLite, version 3.
 */

typedef struct {
    sqlite3_stmt *stmt;
    bool in_use;
} PALITE_STMT;

typedef enum {
    STMT_BEGIN,
    STMT_COMMIT,
    STMT_ROLLBACK,
    STMT_PUT_CHECKPOINT,
    STMT_GET_CHECKPOINT,
    STMT_PUT_GLOBAL,
    STMT_GET_GLOBAL,
    STMT_PUT_PAGE,
    STMT_GET_PAGE_LSN,
    STMT_GET_PAGE,
    STMT_TYPE_COUNT              /* must be last */
} PALITE_STMT_TYPE;
/*
 * Although SQLite is generally thread safe, the SQLite statement struct is not.
 */
typedef struct PALITE_STMT_GROUP {
    PALITE_STMT stmts[STMT_TYPE_COUNT];
    bool group_in_use;
} PALITE_STMT_GROUP;
//#define PALITE_STMT_GROUP_MAX 3   TODO
#define PALITE_STMT_GROUP_MAX 1

/* On the last error from SQLite, at the point of the call, we save this information. */
typedef struct PALITE_KV_ERROR {
    char *msg;
    int sqlite3_rc;
    const char *filename;
    int linenum;

} PALITE_KV_ERROR;

typedef struct PALITE_KV_ENV {
    sqlite3 *db;
    PALITE_STMT_GROUP stmt_groups[PALITE_STMT_GROUP_MAX];    
    pthread_spinlock_t lock;
    sem_t sem;
    PALITE_KV_ERROR error;
    bool sqlite_trace;
} PALITE_KV_ENV;

typedef struct PALITE_KV_CONTEXT {
    PALITE_KV_ENV *env;
    PALITE_STMT_GROUP *stmt_group;

    bool in_txn;
    bool sqlite_trace;
    PALITE_KV_ERROR error;

    uint64_t last_materialized_lsn;
    uint32_t materialization_delay_us;
} PALITE_KV_CONTEXT;

typedef struct PALITE_KV_PAGE_MATCHES {
    PALITE_KV_CONTEXT *context;

    sqlite3_stmt *sstmt;
    size_t size;
    const void *data;
    int error;
    bool first;
    bool done;

    bool is_delta;
    uint64_t query_lsn;

    uint64_t table_id;
    uint64_t page_id;
    uint64_t lsn;

    uint64_t backlink_lsn;
    uint64_t base_lsn;
    WT_PAGE_LOG_ENCRYPTION encryption;
    uint32_t flags;
} PALITE_KV_PAGE_MATCHES;

int palite_kv_env_create(PALITE_KV_ENV **env, uint32_t cache_size_mb);
int palite_kv_env_open(PALITE_KV_ENV *env, const char *homedir);
void palite_kv_env_close(PALITE_KV_ENV *env);

int palite_kv_begin_transaction(PALITE_KV_CONTEXT *context, PALITE_KV_ENV *env, bool readonly);
int palite_kv_commit_transaction(PALITE_KV_CONTEXT *context);
void palite_kv_rollback_transaction(PALITE_KV_CONTEXT *context);

typedef enum PALITE_KV_GLOBAL_KEY {
    PALITE_KV_GLOBAL_LSN = 0,
} PALITE_KV_GLOBAL_KEY;

int palite_kv_put_global(PALITE_KV_CONTEXT *context, PALITE_KV_GLOBAL_KEY key, uint64_t value);
int palite_kv_get_global(PALITE_KV_CONTEXT *context, PALITE_KV_GLOBAL_KEY key, uint64_t *valuep);
int palite_kv_put_page(PALITE_KV_CONTEXT *context, uint64_t table_id, uint64_t page_id, uint64_t lsn,
  bool is_delta, uint64_t backlink_lsn, uint64_t base_lsn, const WT_PAGE_LOG_ENCRYPTION *encryption,
  uint32_t flags, const WT_ITEM *buf);
int palite_kv_get_page_matches(PALITE_KV_CONTEXT *context, uint64_t table_id, uint64_t page_id,
  uint64_t lsn, PALITE_KV_PAGE_MATCHES *matchesp);
bool palite_kv_next_page_match(PALITE_KV_PAGE_MATCHES *matches);
int palite_kv_put_checkpoint(PALITE_KV_CONTEXT *context, uint64_t checkpoint_lsn,
  uint64_t checkpoint_timestamp, const WT_ITEM *checkpoint_metadata);
int palite_kv_get_last_checkpoint(PALITE_KV_CONTEXT *context, uint64_t *checkpoint_lsn,
  uint64_t *checkpoint_timestamp, const void **checkpoint_metadata, size_t *checkpoint_metadata_size);
