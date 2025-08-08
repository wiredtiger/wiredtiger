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

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

/*
 * In theory, extensions should not call into WT functions willy-nilly, but the swap functions are
 * inlined. We could call the system's swap functions directly, and/or write our own, but we'd
 * duplicate some existing logic.
 *
 * Possibly some functions like swap should live in a more general library than WT.
 */
#include <gcc.h>
#include <swap.h>

#include "palite_kv.h"
#include "palite_trace.h"

/* In these macros, c_or_e can be a context or environment. */
#define PALITE_SQ_ERR(c_or_e, r)                                                            \
    {                                                                                            \
        ret = (r);                                                                               \
        if (ret == SQLITE_ROW)                                        \
            ret = 0;                                                   \
        if (ret != 0) {                                                                          \
            ret =                                                                                \
              palite_sq_err(&c_or_e->error, ret, "%s: %d: \"%s\": failed", __FILE__, __LINE__, #r); \
            goto err;                                                                            \
        }                                                                                        \
    }

/* In this version, DONE is treated exactly as OK. */
#define PALITE_SQ_ERR_DONE(c_or_e, r)                                   \
    {                                                                   \
        ret = (r);                                                      \
        if (ret == SQLITE_ROW || ret == SQLITE_DONE)                                          \
            ret = 0;                                                    \
        if (ret != 0) {                                                 \
            ret =                                                       \
              palite_sq_err(&c_or_e->error, ret, "%s: %d: \"%s\": failed", __FILE__, __LINE__, #r); \
            goto err;                                                   \
        }                                                               \
    }

#define PALITE_SQ_ERR_MSG(c_or_e, r, msg)                                   \
    {                                                                   \
        ret = (r);                                                      \
        if (ret == SQLITE_ROW)                    \
            ret = 0;                                                   \
        if (ret != 0) {                                                 \
            ret =                                                       \
              palite_sq_err(&c_or_e->error, ret, "%s: %d: %s", __FILE__, __LINE__, msg); \
            goto err;                                                   \
        }                                                               \
    }

#define PALITE_SINGLE_SQLITE
#ifdef PALITE_SINGLE_SQLITE
/* TODO: should work with a lock, and allow more sophistication for multiple sql instances in different dirs, etc. */
static sqlite3 *shared_db = NULL;
static int shared_db_refcnt = 0;
#endif

static const char *SQL_BEGIN = "BEGIN";
static const char *SQL_COMMIT = "COMMIT";
static const char *SQL_ROLLBACK = "ROLLBACK";
static const char *SQL_PUT_CHECKPOINT = "INSERT INTO checkpoints (lsn, timestamp, checkpoint_metadata) VALUES (?, ?, ?);";
static const char *SQL_GET_CHECKPOINT = "SELECT lsn, timestamp, checkpoint_metadata FROM checkpoints ORDER BY lsn DESC;";
static const char *SQL_PUT_GLOBAL = "INSERT OR REPLACE INTO globals (key, val) VALUES (?, ?);";
static const char *SQL_GET_GLOBAL = "SELECT val FROM globals WHERE key = ?;";
static const char *SQL_PUT_PAGE =
  "INSERT INTO pages (table_id, page_id, lsn, is_delta, backlink_lsn, base_lsn,"
  " flags, encryption, timestamp_materialized_us, page_data)"
  " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
static const char *SQL_GET_PAGE = "SELECT lsn, is_delta, backlink_lsn, base_lsn,"
  " flags, encryption, page_data"
  " FROM pages WHERE table_id = ? AND page_id = ?"
  " AND lsn <= ? AND timestamp_materialized_us <= ? ORDER BY lsn DESC;";

static int
palite_sq_err(PALITE_KV_ERROR *error, int rc, const char *fmt, const char *file, int linenum, const char *msg)
{
    /*
     * SQLITE_DONE is similar to WT_NOTFOUND.  The former means no more rows to be returned after this one.
     */
    if (rc != SQLITE_OK && rc != SQLITE_DONE) {
        if (error->sqlite3_rc == 0)
            error->sqlite3_rc = rc;
        if (error->msg == NULL) {
            error->msg = malloc(100);
            snprintf(error->msg, 100, fmt, file, linenum, msg);
        }
        if (error->filename == NULL) {
            error->filename = file;
            error->linenum = linenum;
        }
    }
    return (rc);
}

static uint64_t
palite_kv_timestamp_us(void)
{
    struct timeval v;
    int ret;

    ret = gettimeofday(&v, NULL);
    assert(ret == 0);
    (void)ret; /* Assure that ret is "used" when assertions are not in effect. */

    return (uint64_t)(v.tv_sec * WT_MILLION + v.tv_usec);
}

int
palite_kv_env_create(PALITE_KV_ENV **envp, uint32_t cache_size_mb)
{
    PALITE_KV_ENV *env;

    (void)cache_size_mb;        /* TODO: is there a use for this with SQLite? */
    env = (PALITE_KV_ENV *)calloc(1, sizeof(PALITE_KV_ENV));
    if (env == 0)
        return (ENOMEM);
    if (sem_init(&env->sem, 0, PALITE_STMT_GROUP_MAX))
        return (errno);         /* TODO log error */
    if (pthread_spin_init(&env->lock, PTHREAD_PROCESS_PRIVATE) != 0)
        return (errno);
    *envp = env;
    return (0);
}

static int
palite_kv_exec(PALITE_KV_ENV *env, const char *s)
{
    int ret;
    char *msg;

    ret = 0;
    msg = NULL;
    PALITE_SQ_ERR_MSG(env, t_sqlite3_exec(env, env->db, s, NULL, 0, &msg), msg);
err:
    return (ret);
}

int
palite_kv_env_open(PALITE_KV_ENV *env, const char *homedir)
{
    PALITE_STMT_GROUP *stmt_group;
    int ret;
    char *sqlite_filename;
    size_t len;
    int i;

    /*
     * All these statements are written so that they can be executed multiple times,
     * yet only the first time they are run actually creates tables or inserts values.
     */
    static const char *creation_statements[] = {
        "CREATE TABLE IF NOT EXISTS pages ("
        "    table_id INTEGER NOT NULL,"
        "    page_id INTEGER NOT NULL,"
        "    lsn INTEGER NOT NULL,"
        "    is_delta INTEGER NOT NULL,"
        "    backlink_lsn INTEGER NOT NULL,"
        "    base_lsn INTEGER NOT NULL,"
        "    flags INTEGER NOT NULL,"
        "    encryption STRING NOT NULL,"
        "    timestamp_materialized_us INTEGER NOT NULL,"
        "    page_data BLOB,"
        "    PRIMARY KEY (table_id, page_id, lsn)"
        ");",
        "CREATE TABLE IF NOT EXISTS globals ("
        "    key INTEGER NOT NULL,"
        "    val INTEGER NOT NULL,"
        "    PRIMARY KEY (key)"
        ");",
        "CREATE TABLE IF NOT EXISTS checkpoints ("
        "    lsn INTEGER NOT NULL,"
        "    timestamp INTEGER NOT NULL,"
        "    checkpoint_metadata BLOB,"
        "    PRIMARY KEY (lsn, timestamp)"
        ");",
        /* These keys correspond to the PALITE_KV_GLOBAL_KEY enumeration. */
        /* Key 0: LSN, 1 will be used next */
        "INSERT INTO globals(key,val) SELECT 0, 1 WHERE NOT EXISTS(SELECT 1 FROM globals WHERE key = 0);",
        /* Key 1: Checkpoint completed */
        "INSERT INTO globals(key,val) SELECT 1, 0 WHERE NOT EXISTS(SELECT 1 FROM globals WHERE key = 1);",
        /* Key 2: Checkpoint started */
        "INSERT INTO globals(key,val) SELECT 2, 0 WHERE NOT EXISTS(SELECT 1 FROM globals WHERE key = 2);",
        NULL
    };

    /*
     * There are some subtle assumptions that an okay return from the SQLite API
     * matches the okay return (that is, zero) that we use throughout the rest of this module.
     */
    assert(SQLITE_OK == 0);

    ret = 0;
    len = strlen(homedir) + 20;
    sqlite_filename = malloc(len);
    snprintf(sqlite_filename, len, "%s/sqlite.database", homedir);

#ifdef PALITE_SINGLE_SQLITE
    if (shared_db_refcnt > 0)
        env->db = shared_db;
    else {
        ret = t_sqlite3_open(env, sqlite_filename, &env->db);
        shared_db = env->db;
    }

    shared_db_refcnt++;
#else
    ret = t_sqlite3_open(context, sqlite_filename, &env->db);
#endif
    free(sqlite_filename);
    if (ret != SQLITE_OK)
        return (ret);

    for (i = 0; creation_statements[i] != NULL; ++i)
        if ((ret = palite_kv_exec(env, creation_statements[i])) != SQLITE_OK)
            return (ret);

    /* Prepare statements to be used later in execution. */
    for (i = 0; i < PALITE_STMT_GROUP_MAX; i++) {
        stmt_group = &env->stmt_groups[i];
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_BEGIN, -1,
            &stmt_group->stmts[STMT_BEGIN].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_COMMIT, -1,
            &stmt_group->stmts[STMT_COMMIT].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_ROLLBACK, -1,
            &stmt_group->stmts[STMT_ROLLBACK].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_PUT_CHECKPOINT, -1,
            &stmt_group->stmts[STMT_PUT_CHECKPOINT].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_GET_CHECKPOINT, -1,
        &stmt_group->stmts[STMT_GET_CHECKPOINT].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_PUT_GLOBAL, -1,
            &stmt_group->stmts[STMT_PUT_GLOBAL].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_GET_GLOBAL, -1,
            &stmt_group->stmts[STMT_GET_GLOBAL].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_PUT_PAGE, -1,
            &stmt_group->stmts[STMT_PUT_PAGE].stmt, NULL));
        PALITE_SQ_ERR(env, t_sqlite3_prepare_v2(env, env->db, SQL_GET_PAGE, -1,
            &stmt_group->stmts[STMT_GET_PAGE].stmt, NULL));
    }

err:
    return (ret);
}

void
palite_kv_env_close(PALITE_KV_ENV *env)
{
    int ret;

    ret = 0;
#ifdef PALITE_SINGLE_SQLITE
    shared_db_refcnt--;
    if (shared_db_refcnt == 0) {
        assert(env->db == shared_db);
        PALITE_SQ_ERR(env, t_sqlite3_close(env, env->db));
        shared_db = NULL;
    }
#else
    PALITE_SQ_ERR(env, t_sqlite3_close(context, env->db));
#endif
    (void)pthread_spin_destroy(&env->lock);
    (void)sem_destroy(&env->sem);
    free(env);
err:
    assert(ret == 0 || ret == SQLITE_BUSY); /* TODO?? */
}

static sqlite3_stmt *
palite_kv_start_using_stmt(PALITE_KV_CONTEXT *context, PALITE_STMT_TYPE type)
{
    PALITE_STMT *pstmt;
    pstmt = &context->stmt_group->stmts[type];
    assert(!pstmt->in_use);
    pstmt->in_use = true;
    return (pstmt->stmt);
}

static sqlite3_stmt *
palite_kv_continue_using_stmt(PALITE_KV_CONTEXT *context, PALITE_STMT_TYPE type)
{
    assert(context->stmt_group->stmts[type].in_use);
    return (context->stmt_group->stmts[type].stmt);
}

static void
palite_kv_stop_using_stmt(PALITE_KV_CONTEXT *context, PALITE_STMT_TYPE type)
{
    int ret;
    PALITE_STMT *pstmt;

    ret = 0;
    pstmt = &context->stmt_group->stmts[type];
    assert(pstmt->in_use);
    pstmt->in_use = false;
    PALITE_SQ_ERR(context, t_sqlite3_reset(context, pstmt->stmt));
err:
    assert(ret == 0);
}

int
palite_kv_begin_transaction(PALITE_KV_CONTEXT *context, PALITE_KV_ENV *env, bool readonly)
{
    sqlite3_stmt *stmt;
    int ret;

    ret = 0;
    context->env = env;
    assert(!context->in_txn);
    assert(context->stmt_group == NULL);

    (void)readonly;

    // TODO: barriers
    sem_wait(&env->sem);
    pthread_spin_lock(&env->lock);
    for (int i=0; i < PALITE_STMT_GROUP_MAX; i++) {
        if (!env->stmt_groups[i].group_in_use) {
            context->stmt_group = &env->stmt_groups[i];
            context->stmt_group->group_in_use = true;
            break;
        }
    }
    assert(context->stmt_group != NULL);
    // TODO: publish
    pthread_spin_unlock(&env->lock);

    stmt = palite_kv_start_using_stmt(context, STMT_BEGIN);
    PALITE_SQ_ERR_DONE(context, t_sqlite3_step(context, stmt));
    context->in_txn = true;

err:
    return (ret);
}

int
palite_kv_commit_transaction(PALITE_KV_CONTEXT *context)
{
    sqlite3_stmt *stmt;
    int i, ret, t_ret;

    assert(context->in_txn);
    assert(context->stmt_group != NULL);

    stmt = palite_kv_start_using_stmt(context, STMT_COMMIT);
    PALITE_SQ_ERR_DONE(context, t_sqlite3_step(context, stmt));

    context->in_txn = false;
    context->stmt_group->group_in_use = false;
    for (i = 0; i < (int)STMT_TYPE_COUNT; ++i) {
        //if (context->stmt_group->stmts[i].in_use) {  //TODO
        if (true) {
            t_ret = t_sqlite3_reset(context, context->stmt_group->stmts[i].stmt);
            if (ret == 0 && t_ret != 0)
                ret = t_ret;
            context->stmt_group->stmts[i].in_use = false;
        }
    }
    // TODO: publish
    sem_post(&context->env->sem);
err:
    return (ret);
}

void
palite_kv_rollback_transaction(PALITE_KV_CONTEXT *context)
{
    sqlite3_stmt *stmt;
    int i, ret, t_ret;

    assert(context->in_txn);
    assert(context->stmt_group != NULL);

    stmt = palite_kv_start_using_stmt(context, STMT_ROLLBACK);
    PALITE_SQ_ERR_DONE(context, t_sqlite3_step(context, stmt));

    context->in_txn = false;
    context->stmt_group->group_in_use = false;
    for (i = 0; i < (int)STMT_TYPE_COUNT; ++i) {
        //if (context->stmt_group->stmts[i].in_use) {  //TODO
        if (true) {
            t_ret = t_sqlite3_reset(context, context->stmt_group->stmts[i].stmt);
            if (ret == 0 && t_ret != 0)
                ret = t_ret;
            context->stmt_group->stmts[i].in_use = false;
        }
    }
    // TODO: publish
    sem_post(&context->env->sem);
err:
    assert(ret == 0);
}

int
palite_kv_put_global(PALITE_KV_CONTEXT *context, PALITE_KV_GLOBAL_KEY key, uint64_t value)
{
    sqlite3_stmt *stmt;
    int ret;

    ret = 0;
    assert(context->in_txn);

    stmt = palite_kv_start_using_stmt(context, STMT_PUT_GLOBAL);
    PALITE_SQ_ERR(context, t_sqlite3_reset(context, stmt)); /* TODO: not needed? */
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 1, (int64_t)key));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 2, (int64_t)value));
    PALITE_SQ_ERR(context, t_sqlite3_step(context, stmt));
err:
    /*
     * We must explicitly stop using the statement because there may be more than
     * one call to set a global in a single transaction.
     */
    palite_kv_stop_using_stmt(context, STMT_PUT_GLOBAL);
    return (ret);
}

int
palite_kv_get_global(PALITE_KV_CONTEXT *context, PALITE_KV_GLOBAL_KEY key, uint64_t *valuep)
{
    sqlite3_stmt *stmt;
    int ret;

    ret = 0;
    assert(context->in_txn);

    stmt = palite_kv_start_using_stmt(context, STMT_GET_GLOBAL);
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 1, (int64_t)key));
    PALITE_SQ_ERR(context, t_sqlite3_step(context, stmt));

    *valuep = (uint64_t)t_sqlite3_column_int64(context, stmt, 0);
err:
    /*
     * We must explicitly stop using the statement because there may be more than
     * one call to set a global in a single transaction.
     */
    palite_kv_stop_using_stmt(context, STMT_GET_GLOBAL);
    return (0);
}

int
palite_kv_put_page(PALITE_KV_CONTEXT *context, uint64_t table_id, uint64_t page_id, uint64_t lsn,
  bool is_delta, uint64_t backlink_lsn, uint64_t base_lsn, const WT_PAGE_LOG_ENCRYPTION *encryption,
  uint32_t flags, const WT_ITEM *buf)
{
    sqlite3_stmt *stmt;
    int ret;
    uint64_t materialized;

    ret = 0;
    assert(context->in_txn);
    stmt = palite_kv_start_using_stmt(context, STMT_PUT_PAGE);

    if (context->materialization_delay_us > 0)
        materialized = palite_kv_timestamp_us() + context->materialization_delay_us;
    else
        materialized = 0;

    PALITE_SQ_ERR(context, t_sqlite3_reset(context, stmt)); /* TODO: not needed? */
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 1, (int64_t)table_id));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 2, (int64_t)page_id));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 3, (int64_t)lsn));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 4, (int64_t)is_delta));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 5, (int64_t)backlink_lsn));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 6, (int64_t)base_lsn));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 7, (int64_t)flags));
    PALITE_SQ_ERR(context, t_sqlite3_bind_text(context, stmt, 8, encryption->dek, strlen(encryption->dek), SQLITE_TRANSIENT));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 9, (int64_t)materialized));
    PALITE_SQ_ERR(context, t_sqlite3_bind_blob64(context, stmt, 10, buf->data, buf->size, SQLITE_TRANSIENT));

    PALITE_SQ_ERR(context, t_sqlite3_step(context, stmt));
err:
    return (ret);
}

int
palite_kv_get_page_matches(PALITE_KV_CONTEXT *context, uint64_t table_id, uint64_t page_id,
  uint64_t lsn, PALITE_KV_PAGE_MATCHES *matches)
{
    sqlite3_stmt *stmt;
    uint64_t now;
    int ret;

    ret = 0;
    assert(context->in_txn);

    now = palite_kv_timestamp_us();
    memset(matches, 0, sizeof(*matches));
    matches->context = context;

    stmt = palite_kv_start_using_stmt(context, STMT_GET_PAGE);
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 1, (int64_t)table_id));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 2, (int64_t)page_id));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 3, (int64_t)lsn));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 4, (int64_t)now));

err:
    matches->error = ret;
    return (ret);
}

bool
palite_kv_next_page_match(PALITE_KV_PAGE_MATCHES *matches)
{
    PALITE_KV_CONTEXT *context;
    sqlite3_stmt *stmt;
    int ret;
    bool result;
    const char *encryption;

    ret = 0;
    if (matches->done)
        return (false);

    context = matches->context;
    assert(context->in_txn);
    stmt = palite_kv_continue_using_stmt(context, STMT_GET_PAGE);
    result = true;

    /*
     * The statement is set up to give us the next result. Step to get the next row now.
     * The step function always returns SQLITE_ROW when there is data. If we don't see that,
     * we're done, either because there are no more rows, or that an error has occurred.
     */
    ret = t_sqlite3_step(context, stmt);
    if (ret != SQLITE_ROW) {
        result = false;
        if (ret == SQLITE_DONE) {
            matches->done = true;
            ret = SQLITE_OK;
            result = false;
            goto err;
        }
        matches->error = ret;
        PALITE_SQ_ERR(context, ret);
    }
    matches->lsn = (uint64_t)t_sqlite3_column_int64(context, stmt, 0);
    matches->is_delta = t_sqlite3_column_int64(context, stmt, 1);
    matches->backlink_lsn = (uint64_t)t_sqlite3_column_int64(context, stmt, 2);
    matches->base_lsn = (uint64_t)t_sqlite3_column_int64(context, stmt, 3);
    matches->flags = (uint64_t)t_sqlite3_column_int64(context, stmt, 4);
    encryption = (const char *)t_sqlite3_column_text(context, stmt, 5);
    matches->data = t_sqlite3_column_blob(context, stmt, 6);
    matches->size = (size_t)t_sqlite3_column_bytes(context, stmt, 6);
    assert(matches->data != NULL);

    strncpy(&matches->encryption.dek[0], encryption, sizeof(matches->encryption.dek));

    /*
     * Stop when we get a full page.
     * The query is to get all the deltas for the table_id/page_id that are <= the requested LSN,
     * and the results are sorted from highest LSN to lowest.  The intention is to get all deltas,
     * followed by the full page, and stop so we don't get anything older than the full page.
     * Anything older would be for an older checkpoint.
     * TODO: need to verify in a debugger that this is indeed happening.
     */
    if (matches->is_delta == 0)
        matches->done = true;

err:
    return (result);
}

int
palite_kv_put_checkpoint(PALITE_KV_CONTEXT *context, uint64_t checkpoint_lsn,
  uint64_t checkpoint_timestamp, const WT_ITEM *checkpoint_metadata)
{
    sqlite3_stmt *stmt;
    int ret;

    ret = 0;
    stmt = palite_kv_start_using_stmt(context, STMT_PUT_CHECKPOINT);
    PALITE_SQ_ERR(context, t_sqlite3_reset(context, stmt));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 1, (int64_t)checkpoint_lsn));
    PALITE_SQ_ERR(context, t_sqlite3_bind_int64(context, stmt, 2, (int64_t)checkpoint_timestamp));
    PALITE_SQ_ERR(context, t_sqlite3_bind_blob64(context, stmt, 3, checkpoint_metadata->data,
        checkpoint_metadata->size, SQLITE_TRANSIENT));
    PALITE_SQ_ERR(context, t_sqlite3_step(context, stmt));

err:
    return (ret);
}

int
palite_kv_get_last_checkpoint(PALITE_KV_CONTEXT *context, uint64_t *checkpoint_lsn,
  uint64_t *checkpoint_timestamp, const void **checkpoint_metadata, size_t *checkpoint_metadata_size)
{
    sqlite3_stmt *stmt;
    int ret;

    ret = 0;
    stmt = palite_kv_start_using_stmt(context, STMT_GET_CHECKPOINT);
    PALITE_SQ_ERR(context, t_sqlite3_reset(context, stmt)); /* TODO: not needed */
    PALITE_SQ_ERR(context, t_sqlite3_step(context, stmt));
    *checkpoint_lsn = (uint64_t)t_sqlite3_column_int64(context, stmt, 0);
    *checkpoint_timestamp = (uint64_t)t_sqlite3_column_int64(context, stmt, 1);
    *checkpoint_metadata = t_sqlite3_column_blob(context, stmt, 2);
    *checkpoint_metadata_size = (size_t)t_sqlite3_column_bytes(context, stmt, 2);
    assert(*checkpoint_metadata != NULL);

err:
    return (ret);
}
