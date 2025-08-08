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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>

#include "palite_kv.h"
#include "palite_trace.h"

#define TRACE_FUNC(c_or_e, name)              \
    if (c_or_e->sqlite_trace)                \
        fprintf(stderr, "==== %s(", #name)
#define TRACE_ARG1(c_or_e, fmt, a)             \
    do {                                      \
        if (c_or_e->sqlite_trace) {          \
        if (strcmp(fmt, "%s") == 0)           \
            fprintf(stderr, #a "=\"" fmt "\"", a);   \
        else                                  \
            fprintf(stderr, #a "=" fmt, a);   \
        }                                     \
    } while(0)
#define TRACE_ARG(c_or_e, fmt, a)                  \
    do {                                          \
        if (c_or_e->sqlite_trace) {              \
        fprintf(stderr, ", ");                    \
        TRACE_ARG1(c_or_e, fmt, a);                \
        }                                         \
        } while(0)

#define TRACE_RESULT(c_or_e, fmt, r)             \
    if (c_or_e->sqlite_trace)                   \
        fprintf(stderr, ") returns " fmt "\n", r)
#define TRACE_EXTRA_RESULT(c_or_e, fmt, r)                               \
    do {                                                                \
        if (c_or_e->sqlite_trace) {                                    \
        if (r == NULL)                                                  \
            fprintf(stderr, "  no return value for NULL arg " #r "\n"); \
        else                                                            \
            fprintf(stderr, "  also returns " #r "=" fmt "\n", (*r));   \
        }                                                               \
    } while(0)

int
t_sqlite3_exec(PALITE_KV_ENV *env, sqlite3 *db, const char *sql, int (*callback)(void *,int,char**,char**), void *cookie, char **errmsg)
{
    int ret;

    TRACE_FUNC(env, sqlite3_exec);
    TRACE_ARG1(env, "%p", db);
    TRACE_ARG(env, "%s", sql);
    TRACE_ARG(env, "%p", callback);
    TRACE_ARG(env, "%p", cookie);
    ret = sqlite3_exec(db, sql, callback, cookie, errmsg);
    TRACE_RESULT(env, "%d", ret);
    TRACE_EXTRA_RESULT(env, "%s", errmsg);
    return (ret);
}

int
t_sqlite3_open(PALITE_KV_ENV *env, const char *filename, sqlite3 **returned_db)
{
    int ret;

    TRACE_FUNC(env, sqlite3_open);
    TRACE_ARG1(env, "%s", filename);
    ret = sqlite3_open(filename, returned_db);
    TRACE_RESULT(env, "%d", ret);
    TRACE_EXTRA_RESULT(env, "%p", returned_db);
    return (ret);
}

int
t_sqlite3_prepare_v2(PALITE_KV_ENV *env, sqlite3 *db, const char *sql, int nByte, sqlite3_stmt **returned_stmt, const char **returned_tail)
{
    int ret;

    TRACE_FUNC(env, sqlite3_prepare_v2);
    TRACE_ARG1(env, "%p", db);
    TRACE_ARG(env, "%s", sql);
    TRACE_ARG(env, "%d", nByte);
    ret = sqlite3_prepare_v2(db, sql, nByte, returned_stmt, returned_tail);
    TRACE_RESULT(env, "%d", ret);
    TRACE_EXTRA_RESULT(env, "%p", returned_stmt);
    TRACE_EXTRA_RESULT(env, "%p", returned_tail);
    return (ret);
}

int
t_sqlite3_close(PALITE_KV_ENV *env, sqlite3* db)
{
    int ret;

    TRACE_FUNC(env, sqlite3_close);
    TRACE_ARG1(env, "%p", db);
    ret = sqlite3_close(db);
    TRACE_RESULT(env, "%d", ret);
    return (ret);
}

int
t_sqlite3_reset(PALITE_KV_CONTEXT *context, sqlite3_stmt *stmt)
{
    int ret;

    TRACE_FUNC(context, sqlite3_reset);
    TRACE_ARG1(context, "%p", stmt);
    ret = sqlite3_reset(stmt);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

int
t_sqlite3_step(PALITE_KV_CONTEXT *context, sqlite3_stmt *stmt)
{
    int ret;

    TRACE_FUNC(context, sqlite3_step);
    TRACE_ARG1(context, "%p", stmt);
    ret = sqlite3_step(stmt);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

int
t_sqlite3_bind_int64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno, sqlite3_int64 value)
{
    int ret;

    TRACE_FUNC(context, sqlite3_bind_int64);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    TRACE_ARG(context, "%" PRId64, (int64_t)value);
    ret = sqlite3_bind_int64(stmt, argno, value);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

int
t_sqlite3_bind_blob64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno, const void* data, sqlite3_uint64 size, void(*callback)(void*))
{
    int ret;

    TRACE_FUNC(context, sqlite3_bind_blob64);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    TRACE_ARG(context, "%p", data);
    TRACE_ARG(context, "%" PRIu64, (uint64_t)size);
    TRACE_ARG(context, "%p", callback);
    ret = sqlite3_bind_blob64(stmt, argno, data, size, callback);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

int
t_sqlite3_bind_text(PALITE_KV_CONTEXT *context, sqlite3_stmt *stmt, int argno, const char *s, int len, void(*callback)(void*))
{
    int ret;

    TRACE_FUNC(context, sqlite3_bind_text);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    TRACE_ARG(context, "%s", s);
    TRACE_ARG(context, "%" PRIu64, (uint64_t)len);
    TRACE_ARG(context, "%p", callback);
    ret = sqlite3_bind_text(stmt, argno, s, len, callback);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

sqlite3_int64
t_sqlite3_column_int64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno)
{
    sqlite3_int64 ret;

    TRACE_FUNC(context, sqlite3_column_int64);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    ret = sqlite3_column_int64(stmt, argno);
    TRACE_RESULT(context, "%" PRId64, (int64_t)ret);
    return (ret);
}

const void *
t_sqlite3_column_blob(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno)
{
    const void *result;

    TRACE_FUNC(context, sqlite3_column_blob);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    result = sqlite3_column_blob(stmt, argno);
    TRACE_RESULT(context, "%p", result);
    return (result);
}

int
t_sqlite3_column_bytes(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno)
{
    int ret;

    TRACE_FUNC(context, sqlite3_column_bytes);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    ret = sqlite3_column_bytes(stmt, argno);
    TRACE_RESULT(context, "%d", ret);
    return (ret);
}

const unsigned char *
t_sqlite3_column_text(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int argno)
{
    const unsigned char *ret_str;

    TRACE_FUNC(context, sqlite3_column_text);
    TRACE_ARG1(context, "%p", stmt);
    TRACE_ARG(context, "%d", argno);
    ret_str = sqlite3_column_text(stmt, argno);
    TRACE_RESULT(context, "%s", ret_str);
    return (ret_str);
}
