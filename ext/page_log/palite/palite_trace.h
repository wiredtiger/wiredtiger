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

extern int t_sqlite3_exec(PALITE_KV_ENV *env, sqlite3 *db, const char *sql, int (*callback)(void *,int,char**,char**), void *, char **errmsg);
extern int t_sqlite3_open(PALITE_KV_ENV *env, const char *filename, sqlite3 **pdb);
extern int t_sqlite3_prepare_v2(PALITE_KV_ENV *env, sqlite3 *db, const char *sql, int nByte, sqlite3_stmt **pstmt, const char **ptail);
extern int t_sqlite3_close(PALITE_KV_ENV *env, sqlite3* db);
extern int t_sqlite3_reset(PALITE_KV_CONTEXT *context, sqlite3_stmt *stmt);
extern int t_sqlite3_step(PALITE_KV_CONTEXT *context, sqlite3_stmt *stmt);
extern int t_sqlite3_bind_int64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int, sqlite3_int64);
extern int t_sqlite3_bind_blob64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int, const void*, sqlite3_uint64, void(*)(void*));
extern int t_sqlite3_bind_text(PALITE_KV_CONTEXT *context, sqlite3_stmt*,int,const char*,int,void(*)(void*));
extern sqlite3_int64 t_sqlite3_column_int64(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int iCol);
extern const void *t_sqlite3_column_blob(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int iCol);
extern int t_sqlite3_column_bytes(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int iCol);
extern const unsigned char *t_sqlite3_column_text(PALITE_KV_CONTEXT *context, sqlite3_stmt* stmt, int iCol);
