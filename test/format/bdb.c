/*-
 * Public Domain 2014-2018 MongoDB, Inc.
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

#define	BDB	1			/* Berkeley DB header files */
#include "format.h"

static DBT key, value;
static WT_ITEM keyitem;

#define	bdb_die(ret, fmt, ...)						\
	testutil_die(0, "%s/%d: %s: " fmt,				\
	    __func__, __LINE__, db_strerror(ret), __VA_ARGS__);

static int
bdb_compare_reverse(DB *dbp, const DBT *k1, const DBT *k2
#if DB_VERSION_MAJOR >= 6
		, size_t *locp
#endif
)
{
	size_t len;
	int cmp;

	(void)(dbp);
#if DB_VERSION_MAJOR >= 6
	(void)(locp);
#endif

	len = (k1->size < k2->size) ? k1->size : k2->size;
	if ((cmp = memcmp(k2->data, k1->data, len)) == 0)
		cmp = ((int)k1->size - (int)k2->size);
	return (cmp);
}

void
bdb_open(void)
{
	DB *db;
	DBC *dbc;
	DB_ENV *dbenv;

	assert(db_env_create(&dbenv, 0) == 0);
	dbenv->set_errpfx(dbenv, "bdb");
	dbenv->set_errfile(dbenv, stderr);
	assert(dbenv->mutex_set_max(dbenv, 10000) == 0);
	assert(dbenv->set_cachesize(dbenv, 0, 50 * 1024 * 1024, 1) == 0);
	assert(dbenv->open(dbenv, NULL,
	    DB_CREATE | DB_INIT_LOCK | DB_INIT_MPOOL | DB_PRIVATE, 0) == 0);
	assert(db_create(&db, dbenv, 0) == 0);

	if (g.c_reverse)
		assert(db->set_bt_compare(db, bdb_compare_reverse) == 0);

	assert(db->open(
	    db, NULL, g.home_bdb, NULL, DB_BTREE, DB_CREATE, 0) == 0);
	g.bdb = db;
	assert(db->cursor(db, NULL, &dbc, 0) == 0);
	g.dbc = dbc;

	key_gen_init(&keyitem);
}

void
bdb_close(void)
{
	DB *db;
	DBC *dbc;
	DB_ENV *dbenv;

	dbc = g.dbc;
	db = g.bdb;
	dbenv = db->dbenv;
	assert(dbc->close(dbc) == 0);
	assert(db->close(db, 0) == 0);
	assert(dbenv->close(dbenv, 0) == 0);

	key_gen_teardown(&keyitem);
}

void
bdb_insert(
    const void *key_data, size_t key_size,
    const void *value_data, size_t value_size)
{
	DBC *dbc;

	key.data = (void *)key_data;
	key.size = (u_int32_t)key_size;
	value.data = (void *)value_data;
	value.size = (u_int32_t)value_size;

	dbc = g.dbc;

	assert(dbc->put(dbc, &key, &value, DB_KEYFIRST) == 0);
}

void
bdb_np(bool next,
    void *keyp, size_t *keysizep,
    void *valuep, size_t *valuesizep, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	*notfoundp = 0;
	if ((ret =
	    dbc->get(dbc, &key, &value, next ? DB_NEXT : DB_PREV)) != 0) {
		if (ret != DB_NOTFOUND)
			bdb_die(ret, "dbc.get: %s: {%.*s}",
			    next ? "DB_NEXT" : "DB_PREV",
			    (int)key.size, (char *)key.data);
		*notfoundp = 1;
	} else {
		*(void **)keyp = key.data;
		*keysizep = key.size;
		*(void **)valuep = value.data;
		*valuesizep = value.size;
	}
}

void
bdb_read(uint64_t keyno, void *valuep, size_t *valuesizep, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	key_gen(&keyitem, keyno);
	key.data = (void *)keyitem.data;
	key.size = (u_int32_t)keyitem.size;

	*notfoundp = 0;
	if ((ret = dbc->get(dbc, &key, &value, DB_SET)) != 0) {
		if (ret != DB_NOTFOUND)
			bdb_die(ret, "dbc.get: DB_SET: {%.*s}",
			    (int)key.size, (char *)key.data);
		*notfoundp = 1;
	} else {
		*(void **)valuep = value.data;
		*valuesizep = value.size;
	}
}

void
bdb_update(const void *arg_key, size_t arg_key_size,
    const void *arg_value, size_t arg_value_size)
{
	DBC *dbc = g.dbc;
	int ret;

	key.data = (void *)arg_key;
	key.size = (u_int32_t)arg_key_size;
	value.data = (void *)arg_value;
	value.size = (u_int32_t)arg_value_size;

	if ((ret = dbc->put(dbc, &key, &value, DB_KEYFIRST)) != 0)
		bdb_die(ret, "dbc.put: DB_KEYFIRST: {%.*s}{%.*s}",
		    (int)key.size, (char *)key.data,
		    (int)value.size, (char *)value.data);
}

void
bdb_remove(uint64_t keyno, int *notfoundp)
{
	DBC *dbc = g.dbc;
	size_t size;
	int ret;

	size = 0;
	key_gen(&keyitem, keyno);
	key.data = (void *)keyitem.data;
	key.size = (u_int32_t)keyitem.size;

	bdb_read(keyno, &value.data, &size, notfoundp);
	value.size = (u_int32_t)size;
	if (*notfoundp)
		return;

	/* Deleting a fixed-length item is the same as setting the bits to 0. */
	if (g.type == FIX)
		bdb_update(key.data, key.size, "", 1);
	else
		if ((ret = dbc->del(dbc, 0)) != 0) {
			if (ret != DB_NOTFOUND)
				bdb_die(ret, "dbc.del: {%.*s}",
				    (int)key.size, (char *)key.data);
			*notfoundp = 1;
		}
}

void
bdb_truncate(uint64_t start, uint64_t stop)
{
	DBC *dbc = g.dbc;
	size_t len;
	int cmp, ret, notfound;

	/* Deleting a fixed-length item is the same as setting the bits to 0. */
	if (g.type == FIX) {
		/*
		 * If we're deleting from/to the start/end of the database,
		 * correct for the number of records we have.
		 */
		if (start == 0)
			start = 1;
		if (stop == 0)
			stop = g.rows;
		for (; start <= stop; ++start)
			bdb_remove(start, &notfound);
		return;
	}

	if (start == 0) {
		ret = dbc->get(dbc, &key, &value, DB_FIRST);
		if (ret != 0 && ret != DB_NOTFOUND)
			bdb_die(ret, "%s", "dbc.get: DB_FIRST");
	} else {
		key_gen(&keyitem, start);
		key.data = (void *)keyitem.data;
		key.size = (u_int32_t)keyitem.size;
		ret = dbc->get(dbc, &key, &value, DB_SET_RANGE);
		if (ret != 0 && ret != DB_NOTFOUND)
			bdb_die(ret, "dbc.get: DB_SET: {%.*s}",
			    (int)key.size, (char *)key.data);
	}
	if (ret == DB_NOTFOUND)
		return;

	if (stop == 0) {
		do {
			ret = dbc->del(dbc, 0);
			if (ret != 0 && ret != DB_NOTFOUND)
				bdb_die(ret, "dbc.del: {%.*s}",
				    (int)key.size, (char *)key.data);
		} while ((ret = dbc->get(dbc, &key, &value, DB_NEXT)) == 0);
	} else {
		key_gen(&keyitem, stop);
		do {
			len = WT_MIN(key.size, keyitem.size);
			cmp = memcmp(key.data, keyitem.data, len);
			if (g.c_reverse) {
				if (cmp < 0 ||
				    (cmp == 0 && key.size < keyitem.size))
					break;
			} else
				if (cmp > 0 ||
				    (cmp == 0 && key.size > keyitem.size))
					break;
			ret = dbc->del(dbc, 0);
			if (ret != 0 && ret != DB_NOTFOUND)
				bdb_die(ret, "dbc.del: {%.*s}",
				    (int)key.size, (char *)key.data);
		} while ((ret = dbc->get(dbc, &key, &value, DB_NEXT)) == 0);
	}
	if (ret != 0 && ret != DB_NOTFOUND)
		bdb_die(ret, "%s", "dbc.get: DB_NEXT");
}
