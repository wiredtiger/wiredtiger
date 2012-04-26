/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	BDB	1			/* Berkeley DB header files */
#include "format.h"

static DBT key, value;
static uint8_t *keybuf;

static int
bdb_compare_reverse(DB *dbp, const DBT *k1, const DBT *k2)
{
	int cmp;
	size_t len;

	UNUSED(dbp);

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
	    DB_CREATE |
	    (g.c_delete_pct == 0 && g.c_insert_pct == 0 && g.c_write_pct == 0 ?
	    0 : DB_INIT_LOCK) |
	    DB_INIT_MPOOL | DB_PRIVATE, 0) == 0);
	assert(db_create(&db, dbenv, 0) == 0);

	if (g.c_file_type == ROW && g.c_reverse)
		assert(db->set_bt_compare(db, bdb_compare_reverse) == 0);

	assert(db->open(db, NULL, "__bdb", NULL, DB_BTREE, DB_CREATE, 0) == 0);
	g.bdb = db;
	assert(db->cursor(db, NULL, &dbc, 0) == 0);
	g.dbc = dbc;

	key_gen_setup(&keybuf);
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

	free(keybuf);
	keybuf = NULL;
}

void
bdb_insert(
    const void *key_data, uint32_t key_size,
    const void *value_data, uint32_t value_size)
{
	DBC *dbc;

	key.data = (void *)key_data;
	key.size = key_size;
	value.data = (void *)value_data;
	value.size = value_size;

	dbc = g.dbc;

	assert(dbc->put(dbc, &key, &value, DB_KEYFIRST) == 0);
}

void
bdb_np(int next,
    void *keyp, uint32_t *keysizep,
    void *valuep, uint32_t *valuesizep, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	*notfoundp = 0;
	if ((ret =
	    dbc->get(dbc, &key, &value, next ? DB_NEXT : DB_PREV)) != 0) {
		if (ret != DB_NOTFOUND)
			die(ret, "dbc.get: %s: {%.*s}",
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
bdb_read(uint64_t keyno, void *valuep, uint32_t *valuesizep, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	key.data = keybuf;
	key_gen(key.data, &key.size, keyno, 0);

	*notfoundp = 0;
	if ((ret = dbc->get(dbc, &key, &value, DB_SET)) != 0) {
		if (ret != DB_NOTFOUND)
			die(ret, "dbc.get: DB_SET: {%.*s}",
			    (int)key.size, (char *)key.data);
		*notfoundp = 1;
	} else {
		*(void **)valuep = value.data;
		*valuesizep = value.size;
	}
}

void
bdb_update(const void *arg_key, uint32_t arg_key_size,
    const void *arg_value, uint32_t arg_value_size, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	key.data = (void *)arg_key;
	key.size = arg_key_size;
	value.data = (void *)arg_value;
	value.size = arg_value_size;

	*notfoundp = 0;
	if ((ret = dbc->put(dbc, &key, &value, DB_KEYFIRST)) != 0) {
		if (ret != DB_NOTFOUND) {
			die(ret, "dbc.put: DB_KEYFIRST: {%.*s}{%.*s}",
			    (int)key.size, (char *)key.data,
			    (int)value.size, (char *)value.data);
		}
		*notfoundp = 1;
	}
}

void
bdb_remove(uint64_t keyno, int *notfoundp)
{
	DBC *dbc = g.dbc;
	int ret;

	key.data = keybuf;
	key_gen(key.data, &key.size, keyno, 0);

	bdb_read(keyno, &value.data, &value.size, notfoundp);
	if (*notfoundp)
		return;

	if ((ret = dbc->del(dbc, 0)) != 0) {
		if (ret != DB_NOTFOUND)
			die(ret, "dbc.del: {%.*s}",
			    (int)key.size, (char *)key.data);
		*notfoundp = 1;
	}
}
