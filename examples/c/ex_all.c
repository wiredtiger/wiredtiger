/*-
 * Public Domain 2014-2017 MongoDB, Inc.
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
 *
 * ex_all.c
 *	Containing a call to every method in the WiredTiger API.
 *
 *	It doesn't do anything very useful, just demonstrates how to call each
 *	method.  This file is used to populate the API reference with code
 *	fragments.
 */
#include <test_util.h>

static const char *home;

void add_collator(WT_CONNECTION *conn);
void add_extractor(WT_CONNECTION *conn);
void backup(WT_SESSION *session);
void checkpoint_ops(WT_SESSION *session);
void connection_ops(WT_CONNECTION *conn);
int  cursor_ops(WT_SESSION *session);
void cursor_search_near(WT_CURSOR *cursor);
void cursor_statistics(WT_SESSION *session);
void named_snapshot_ops(WT_SESSION *session);
void pack_ops(WT_SESSION *session);
void session_ops(WT_SESSION *session);
void transaction_ops(WT_CONNECTION *conn, WT_SESSION *session);

int
cursor_ops(WT_SESSION *session)
{
	WT_CURSOR *cursor;
	int ret;

	/*! [Open a cursor] */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	/*! [Open a cursor] */

	/*! [Open a cursor on the metadata] */
	error_check(session->open_cursor(
	    session, "metadata:", NULL, NULL, &cursor));
	/*! [Open a cursor on the metadata] */

	{
	WT_CURSOR *duplicate;
	const char *key = "some key";
	/*! [Duplicate a cursor] */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, key);
	error_check(cursor->search(cursor));

	/* Duplicate the cursor. */
	error_check(
	    session->open_cursor(session, NULL, cursor, NULL, &duplicate));
	/*! [Duplicate a cursor] */
	}

	{
	const char *key = "some key", *value = "some value";
	/*! [Reconfigure a cursor] */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=false", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);

	/* Reconfigure the cursor to overwrite the record. */
	error_check(cursor->reconfigure(cursor, "overwrite=true"));
	error_check(cursor->insert(cursor));
	/*! [Reconfigure a cursor] */
	}

	{
	/*! [boolean configuration string example] */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite", &cursor));
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=true", &cursor));
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=1", &cursor));
	/*! [boolean configuration string example] */
	}

	{
	/*! [open a named checkpoint] */
	error_check(session->open_cursor(session,
	    "table:mytable", NULL, "checkpoint=midnight", &cursor));
	/*! [open a named checkpoint] */
	}

	{
	/*! [open the default checkpoint] */
	error_check(session->open_cursor(session,
	    "table:mytable", NULL, "checkpoint=WiredTigerCheckpoint", &cursor));
	/*! [open the default checkpoint] */
	}

	{
	/*! [Get the cursor's string key] */
	const char *key;	/* Get the cursor's string key. */
	error_check(cursor->get_key(cursor, &key));
	/*! [Get the cursor's string key] */
	}

	{
	/*! [Set the cursor's string key] */
				/* Set the cursor's string key. */
	const char *key = "another key";
	cursor->set_key(cursor, key);
	/*! [Set the cursor's string key] */
	}

	{
	/*! [Get the cursor's record number key] */
	uint64_t recno;		/* Get the cursor's record number key. */
	error_check(cursor->get_key(cursor, &recno));
	/*! [Get the cursor's record number key] */
	}

	{
	/*! [Set the cursor's record number key] */
	uint64_t recno = 37;	/* Set the cursor's record number key. */
	cursor->set_key(cursor, recno);
	/*! [Set the cursor's record number key] */
	}

	{
	/*! [Get the cursor's composite key] */
			/* Get the cursor's "SiH" format composite key. */
	const char *first;
	int32_t second;
	uint16_t third;
	error_check(cursor->get_key(cursor, &first, &second, &third));
	/*! [Get the cursor's composite key] */
	}

	{
	/*! [Set the cursor's composite key] */
			/* Set the cursor's "SiH" format composite key. */
	cursor->set_key(cursor, "first", (int32_t)5, (uint16_t)7);
	/*! [Set the cursor's composite key] */
	}

	{
	/*! [Get the cursor's string value] */
	const char *value;	/* Get the cursor's string value. */
	error_check(cursor->get_value(cursor, &value));
	/*! [Get the cursor's string value] */
	}

	{
	/*! [Set the cursor's string value] */
				/* Set the cursor's string value. */
	const char *value = "another value";
	cursor->set_value(cursor, value);
	/*! [Set the cursor's string value] */
	}

	{
	/*! [Get the cursor's raw value] */
	WT_ITEM value;		/* Get the cursor's raw value. */
	error_check(cursor->get_value(cursor, &value));
	/*! [Get the cursor's raw value] */
	}

	{
	/*! [Set the cursor's raw value] */
	WT_ITEM value;		/* Set the cursor's raw value. */
	value.data = "another value";
	value.size = strlen("another value");
	cursor->set_value(cursor, &value);
	/*! [Set the cursor's raw value] */
	}

	/*! [Return the next record] */
	error_check(cursor->next(cursor));
	/*! [Return the next record] */

	/*! [Return the previous record] */
	error_check(cursor->prev(cursor));
	/*! [Return the previous record] */

	/*! [Reset the cursor] */
	error_check(cursor->reset(cursor));
	/*! [Reset the cursor] */

	{
	WT_CURSOR *other = NULL;
	/*! [Cursor comparison] */
	int compare;
	error_check(cursor->compare(cursor, other, &compare));
	if (compare == 0) {
		/* Cursors reference the same key */
	} else if (compare < 0) {
		/* Cursor key less than other key */
	} else if (compare > 0) {
		/* Cursor key greater than other key */
	}
	/*! [Cursor comparison] */
	}

	{
	WT_CURSOR *other = NULL;
	/*! [Cursor equality] */
	int equal;
	error_check(cursor->equals(cursor, other, &equal));
	if (equal) {
		/* Cursors reference the same key */
	} else {
		/* Cursors don't reference the same key */
	}
	/*! [Cursor equality] */
	}

	{
	/*! [Search for an exact match] */
	const char *key = "some key";
	cursor->set_key(cursor, key);
	error_check(cursor->search(cursor));
	/*! [Search for an exact match] */
	}

	cursor_search_near(cursor);

	{
	/*! [Insert a new record or overwrite an existing record] */
	/* Insert a new record or overwrite an existing record. */
	const char *key = "some key", *value = "some value";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	error_check(cursor->insert(cursor));
	/*! [Insert a new record or overwrite an existing record] */
	}

	{
	/*! [Insert a new record and fail if the record exists] */
	/* Insert a new record and fail if the record exists. */
	const char *key = "some key", *value = "some value";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=false", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	error_check(cursor->insert(cursor));
	/*! [Insert a new record and fail if the record exists] */
	}

	{
	/*! [Insert a new record and assign a record number] */
	/* Insert a new record and assign a record number. */
	uint64_t recno;
	const char *value = "some value";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "append", &cursor));
	cursor->set_value(cursor, value);
	error_check(cursor->insert(cursor));
	error_check(cursor->get_key(cursor, &recno));
	/*! [Insert a new record and assign a record number] */
	}

	{
	/*! [Reserve a record] */
	const char *key = "some key";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, key);
	error_check(cursor->reserve(cursor));
	/*! [Reserve a record] */
	}

	{
	/*! [Modify an existing record] */
	WT_MODIFY entries[3];
	const char *key = "some key";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));

	/* Position the cursor. */
	cursor->set_key(cursor, key);
	error_check(cursor->search(cursor));

	/* Replace 20 bytes starting at byte offset 5. */
	entries[0].data.data = "some data";
	entries[0].data.size = strlen(entries[0].data.data);
	entries[0].offset = 5;
	entries[0].size = 20;

	/* Insert data at byte offset 40. */
	entries[1].data.data = "and more data";
	entries[1].data.size = strlen(entries[1].data.data);
	entries[1].offset = 40;
	entries[1].size = 0;

	/* Replace 2 bytes starting at byte offset 10. */
	entries[2].data.data = "and more data";
	entries[2].data.size = strlen(entries[2].data.data);
	entries[2].offset = 10;
	entries[2].size = 2;

	error_check(cursor->modify(cursor, entries, 3));
	/*! [Modify an existing record] */
	}

	{
	/*! [Update an existing record or insert a new record] */
	const char *key = "some key", *value = "some value";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	error_check(cursor->update(cursor));
	/*! [Update an existing record or insert a new record] */
	}

	{
	/*! [Update an existing record and fail if DNE] */
	const char *key = "some key", *value = "some value";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=false", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	error_check(cursor->update(cursor));
	/*! [Update an existing record and fail if DNE] */
	}

	{
	/*! [Remove a record] */
	const char *key = "some key";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, key);
	error_check(cursor->remove(cursor));
	/*! [Remove a record] */
	}

	{
	/*! [Remove a record and fail if DNE] */
	const char *key = "some key";
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, "overwrite=false", &cursor));
	cursor->set_key(cursor, key);
	error_check(cursor->remove(cursor));
	/*! [Remove a record and fail if DNE] */
	}

	{
	/*! [Display an error] */
	const char *key = "non-existent key";
	cursor->set_key(cursor, key);
	if ((ret = cursor->remove(cursor)) != 0) {
		fprintf(stderr,
		    "cursor.remove: %s\n", wiredtiger_strerror(ret));
		return (ret);
	}
	/*! [Display an error] */
	}

	{
	/*! [Display an error thread safe] */
	const char *key = "non-existent key";
	cursor->set_key(cursor, key);
	if ((ret = cursor->remove(cursor)) != 0) {
		fprintf(stderr,
		    "cursor.remove: %s\n",
		    cursor->session->strerror(cursor->session, ret));
		return (ret);
	}
	/*! [Display an error thread safe] */
	}

	/*! [Close the cursor] */
	error_check(cursor->close(cursor));
	/*! [Close the cursor] */

	return (0);
}

void
cursor_search_near(WT_CURSOR *cursor)
{
	int exact, ret;
	const char *key = "some key";

	/*! [Search for an exact or adjacent match] */
	cursor->set_key(cursor, key);
	error_check(cursor->search_near(cursor, &exact));
	if (exact == 0) {
		/* an exact match */
	} else if (exact < 0) {
		/* returned smaller key */
	} else if (exact > 0) {
		/* returned larger key */
	}
	/*! [Search for an exact or adjacent match] */

	/*! [Forward scan greater than or equal] */
	cursor->set_key(cursor, key);
	error_check(cursor->search_near(cursor, &exact));
	if (exact >= 0) {
		/* include first key returned in the scan */
	}

	while ((ret = cursor->next(cursor)) == 0) {
		/* the rest of the scan */
	}
	scan_end_check(ret == WT_NOTFOUND);
	/*! [Forward scan greater than or equal] */

	/*! [Backward scan less than] */
	cursor->set_key(cursor, key);
	error_check(cursor->search_near(cursor, &exact));
	if (exact < 0) {
		/* include first key returned in the scan */
	}

	while ((ret = cursor->prev(cursor)) == 0) {
		/* the rest of the scan */
	}
	scan_end_check(ret == WT_NOTFOUND);
	/*! [Backward scan less than] */
}

void
checkpoint_ops(WT_SESSION *session)
{
	/*! [Checkpoint examples] */
	/* Checkpoint the database. */
	error_check(session->checkpoint(session, NULL));

	/* Checkpoint of the database, creating a named snapshot. */
	error_check(session->checkpoint(session, "name=June01"));

	/*
	 * Checkpoint a list of objects.
	 * JSON parsing requires quoting the list of target URIs.
	 */
	error_check(session->checkpoint(
	    session, "target=(\"table:table1\",\"table:table2\")"));

	/*
	 * Checkpoint a list of objects, creating a named snapshot.
	 * JSON parsing requires quoting the list of target URIs.
	 */
	error_check(session->checkpoint(
	    session, "target=(\"table:mytable\"),name=midnight"));

	/* Checkpoint the database, discarding all previous snapshots. */
	error_check(session->checkpoint(session, "drop=(from=all)"));

	/* Checkpoint the database, discarding the "midnight" snapshot. */
	error_check(session->checkpoint(session, "drop=(midnight)"));

	/*
	 * Checkpoint the database, discarding all snapshots after and
	 * including "noon".
	 */
	error_check(session->checkpoint(session, "drop=(from=noon)"));

	/*
	 * Checkpoint the database, discarding all snapshots before and
	 * including "midnight".
	 */
	error_check(session->checkpoint(session, "drop=(to=midnight)"));

	/*
	 * Create a checkpoint of a table, creating the "July01" snapshot and
	 * discarding the "May01" and "June01" snapshots.
	 * JSON parsing requires quoting the list of target URIs.
	 */
	error_check(session->checkpoint(session,
	    "target=(\"table:mytable\"),name=July01,drop=(May01,June01)"));
	/*! [Checkpoint examples] */

	/*! [JSON quoting example] */
	/*
	 * Checkpoint a list of objects.
	 * JSON parsing requires quoting the list of target URIs.
	 */
	error_check(session->checkpoint(
	    session, "target=(\"table:table1\",\"table:table2\")"));
	/*! [JSON quoting example] */
}

void
cursor_statistics(WT_SESSION *session)
{
	WT_CURSOR *cursor;

	/*! [Statistics cursor database] */
	error_check(session->open_cursor(
	    session, "statistics:", NULL, NULL, &cursor));
	/*! [Statistics cursor database] */

	/*! [Statistics cursor table] */
	error_check(session->open_cursor(
	    session, "statistics:table:mytable", NULL, NULL, &cursor));
	/*! [Statistics cursor table] */

	/*! [Statistics cursor table fast] */
	error_check(session->open_cursor(session,
	    "statistics:table:mytable", NULL, "statistics=(fast)", &cursor));
	/*! [Statistics cursor table fast] */

	/*! [Statistics clear configuration] */
	error_check(session->open_cursor(session,
	    "statistics:", NULL, "statistics=(fast,clear)", &cursor));
	/*! [Statistics clear configuration] */

	/*! [Statistics cursor clear configuration] */
	error_check(session->open_cursor(session,
	    "statistics:table:mytable",
	    NULL, "statistics=(all,clear)", &cursor));
	/*! [Statistics cursor clear configuration] */
}

void
named_snapshot_ops(WT_SESSION *session)
{
	/*! [Snapshot examples] */
	/* Create a named snapshot */
	error_check(session->snapshot(session, "name=June01"));

	/* Open a transaction at a given snapshot */
	error_check(session->begin_transaction(session, "snapshot=June01"));

	/* Drop all named snapshots */
	error_check(session->snapshot(session, "drop=(all)"));
	/*! [Snapshot examples] */
}

void
session_ops(WT_SESSION *session)
{
	/*! [Reconfigure a session] */
	error_check(session->reconfigure(session, "isolation=snapshot"));
	/*! [Reconfigure a session] */

	/*! [Create a table] */
	error_check(session->create(session,
	    "table:mytable", "key_format=S,value_format=S"));
	/*! [Create a table] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a column-store table] */
	error_check(session->create(session,
	    "table:mytable", "key_format=r,value_format=S"));

	/*! [Alter a table] */
	error_check(session->alter(session,
	    "table:mytable", "access_pattern_hint=random"));
	/*! [Alter a table] */

	/*! [Create a column-store table] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a table with columns] */
	/*
	 * Create a table with columns: keys are record numbers, values are
	 * (string, signed 32-bit integer, unsigned 16-bit integer).
	 */
	error_check(session->create(session, "table:mytable",
	    "key_format=r,value_format=SiH,"
	    "columns=(id,department,salary,year-started)"));
	/*! [Create a table with columns] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a table and configure the page size] */
	error_check(session->create(session,
	    "table:mytable", "key_format=S,value_format=S,"
	    "internal_page_max=16KB,leaf_page_max=1MB,leaf_value_max=64KB"));
	/*! [Create a table and configure the page size] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a table and configure a large leaf value max] */
	error_check(session->create(session,
	    "table:mytable", "key_format=S,value_format=S,"
	    "leaf_page_max=16KB,leaf_value_max=256KB"));
	/*! [Create a table and configure a large leaf value max] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*
	 * This example code gets run, and the compression libraries might not
	 * be loaded, causing the create to fail.  The documentation requires
	 * the code snippets, use #ifdef's to avoid running it.
	 */
#ifdef MIGHT_NOT_RUN
	/*! [Create a lz4 compressed table] */
	error_check(session->create(session,
	    "table:mytable",
	    "block_compressor=lz4,key_format=S,value_format=S"));
	/*! [Create a lz4 compressed table] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a snappy compressed table] */
	error_check(session->create(session,
	    "table:mytable",
	    "block_compressor=snappy,key_format=S,value_format=S"));
	/*! [Create a snappy compressed table] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a zlib compressed table] */
	error_check(session->create(session,
	    "table:mytable",
	    "block_compressor=zlib,key_format=S,value_format=S"));
	/*! [Create a zlib compressed table] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a zstd compressed table] */
	error_check(session->create(session,
	    "table:mytable",
	    "block_compressor=zstd,key_format=S,value_format=S"));
	/*! [Create a zstd compressed table] */
	error_check(session->drop(session, "table:mytable", NULL));
#endif

	/*! [Configure checksums to uncompressed] */
	error_check(session->create(session, "table:mytable",
	    "key_format=S,value_format=S,checksum=uncompressed"));
	/*! [Configure checksums to uncompressed] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Configure dictionary compression on] */
	error_check(session->create(session, "table:mytable",
	    "key_format=S,value_format=S,dictionary=1000"));
	/*! [Configure dictionary compression on] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Configure key prefix compression on] */
	error_check(session->create(session, "table:mytable",
	    "key_format=S,value_format=S,prefix_compression=true"));
	/*! [Configure key prefix compression on] */
	error_check(session->drop(session, "table:mytable", NULL));

#ifdef MIGHT_NOT_RUN
						/* Requires sync_file_range */
	/*! [os_cache_dirty_max configuration] */
	error_check(session->create(
	    session, "table:mytable", "os_cache_dirty_max=500MB"));
	/*! [os_cache_dirty_max configuration] */
	error_check(session->drop(session, "table:mytable", NULL));

						/* Requires posix_fadvise */
	/*! [os_cache_max configuration] */
	error_check(session->create(
	    session, "table:mytable", "os_cache_max=1GB"));
	/*! [os_cache_max configuration] */
	error_check(session->drop(session, "table:mytable", NULL));
#endif
	/*! [Configure block_allocation] */
	error_check(session->create(session, "table:mytable",
	    "key_format=S,value_format=S,block_allocation=first"));
	/*! [Configure block_allocation] */
	error_check(session->drop(session, "table:mytable", NULL));

	/*! [Create a cache-resident object] */
	error_check(session->create(
	    session, "table:mytable",
	    "key_format=r,value_format=S,cache_resident=true"));
	/*! [Create a cache-resident object] */
	error_check(session->drop(session, "table:mytable", NULL));

	{
	/* Create a table for the session operations. */
	error_check(session->create(
	    session, "table:mytable", "key_format=S,value_format=S"));

	/*! [Compact a table] */
	error_check(session->compact(session, "table:mytable", NULL));
	/*! [Compact a table] */

	/*! [Rebalance a table] */
	error_check(session->rebalance(session, "table:mytable", NULL));
	/*! [Rebalance a table] */

	error_check(session->create(
	    session, "table:old",
	    "key_format=r,value_format=S,cache_resident=true"));
	/*! [Rename a table] */
	error_check(session->rename(session, "table:old", "table:new", NULL));
	/*! [Rename a table] */

	/*! [Salvage a table] */
	error_check(session->salvage(session, "table:mytable", NULL));
	/*! [Salvage a table] */

	/*! [Truncate a table] */
	error_check(session->truncate(
	    session, "table:mytable", NULL, NULL, NULL));
	/*! [Truncate a table] */

	/*! [Transaction sync] */
	error_check(session->transaction_sync(session, NULL));
	/*! [Transaction sync] */

	/*! [Reset the session] */
	error_check(session->reset(session));
	/*! [Reset the session] */

	{
	/*
	 * Insert a pair of keys so we can truncate a range.
	 */
	WT_CURSOR *cursor;
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	cursor->set_key(cursor, "June01");
	cursor->set_value(cursor, "value");
	error_check(cursor->update(cursor));
	cursor->set_key(cursor, "June30");
	cursor->set_value(cursor, "value");
	error_check(cursor->update(cursor));
	error_check(cursor->close(cursor));

	{
	/*! [Truncate a range] */
	WT_CURSOR *start, *stop;

	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &start));
	start->set_key(start, "June01");
	error_check(start->search(start));

	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &stop));
	stop->set_key(stop, "June30");
	error_check(stop->search(stop));

	error_check(session->truncate(session, NULL, start, stop, NULL));
	/*! [Truncate a range] */
	error_check(stop->close(stop));
	error_check(start->close(start));
	}
	}

	/*! [Upgrade a table] */
	error_check(session->upgrade(session, "table:mytable", NULL));
	/*! [Upgrade a table] */

	/*! [Verify a table] */
	error_check(session->verify(session, "table:mytable", NULL));
	/*! [Verify a table] */

	/*! [Drop a table] */
	error_check(session->drop(session, "table:mytable", NULL));
	/*! [Drop a table] */
	}

	/*! [Close a session] */
	error_check(session->close(session, NULL));
	/*! [Close a session] */
}

void
transaction_ops(WT_CONNECTION *conn, WT_SESSION *session)
{
	WT_CURSOR *cursor;

	/*! [transaction commit/rollback] */
	/*
	 * Cursors may be opened before or after the transaction begins, and in
	 * either case, subsequent operations are included in the transaction.
	 * Opening cursors before the transaction begins allows applications to
	 * cache cursors and use them for multiple operations.
	 */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	error_check(session->begin_transaction(session, NULL));

	cursor->set_key(cursor, "key");
	cursor->set_value(cursor, "value");
	switch (cursor->update(cursor)) {
	case 0:					/* Update success */
		error_check(session->commit_transaction(session, NULL));
		/*
		 * If commit_transaction succeeds, cursors remain positioned; if
		 * commit_transaction fails, the transaction was rolled-back and
		 * and all cursors are reset.
		 */
		break;
	case WT_ROLLBACK:			/* Update conflict */
	default:				/* Other error */
		error_check(session->rollback_transaction(session, NULL));
		/* The rollback_transaction call resets all cursors. */
		break;
	}

	/*
	 * Cursors remain open and may be used for multiple transactions.
	 */
	/*! [transaction commit/rollback] */
	error_check(cursor->close(cursor));

	/*! [transaction isolation] */
	/* A single transaction configured for snapshot isolation. */
	error_check(session->open_cursor(
	    session, "table:mytable", NULL, NULL, &cursor));
	error_check(session->begin_transaction(session, "isolation=snapshot"));
	cursor->set_key(cursor, "some-key");
	cursor->set_value(cursor, "some-value");
	error_check(cursor->update(cursor));
	error_check(session->commit_transaction(session, NULL));
	/*! [transaction isolation] */

	/*! [session isolation configuration] */
	/* Open a session configured for read-uncommitted isolation. */
	error_check(conn->open_session(
	    conn, NULL, "isolation=read_uncommitted", &session));
	/*! [session isolation configuration] */

	/*! [session isolation re-configuration] */
	/* Re-configure a session for snapshot isolation. */
	error_check(session->reconfigure(session, "isolation=snapshot"));
	/*! [session isolation re-configuration] */

	{
	/*! [transaction pinned range] */
	/* Check the transaction ID range pinned by the session handle. */
	uint64_t range;

	error_check(session->transaction_pinned_range(session, &range));
	/*! [transaction pinned range] */
	}

	/*! [transaction timestamp] */
	error_check(
	    session->timestamp_transaction(session, "commit_timestamp=2a"));
	/*! [transaction timestamp] */

	{
#ifndef WT_TIMESTAMP_SIZE
#define	WT_TIMESTAMP_SIZE	8
#endif
	/*! [query timestamp] */
	char timestamp_buf[2 * WT_TIMESTAMP_SIZE + 1];

	error_check(conn->query_timestamp(
	    conn, timestamp_buf, "get=all_committed"));
	/*! [query timestamp] */
	}

	/*! [set oldest timestamp] */
	error_check(conn->set_timestamp(conn, "oldest_timestamp=2a"));
	/*! [set oldest timestamp] */
}

/*! [Implement WT_COLLATOR] */
/*
 * A simple example of the collator API: compare the keys as strings.
 */
static int
my_compare(WT_COLLATOR *collator, WT_SESSION *session,
    const WT_ITEM *value1, const WT_ITEM *value2, int *cmp)
{
	const char *p1, *p2;

	/* Unused parameters */
	(void)collator;
	(void)session;

	p1 = (const char *)value1->data;
	p2 = (const char *)value2->data;
	for (; *p1 != '\0' && *p1 == *p2; ++p1, ++p2)
		;

	*cmp = (int)*p2 - (int)*p1;
	return (0);
}
/*! [Implement WT_COLLATOR] */

void
add_collator(WT_CONNECTION *conn)
{
	/*! [WT_COLLATOR register] */
	static WT_COLLATOR my_collator = { my_compare, NULL, NULL };
	error_check(conn->add_collator(
	    conn, "my_collator", &my_collator, NULL));
	/*! [WT_COLLATOR register] */
}

/*! [WT_EXTRACTOR] */
static int
my_extract(WT_EXTRACTOR *extractor, WT_SESSION *session,
    const WT_ITEM *key, const WT_ITEM *value,
    WT_CURSOR *result_cursor)
{
	/* Unused parameters */
	(void)extractor;
	(void)session;
	(void)key;

	result_cursor->set_key(result_cursor, value);
	return (result_cursor->insert(result_cursor));
}
/*! [WT_EXTRACTOR] */

void
add_extractor(WT_CONNECTION *conn)
{
	/*! [WT_EXTRACTOR register] */
	static WT_EXTRACTOR my_extractor = {my_extract, NULL, NULL};

	error_check(conn->add_extractor(
	    conn, "my_extractor", &my_extractor, NULL));
	/*! [WT_EXTRACTOR register] */
}

void
connection_ops(WT_CONNECTION *conn)
{
#ifdef MIGHT_NOT_RUN
	/*! [Load an extension] */
	error_check(conn->load_extension(conn, "my_extension.dll", NULL));

	error_check(conn->load_extension(conn,
	    "datasource/libdatasource.so",
	    "config=[device=/dev/sd1,alignment=64]"));
	/*! [Load an extension] */
#endif

	add_collator(conn);
	add_extractor(conn);

	/*! [Reconfigure a connection] */
	error_check(conn->reconfigure(conn, "eviction_target=75"));
	/*! [Reconfigure a connection] */

	/*! [Get the database home directory] */
	printf("The database home is %s\n", conn->get_home(conn));
	/*! [Get the database home directory] */

	/*! [Check if the database is newly created] */
	if (conn->is_new(conn)) {
		/* First time initialization. */
	}
	/*! [Check if the database is newly created] */

	/*! [Validate a configuration string] */
	/*
	 * Validate a configuration string for a WiredTiger function or method.
	 *
	 * Functions are specified by name (for example, "wiredtiger_open").
	 *
	 * Methods are specified using a concatenation of the handle name, a
	 * period and the method name (for example, session create would be
	 * "WT_SESSION.create" and cursor close would be WT_CURSOR.close").
	 */
	error_check(wiredtiger_config_validate(
	    NULL, NULL, "WT_SESSION.create", "allocation_size=32KB"));
	/*! [Validate a configuration string] */

	{
	/*! [Open a session] */
	WT_SESSION *session;
	error_check(conn->open_session(conn, NULL, NULL, &session));
	/*! [Open a session] */

	session_ops(session);
	}

	/*! [Configure method configuration] */
	/*
	 * Applications opening a cursor for the data-source object "my_data"
	 * have an additional configuration option "entries", which is an
	 * integer type, defaults to 5, and must be an integer between 1 and 10.
	 *
	 * The method being configured is specified using a concatenation of the
	 * handle name, a period and the method name.
	 */
	error_check(conn->configure_method(conn,
	    "WT_SESSION.open_cursor",
	    "my_data:", "entries=5", "int", "min=1,max=10"));

	/*
	 * Applications opening a cursor for the data-source object "my_data"
	 * have an additional configuration option "devices", which is a list
	 * of strings.
	 */
	error_check(conn->configure_method(conn,
	    "WT_SESSION.open_cursor", "my_data:", "devices", "list", NULL));
	/*! [Configure method configuration] */

	/*! [Close a connection] */
	error_check(conn->close(conn, NULL));
	/*! [Close a connection] */
}

void
pack_ops(WT_SESSION *session)
{
	{
	/*! [Get the packed size] */
	size_t size;
	error_check(wiredtiger_struct_size(
	    session, &size, "iSh", 42, "hello", -3));
	/*! [Get the packed size] */
	}

	{
	/*! [Pack fields into a buffer] */
	char buf[100];
	error_check(wiredtiger_struct_pack(
	    session, buf, sizeof(buf), "iSh", 42, "hello", -3));
	/*! [Pack fields into a buffer] */

	{
	/*! [Unpack fields from a buffer] */
	int i;
	char *s;
	short h;
	error_check(wiredtiger_struct_unpack(
	    session, buf, sizeof(buf), "iSh", &i, &s, &h));
	/*! [Unpack fields from a buffer] */
	}
	}
}

void
backup(WT_SESSION *session)
{
	char buf[1024];

	/*! [backup]*/
	WT_CURSOR *cursor;
	const char *filename;
	int ret;

	/* Create the backup directory. */
	error_check(mkdir("/path/database.backup", 077));

	/* Open the backup data source. */
	error_check(session->open_cursor(
	    session, "backup:", NULL, NULL, &cursor));

	/* Copy the list of files. */
	while ((ret = cursor->next(cursor)) == 0) {
		error_check(cursor->get_key(cursor, &filename));
		(void)snprintf(buf, sizeof(buf),
		    "cp /path/database/%s /path/database.backup/%s",
		    filename, filename);
		error_check(system(buf));
	}
	scan_end_check(ret == WT_NOTFOUND);

	error_check(cursor->close(cursor));
	/*! [backup]*/

	/*! [incremental backup]*/
	/* Open the backup data source for incremental backup. */
	error_check(session->open_cursor(
	    session, "backup:", NULL, "target=(\"log:\")", &cursor));
	/*! [incremental backup]*/
	error_check(cursor->close(cursor));

	/*! [backup of a checkpoint]*/
	error_check(session->checkpoint(
	    session, "drop=(from=June01),name=June01"));
	/*! [backup of a checkpoint]*/
}

int
main(int argc, char *argv[])
{
	WT_CONNECTION *conn;

	home = example_setup(argc, argv);

	/*! [Open a connection] */
	error_check(wiredtiger_open(home, NULL,
	    "create,cache_size=5GB,log=(enabled,recover=on)", &conn));
	/*! [Open a connection] */

	connection_ops(conn);
	/*
	 * The connection has been closed.
	 */

#ifdef MIGHT_NOT_RUN
	/*
	 * This example code gets run, and the compression libraries might not
	 * be installed, causing the open to fail.  The documentation requires
	 * the code snippets, use #ifdef's to avoid running it.
	 */
	/*! [Configure lz4 extension] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/libwiredtiger_lz4.so]", &conn));
	/*! [Configure lz4 extension] */
	error_check(conn->close(conn, NULL));

	/*! [Configure snappy extension] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/libwiredtiger_snappy.so]", &conn));
	/*! [Configure snappy extension] */
	error_check(conn->close(conn, NULL));

	/*! [Configure zlib extension] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/libwiredtiger_zlib.so]", &conn));
	/*! [Configure zlib extension] */
	error_check(conn->close(conn, NULL));

	/*! [Configure zlib extension with compression level] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/"
	    "libwiredtiger_zlib.so=[config=[compression_level=3]]]", &conn));
	/*! [Configure zlib extension with compression level] */
	error_check(conn->close(conn, NULL));

	/*! [Configure zstd extension] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/libwiredtiger_zstd.so]", &conn));
	/*! [Configure zstd extension] */
	error_check(conn->close(conn, NULL));

	/*! [Configure zstd extension with compression level] */
	error_check(wiredtiger_open(home, NULL,
	    "create,"
	    "extensions=[/usr/local/lib/"
	    "libwiredtiger_zstd.so=[config=[compression_level=9]]]", &conn));
	/*! [Configure zstd extension with compression level] */
	error_check(conn->close(conn, NULL));

	/*
	 * This example code gets run, and direct I/O might not be available,
	 * causing the open to fail.  The documentation requires code snippets,
	 * use #ifdef's to avoid running it.
	 */
	/* Might Not Run: direct I/O may not be available. */
	/*! [Configure direct_io for data files] */
	error_check(wiredtiger_open(
	    home, NULL, "create,direct_io=[data]", &conn));
	/*! [Configure direct_io for data files] */
	error_check(conn->close(conn, NULL));
#endif

	/*! [Configure file_extend] */
	error_check(wiredtiger_open(
	    home, NULL, "create,file_extend=(data=16MB)", &conn));
	/*! [Configure file_extend] */
	error_check(conn->close(conn, NULL));

	/*! [Eviction configuration] */
	/*
	 * Configure eviction to begin at 90% full, and run until the cache
	 * is only 75% dirty.
	 */
	error_check(wiredtiger_open(home, NULL,
	    "create,eviction_trigger=90,eviction_dirty_target=75", &conn));
	/*! [Eviction configuration] */
	error_check(conn->close(conn, NULL));

	/*! [Eviction worker configuration] */
	/* Configure up to four eviction threads */
	error_check(wiredtiger_open(home, NULL,
	    "create,eviction_trigger=90,eviction=(threads_max=4)", &conn));
	/*! [Eviction worker configuration] */
	error_check(conn->close(conn, NULL));

	/*! [Statistics configuration] */
	error_check(wiredtiger_open(
	    home, NULL, "create,statistics=(all)", &conn));
	/*! [Statistics configuration] */
	error_check(conn->close(conn, NULL));

	/*! [Statistics logging] */
	error_check(wiredtiger_open(
	    home, NULL, "create,statistics_log=(wait=30)", &conn));
	/*! [Statistics logging] */
	error_check(conn->close(conn, NULL));

#ifdef MIGHT_NOT_RUN
	/*
	 * Don't run this code, statistics logging doesn't yet support tables.
	 */
	/*! [Statistics logging with a table] */
	error_check(wiredtiger_open(home, NULL,
	    "create, statistics_log=("
	    "sources=(\"table:table1\",\"table:table2\"), wait=5)", &conn));
	/*! [Statistics logging with a table] */
	error_check(conn->close(conn, NULL));

	/*
	 * Don't run this code, statistics logging doesn't yet support indexes.
	 */
	/*! [Statistics logging with a source type] */
	error_check(wiredtiger_open(home, NULL,
	    "create, statistics_log=(sources=(\"index:\"), wait=5)", &conn));
	/*! [Statistics logging with a source type] */
	error_check(conn->close(conn, NULL));

	/*
	 * Don't run this code, because memory checkers get very upset when we
	 * leak memory.
	 */
	error_check(wiredtiger_open(home, NULL, "create", &conn));
	/*! [Connection close leaking memory] */
	error_check(conn->close(conn, "leak_memory=true"));
	/*! [Connection close leaking memory] */
#endif

	/*! [Get the WiredTiger library version #1] */
	printf("WiredTiger version %s\n", wiredtiger_version(NULL, NULL, NULL));
	/*! [Get the WiredTiger library version #1] */

	{
	/*! [Get the WiredTiger library version #2] */
	int major_v, minor_v, patch;
	(void)wiredtiger_version(&major_v, &minor_v, &patch);
	printf("WiredTiger version is %d, %d (patch %d)\n",
	    major_v, minor_v, patch);
	/*! [Get the WiredTiger library version #2] */
	}

	return (EXIT_SUCCESS);
}
