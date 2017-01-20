/*-
 * Public Domain 2014-2016 MongoDB, Inc.
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
#include "test_util.h"

/*
 * JIRA ticket reference: WT-3135
 * Test case description: There are two tests, one uses a custom collator,
 * the second uses a custom collator and extractor. In each case there
 * are index keys having variable length and search_near is used with
 * keys both longer and shorter than the keys in the index.
 * Failure mode: The custom compare routine is given a truncated
 * key to compare, and the unpack functions return errors because of that.
 */

static
bool item_str_equal(WT_ITEM *item, const char *str)
{
	return (item->size == strlen(str) + 1 && strncmp((char *)item->data,
	    str, item->size) == 0);
}

static
int compare_int(int a, int b)
{
	return (a < b ? -1 : (a > b ? 1 : 0));
}

static
int index_compare_primary(WT_PACK_STREAM *s1, WT_PACK_STREAM *s2, int *cmp)
{
	int64_t pkey1, pkey2;
	int rc1, rc2;

	rc1 = wiredtiger_unpack_int(s1, &pkey1);
	rc2 = wiredtiger_unpack_int(s2, &pkey2);

	if (rc1 == 0 && rc2 == 0)
		*cmp = compare_int(pkey1, pkey2);
	else if (rc1 != 0 && rc2 != 0)
		*cmp = 0;
	else if (rc1 != 0)
		*cmp = -1;
	else
		*cmp = 1;
	return (0);
}

static
int index_compare_S(WT_COLLATOR *collator, WT_SESSION *session,
    const WT_ITEM *key1, const WT_ITEM *key2, int *cmp)
{
	WT_PACK_STREAM *s1, *s2;
	const char *skey1, *skey2;

	(void)collator;

	testutil_check(wiredtiger_unpack_start(session, "Si", key1->data,
	    key1->size, &s1));
	testutil_check(wiredtiger_unpack_start(session, "Si", key2->data,
	    key2->size, &s2));

	testutil_check(wiredtiger_unpack_str(s1, &skey1));
	testutil_check(wiredtiger_unpack_str(s2, &skey2));

	if ((*cmp = strcmp(skey1, skey2)) == 0)
		testutil_check(index_compare_primary(s1, s2, cmp));

	testutil_check(wiredtiger_pack_close(s1, NULL));
	testutil_check(wiredtiger_pack_close(s2, NULL));

	return (0);
}

static
int index_compare_u(WT_COLLATOR *collator, WT_SESSION *session,
    const WT_ITEM *key1, const WT_ITEM *key2, int *cmp)
{
	WT_ITEM skey1, skey2;
	WT_PACK_STREAM *s1, *s2;

	(void)collator;

	testutil_check(wiredtiger_unpack_start(session, "ui", key1->data,
	    key1->size, &s1));
	testutil_check(wiredtiger_unpack_start(session, "ui", key2->data,
	    key2->size, &s2));

	testutil_check(wiredtiger_unpack_item(s1, &skey1));
	testutil_check(wiredtiger_unpack_item(s2, &skey2));

	if ((*cmp = strcmp(skey1.data, skey2.data)) == 0)
		testutil_check(index_compare_primary(s1, s2, cmp));

	testutil_check(wiredtiger_pack_close(s1, NULL));
	testutil_check(wiredtiger_pack_close(s2, NULL));

	return (0);
}

static
int index_extractor_u(WT_EXTRACTOR *extractor, WT_SESSION *session,
    const WT_ITEM *key, const WT_ITEM *value, WT_CURSOR *result_cursor)
{
	(void)extractor;
	(void)session;
	(void)key;

	result_cursor->set_key(result_cursor, value);
	return result_cursor->insert(result_cursor);
}

static WT_COLLATOR collator_S = { index_compare_S, NULL, NULL };
static WT_COLLATOR collator_u = { index_compare_u, NULL, NULL };
static WT_EXTRACTOR extractor_u = { index_extractor_u, NULL, NULL };

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	WT_CURSOR *cursor;
	WT_ITEM item;
	WT_SESSION *session;
	int32_t i;
	int exact;
	const char *found_key;
	const char *search_key = "1234";
	const char *values[] = { "123", "12345" };

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	testutil_check(wiredtiger_open(opts->home, NULL, "create",
	    &opts->conn));
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &session));

	/*
	 * Part 1: Using a custom collator, insert some elements
	 * and verify results from search_near.
	 */
	testutil_check(opts->conn->add_collator(opts->conn, "collator_S",
	    &collator_S, NULL));

	testutil_check(session->create(session,
	    "table:main", "key_format=i,value_format=S,columns=(k,v)"));
	testutil_check(session->create(session,
	    "index:main:def_collator", "columns=(v)"));
	testutil_check(session->create(session,
	    "index:main:custom_collator",
	    "columns=(v),collator=collator_S"));

	testutil_check(session->open_cursor(session,
	    "table:main", NULL, NULL, &cursor));

	for (i = 0; i < (int32_t)(sizeof(values) / sizeof(values[0])); i++) {
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, values[i]);
		testutil_check(cursor->insert(cursor));
	}
	testutil_check(cursor->close(cursor));

	/* Check search_near in def_collator index. */
	testutil_check(session->open_cursor(session,
	    "index:main:def_collator", NULL, NULL, &cursor));
	cursor->set_key(cursor, search_key);
	testutil_check(cursor->search_near(cursor, &exact));
	testutil_check(cursor->get_key(cursor, &found_key));
	testutil_assert((strcmp(found_key, "12345") == 0 && exact > 0) ||
	    (strcmp(found_key, "123") == 0 && exact < 0));
	testutil_check(cursor->close(cursor));

	/* Check search_near in custom_collator index */
	testutil_check(session->open_cursor(session,
	    "index:main:custom_collator", NULL, NULL, &cursor));
	cursor->set_key(cursor, search_key);
	testutil_check(cursor->search_near(cursor, &exact));
	testutil_check(cursor->get_key(cursor, &found_key));
	testutil_assert((strcmp(found_key, "12345") == 0 && exact > 0) ||
	    (strcmp(found_key, "123") == 0 && exact < 0));
	testutil_check(cursor->close(cursor));

	/*
	 * Part 2: perform the same checks using a custom collator and
	 * extractor.
	 */
	testutil_check(opts->conn->add_collator(opts->conn, "collator_u",
	    &collator_u, NULL));
	testutil_check(opts->conn->add_extractor(opts->conn, "extractor_u",
	    &extractor_u, NULL));
	testutil_check(session->create(session,
	    "table:main2", "key_format=i,value_format=u,columns=(k,v)"));

	testutil_check(session->create(session, "index:main2:idx_w_coll",
	    "key_format=u,collator=collator_u,extractor=extractor_u"));

	testutil_check(session->open_cursor(session,
	    "table:main2", NULL, NULL, &cursor));

	memset(&item, 0, sizeof(item));
	for (i = 0; i < (int32_t)(sizeof(values) / sizeof(values[0])); i++) {
		item.size = strlen(values[i]) + 1;
		item.data = values[i];

		cursor->set_key(cursor, i);
		cursor->set_value(cursor, &item);
		testutil_check(cursor->insert(cursor));
	}
	testutil_check(cursor->close(cursor));

	testutil_check(session->open_cursor(session,
	    "index:main2:idx_w_coll", NULL, NULL, &cursor));
	item.data = search_key;
	item.size = sizeof(search_key);
	cursor->set_key(cursor, &item);
	testutil_check(cursor->search_near(cursor, &exact));
	testutil_check(cursor->get_key(cursor, &item));
	testutil_assert((item_str_equal(&item, "12345") && exact > 0) ||
	    (item_str_equal(&item, "123") && exact < 0));
	testutil_check(cursor->close(cursor));

	testutil_check(session->close(session, NULL));
	testutil_cleanup(opts);
	return (EXIT_SUCCESS);
}
