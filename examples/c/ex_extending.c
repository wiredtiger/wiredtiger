/*-
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
 * ex_extending.c
 *	This is an example demonstrating ways to extend WiredTiger with
 *	collators and discard filters.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <wiredtiger.h>

const char *home;

/*! [case insensitive comparator] */
/* A simple case insensitive comparator. */
static int
__compare_nocase(WT_COLLATOR *collator, WT_SESSION *session,
    const WT_ITEM *v1, const WT_ITEM *v2, int *cmp)
{
	const char *s1 = (const char *)v1->data;
	const char *s2 = (const char *)v2->data;

	(void)session; /* unused */
	(void)collator; /* unused */

	*cmp = strcasecmp(s1, s2);
	return (0);
}

static WT_COLLATOR nocasecoll = { __compare_nocase, NULL, NULL };
/*! [case insensitive comparator] */

/*! [monthly discard filter] */
/* A simple monthly discard filter. */
static int
__filter_monthly(WT_DISCARD_FILTER *filter,
    WT_SESSION *session, const WT_ITEM *key, int *removep)
{
	struct tm *today;
	time_t now;

	/* Unused parameters */
	(void)filter;
	(void)session;

	(void)time(&now);
	if ((today = localtime(&now)) == NULL)
		return (1);

	/*
	 * The first byte of the key is the month as an integer, 1-12.
	 * The localtime representation of the month is 0-11, requires
	 * correction.
	 */
	*removep = *(u_char *)key->data == today->tm_mon - 1 ? 0 : 1;
	return (0);
}

static WT_DISCARD_FILTER monthly_filter = { __filter_monthly, NULL };
/*! [monthly discard filter] */

/*! [n character comparator] */
/*
 * Comparator that only compares the first N prefix characters of the string.
 * This has associated data, so we need to extend WT_COLLATOR.
 */
typedef struct {
	WT_COLLATOR iface;
	uint32_t maxlen;
} PREFIX_COLLATOR;

static int
__compare_prefixes(WT_COLLATOR *collator, WT_SESSION *session,
    const WT_ITEM *v1, const WT_ITEM *v2, int *cmp)
{
	PREFIX_COLLATOR *pcoll = (PREFIX_COLLATOR *)collator;
	const char *s1 = (const char *)v1->data;
	const char *s2 = (const char *)v2->data;

	(void)session; /* unused */

	*cmp = strncmp(s1, s2, pcoll->maxlen);
	return (0);
}

static PREFIX_COLLATOR pcoll10 = { {__compare_prefixes, NULL, NULL}, 10 };
/*! [n character comparator] */

int main(void)
{
	int ret;
	WT_CONNECTION *conn;
	WT_SESSION *session;

	/*
	 * Create a clean test directory for this run of the test program if the
	 * environment variable isn't already set (as is done by make check).
	 */
	if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
	} else
		home = NULL;

	/* Open a connection to the database, creating it if necessary. */
	if ((ret = wiredtiger_open(home, NULL, "create", &conn)) != 0)
		fprintf(stderr, "Error connecting to %s: %s\n",
		    home, wiredtiger_strerror(ret));

	/*! [add collator] */
	ret = conn->add_collator(conn, "nocase", &nocasecoll, NULL);
	/*! [add collator] */

	/*! [add discard filter] */
	ret = conn->add_discard_filter(conn, "monthly", &monthly_filter, NULL);
	/*! [add discard filter] */

	ret = conn->add_collator(conn, "prefix10", &pcoll10.iface, NULL);

	/* Open a session for the current thread's work. */
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		fprintf(stderr, "Error opening a session on %s: %s\n",
		    home, wiredtiger_strerror(ret));

	/* XXX Do some work... */

	/* Note: closing the connection implicitly closes open session(s). */
	if ((ret = conn->close(conn, NULL)) != 0)
		fprintf(stderr, "Error connecting to %s: %s\n",
		    home, wiredtiger_strerror(ret));

	return (ret);
}
