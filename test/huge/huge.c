/*-
 * Public Domain 2008-2013 WiredTiger, Inc.
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

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <wiredtiger.h>

const char *progname;				/* Program name */
uint8_t *big;					/* Big key/value buffer */

#define	GIGABYTE	(1073741824)
#define	MEGABYTE	(1048576)

/*
 * List of configurations we test.
 */
typedef	struct {
	const char *uri;			/* Object URI */
	const char *config;			/* Object configuration */
	int recno;				/* Column-store key */
} CONFIG;

static CONFIG config[] = {
	{ "file:xxx", "key_format=S,value_format=S", 0 },
	{ "file:xxx", "key_format=r,value_format=S", 1 },
	{ "lsm:xxx", "key_format=S,value_format=S", 0 },
	{ "table:xxx", "key_format=S,value_format=S", 0 },
	{ "table:xxx", "key_format=r,value_format=S", 1 },
	{ NULL, NULL, 0 }
};

static size_t lengths[] = {
    20,					/* Check configuration */
    (size_t)1 * MEGABYTE,		/* 1MB */
    (size_t)250 * MEGABYTE,		/* 250MB (largest -s configuration) */
    (size_t)1 * GIGABYTE,		/* 1GB */
    (size_t)2 * GIGABYTE,		/* 2GB */
    (size_t)3 * GIGABYTE,		/* 3GB */
    ((size_t)4 * GIGABYTE) - MEGABYTE,	/* Roughly the max we can handle */
    0
};

static void
usage(void)
{
	fprintf(stderr, "usage: %s [-s]\n", progname);
	fprintf(stderr, "%s",
	    "\t-s small run, only test up to 1GB\n");

	exit(EXIT_FAILURE);
}

static void
die(int e, const char *fmt, ...)
{
	va_list ap;

	if (fmt != NULL) {				/* Death message. */
		fprintf(stderr, "%s: ", progname);
		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);
		if (e != 0)
			fprintf(stderr, ": %s", wiredtiger_strerror(e));
		fprintf(stderr, "\n");
	}
	exit(EXIT_FAILURE);
}

static void
run(CONFIG *cp, int bigkey, size_t bytes)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_CURSOR *cursor;
	uint64_t keyno;
	int ret;
	void *p;

	big[bytes] = '\0';

	printf("%zu" "%s%s: %s %s big %s\n",
	    bytes < MEGABYTE ? bytes :
	    (bytes < GIGABYTE ? bytes / MEGABYTE : bytes / GIGABYTE),
	    bytes < MEGABYTE ? "" :
	    (bytes < GIGABYTE ?
	    (bytes % MEGABYTE == 0 ? "" : "+") :
	    (bytes % GIGABYTE == 0 ? "" : "+")),
	    bytes < MEGABYTE ? "B" : (bytes < GIGABYTE ? "MB" : "GB"),
	    cp->uri, cp->config, bigkey ? "key" : "value");

	(void)system("rm -rf WT_TEST && mkdir WT_TEST");

	/*
	 * Open/create the database, connection, session and cursor; set the
	 * cache size large, we don't want to try and evict anything.
	 */
	if ((ret = wiredtiger_open(
	    "WT_TEST", NULL, "create,cache_size=10GB", &conn)) != 0)
		die(ret, "wiredtiger_open");
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "WT_CONNECTION.open_session");
	if ((ret = session->create(session, cp->uri, cp->config)) != 0)
		die(ret, "WT_SESSION.create: %s %s", cp->uri, cp->config);
	if ((ret =
	    session->open_cursor(session, cp->uri, NULL, NULL, &cursor)) != 0)
		die(ret, "WT_SESSION.open_cursor: %s", cp->uri);

	/* Set the key/value. */
	if (bigkey) {
		cursor->set_key(cursor, big);
		cursor->set_value(cursor, big);
	} else {
		if (cp->recno) {
			keyno = 1;
			cursor->set_key(cursor, keyno);
		} else
			cursor->set_key(cursor, "key001");
		cursor->set_value(cursor, big);
	}

	/* Insert the record. */
	if ((ret = cursor->insert(cursor)) != 0)
		die(ret, "WT_CURSOR.insert");

	/* Retrieve the record and check it. */
	if ((ret = cursor->search(cursor)) != 0)
		die(ret, "WT_CURSOR.search");
	if (bigkey && (ret = cursor->get_key(cursor, &p)) != 0)
		die(ret, "WT_CURSOR.get_key");
	if ((ret = cursor->get_value(cursor, &p)) != 0)
		die(ret, "WT_CURSOR.get_value");
	if (memcmp(p, big, bytes) != 0)
		die(0, "retrieved big key/value item did not match original");

	/* Remove the record. */
	if ((ret = cursor->remove(cursor)) != 0)
		die(ret, "WT_CURSOR.remove");

	if ((ret = conn->close(conn, NULL)) != 0)
		die(ret, "WT_CONNECTION.close");

	big[bytes] = 'a';
}

int
main(int argc, char *argv[])
{
	CONFIG *cp;
	size_t len, *lp;
	int ch, small;

	if ((progname = strrchr(argv[0], '/')) == NULL)
		progname = argv[0];
	else
		++progname;

	small = 0;
	while ((ch = getopt(argc, argv, "s")) != EOF)
		switch (ch) {
		case 's':			/* Gigabytes */
			small = 1;
			break;
		default:
			usage();
		}

	/* Allocate a buffer to use. */
	len = (size_t)4 * GIGABYTE;
	if ((big = malloc(len)) == NULL)
		die(errno, "");
	memset(big, 'a', len);

	/* Make sure the configurations all work. */
	for (lp = lengths; *lp != 0; ++lp) {
		if (small && *lp >= GIGABYTE)
			break;
		for (cp = config; cp->uri != NULL; ++cp) {
			if (!cp->recno)		/* Big key on row-store */
				run(cp, 1, *lp);
			run(cp, 0, *lp);	/* Big value */
		}
	}

	(void)system("rm -rf WT_TEST");

	return (EXIT_SUCCESS);
}
