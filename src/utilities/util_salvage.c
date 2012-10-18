/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int usage(void);

int
util_salvage(WT_SESSION *session, int argc, char *argv[])
{
	WT_DECL_RET;
	int ch;
	const char *force;
	char *name;

	force = NULL;
	name = NULL;
	while ((ch = util_getopt(argc, argv, "F")) != EOF)
		switch (ch) {
		case 'F':
			force = "force";
			break;
		case '?':
		default:
			return (usage());
		}
	argc -= util_optind;
	argv += util_optind;

	/* The remaining argument is the file name. */
	if (argc != 1)
		return (usage());
	if ((name = util_name(*argv, "file", UTIL_FILE_OK)) == NULL)
		return (1);

	if ((ret = session->salvage(session, name, force)) != 0) {
		fprintf(stderr, "%s: salvage(%s): %s\n",
		    progname, name, wiredtiger_strerror(ret));
		goto err;
	}

	/* Verbose configures a progress counter, move to the next line. */
	if (verbose)
		printf("\n");

	if (0) {
err:		ret = 1;
	}

	if (name != NULL)
		free(name);

	return (ret);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "salvage [-F] uri\n",
	    progname, usage_prefix);
	return (1);
}
