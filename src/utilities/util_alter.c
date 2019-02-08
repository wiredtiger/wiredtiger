/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int usage(void);

int
util_alter(WT_SESSION *session, int argc, char *argv[])
{
	WT_DECL_RET;
	int ch;
	char **configp;

	while ((ch = __wt_getopt(progname, argc, argv, "")) != EOF)
		switch (ch) {
		case '?':
		default:
			return (usage());
		}

	argc -= __wt_optind;
	argv += __wt_optind;

	/* The remaining arguments are uri/string pairs. */
	if (argc % 2 != 0)
		return (usage());

	for (configp = argv; *configp != NULL; configp += 2)
		if ((ret = session->alter(
		    session, configp[0], configp[1])) != 0) {
			(void)util_err(session, ret,
			    "session.alter: %s, %s", configp[0], configp[1]);
			return (1);
		}
	return (0);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "alter uri configuration ...\n",
	    progname, usage_prefix);
	return (1);
}
