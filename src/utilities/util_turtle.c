/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * usage --
 *     Display a usage message for the turtle command.
 */
static int
usage(void)
{
    static const char *options[] = {"-l lsn",
      "optional: dump the shared metadata page at this LSN instead of the latest turtle "
      "(decimal or 0x-prefixed hex)",
      "-?", "show this message", NULL, NULL};

    util_usage("turtle [-l lsn]", "options:", options);
    return (1);
}

/*
 * util_turtle --
 *     The turtle command: dump the disaggregated-storage turtle blob (and chase to the
 *     shared metadata page it points at).
 */
int
util_turtle(WT_SESSION *session, int argc, char *argv[])
{
    int ch;

    while ((ch = __wt_getopt(progname, argc, argv, "l:?")) != EOF)
        switch (ch) {
        case 'l':
            break;
        case '?':
            usage();
            return (0);
        default:
            return (usage());
        }

    (void)session;
    fprintf(stderr, "%s: turtle: not implemented yet\n", progname);
    return (1);
}
