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
 *     Display a usage message for the alter command.
 */
int
usage_alter(void)
{
    static const char *options[] = {"-?", "show this message", NULL, NULL};

    util_usage("alter uri configuration ...", "options:", options);
    return (1);
}

/*
 * util_alter --
 *     The alter command.
 */
int
util_alter(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    int ch;
    char **configp;

    while ((ch = __wt_getopt(progname, argc, argv, "?")) != EOF)
        switch (ch) {
        case '?':
            usage_alter();
            return (0);
        default:
            return (usage_alter());
        }

    argc -= __wt_optind;
    argv += __wt_optind;

    /* The remaining arguments are uri/string pairs. */
    if (argc == 0 || argc % 2 != 0)
        return (usage_alter());

    for (configp = argv; *configp != NULL; configp += 2)
        if ((ret = session->alter(session, configp[0], configp[1])) != 0) {
            (void)util_err(session, ret, "session.alter: %s, %s", configp[0], configp[1]);
            return (1);
        }
    return (0);
}
