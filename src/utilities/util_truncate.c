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
 *     Display a usage message for the truncate command.
 */
int
usage_truncate(void)
{
    static const char *options[] = {"-?", "show this message", NULL, NULL};

    util_usage("truncate uri", "options:", options);
    return (1);
}

/*
 * util_truncate --
 *     The truncate command.
 */
int
util_truncate(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    int ch;
    char *uri;

    uri = NULL;
    while ((ch = __wt_getopt(progname, argc, argv, "?")) != EOF)
        switch (ch) {
        case '?':
            usage_truncate();
            return (0);
        default:
            return (usage_truncate());
        }

    argc -= __wt_optind;
    argv += __wt_optind;

    /* The remaining argument is the uri. */
    if (argc != 1)
        return (usage_truncate());
    if ((uri = util_uri(session, *argv, "table")) == NULL)
        return (1);

    if ((ret = session->truncate(session, uri, NULL, NULL, NULL)) != 0)
        (void)util_err(session, ret, "session.truncate: %s", uri);

    util_free(uri);
    return (ret);
}
