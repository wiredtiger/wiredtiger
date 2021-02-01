/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int
usage(void)
{
    static const char *options[] = {"-f", "output to the specified file", "-x",
      "display key and value items in hexadecimal format", NULL, NULL};

    util_usage("printlog [-mux] [-f output-file]", "options:", options);
    return (1);
}

int
util_printlog(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    uint32_t flags;
    int ch;
    char *ofile;

    flags = 0;
    ofile = NULL;
    /*
     * By default redact user data. This way if any support people are using this on customer data,
     * it is redacted unless they make the effort to keep it in. It lessens the risk of doing the
     * wrong command.
     */
    LF_SET(WT_TXN_PRINTLOG_REDACT);
    while ((ch = __wt_getopt(progname, argc, argv, "f:mux")) != EOF)
        switch (ch) {
        case 'f': /* output file */
            ofile = __wt_optarg;
            break;
        case 'm': /* messages only */
            LF_SET(WT_TXN_PRINTLOG_MSG);
            break;
        case 'u': /* print user data, don't redact */
            LF_CLR(WT_TXN_PRINTLOG_REDACT);
            break;
        case 'x': /* hex output */
            LF_SET(WT_TXN_PRINTLOG_HEX);
            break;
        case '?':
        default:
            return (usage());
        }
    argc -= __wt_optind;

    /* There should not be any more arguments. */
    if (argc != 0)
        return (usage());

    if ((ret = __wt_txn_printlog(session, ofile, flags)) != 0)
        (void)util_err(session, ret, "printlog");

    return (ret);
}
