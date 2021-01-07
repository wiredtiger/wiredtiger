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

    util_usage("printlog [-x] [-f output-file]", "options:", options);
    return (1);
}

int
util_printlog(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    uint32_t flags;
    int ch;
    int end_lsn = 999999;
    int start_lsn = 0;
    char *end_str;
    char *ofile;
    char *start_str;

    flags = 0;
    ofile = NULL;
    while ((ch = __wt_getopt(progname, argc, argv, "f:e:s:mx")) != EOF)
        switch (ch) {
        case 'f': /* output file */
            ofile = __wt_optarg;
            printf("Outfile: %s\n", ofile);
            break;
        case 'm': /* messages only */
            LF_SET(WT_TXN_PRINTLOG_MSG);
            break;
        case 'x': /* hex output */
            LF_SET(WT_TXN_PRINTLOG_HEX);
            break;
        case 's':
            start_str = __wt_optarg;
            start_lsn = atoi(start_str);
            printf("Start option int: %d\n", start_lsn);
            break;
        case 'e':
            end_str = __wt_optarg;
            end_lsn = atoi(end_str);
            printf("End option int: %d\n", end_lsn);
            break;
        case '?':
        default:
            return (usage());
        }
    argc -= __wt_optind;

    /* There should not be any more arguments. */
    if (argc != 0)
        return (usage());

    if ((ret = __wt_txn_printlog(session, ofile, flags, start_lsn, end_lsn)) != 0)
        (void)util_err(session, ret, "printlog");

    return (ret);
}
