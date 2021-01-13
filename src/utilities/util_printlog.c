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
    // int start_lsn = 0;
    WT_LSN start_lsn;
    WT_LSN end_lsn;
    uint32_t start_lsnfile;
    uint32_t start_lsnoffset;
    uint32_t end_lsnfile;
    uint32_t end_lsnoffset;
    char *end_str;
    char *ofile;
    char *start_str;
    int n_args;

    flags = 0;
    ofile = NULL;
    WT_INIT_LSN(&start_lsn);
    WT_INIT_LSN(&end_lsn);
    WT_ZERO_LSN(&end_lsn);
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
            // start_str = __wt_optarg;
            // // start_lsn = atoi(start_str);
            // if (sscanf(start_str, "%" SCNu32 ",%" SCNu32, &start_lsnfile, &start_lsnoffset) == 2)
            //     WT_SET_LSN(&start_lsn, start_lsnfile, start_lsnoffset);
            //     printf("sscanf succeed\n");
            //     printf("sscanf read in %" PRIu32 ", %" PRIu32 "\n",start_lsnfile, start_lsnoffset);
            // printf("Start option file: %" PRIu32 ", offset: %" PRIu32 "\n", start_lsn.l.file, start_lsn.l.offset);

            start_str = __wt_optarg;
            n_args = sscanf(start_str, "%" SCNu32 ",%" SCNu32 ",%" SCNu32 ",%" SCNu32, &start_lsnfile, &start_lsnoffset, &end_lsnfile, &end_lsnoffset);
            if (n_args == 2){
                WT_SET_LSN(&start_lsn, start_lsnfile, start_lsnoffset);
            } else if (n_args == 4){
                WT_SET_LSN(&start_lsn, start_lsnfile, start_lsnoffset);
                WT_SET_LSN(&end_lsn, end_lsnfile, end_lsnoffset);
            } else {
                printf("Invalid start/end\n");
            }
            break;
        case 'e':
            end_str = __wt_optarg;
            if (sscanf(end_str, "%" SCNu32 ",%" SCNu32, &end_lsnfile, &end_lsnoffset) == 2)
                WT_SET_LSN(&end_lsn, end_lsnfile, end_lsnoffset);
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
