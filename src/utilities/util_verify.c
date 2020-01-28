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
util_verify(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    size_t size;
    int ch;
    char *config, *dump_offsets, *uri;
    bool dump_address, dump_blocks, dump_layout, dump_pages, hs_verify, stable_timestamp;

    dump_address = dump_blocks = dump_layout = dump_pages = hs_verify = stable_timestamp = false;
    config = dump_offsets = uri = NULL;
    while ((ch = __wt_getopt(progname, argc, argv, "d:S:h")) != EOF)
        switch (ch) {
        case 'd':
            if (strcmp(__wt_optarg, "dump_address") == 0)
                dump_address = true;
            else if (strcmp(__wt_optarg, "dump_blocks") == 0)
                dump_blocks = true;
            else if (strcmp(__wt_optarg, "dump_history") == 0)
                dump_history = true;
            else if (strcmp(__wt_optarg, "dump_layout") == 0)
                dump_layout = true;
            else if (WT_PREFIX_MATCH(__wt_optarg, "dump_offsets=")) {
                if (dump_offsets != NULL) {
                    fprintf(stderr,
                      "%s: only a single 'dump_offsets' "
                      "argument supported\n",
                      progname);
                    return (usage());
                }
                dump_offsets = __wt_optarg + strlen("dump_offsets=");
            } else if (strcmp(__wt_optarg, "dump_pages") == 0)
                dump_pages = true;
            else
                return (usage());
            break;
        case 'S':
            stable_timestamp = true;
            break;
        case 'h':
            hs_verify = true;
            break;
        case '?':
        default:
            return (usage());
        }
    argc -= __wt_optind;
    argv += __wt_optind;


    /* The remaining argument is the table name. */
    if ((argc != 1 && !hs_verify) || (hs_verify && argc != 0))
        return (usage());
    if (hs_verify == false && ((uri = util_uri(session, *argv, "table")) == NULL))
        return (1);
    else if ((uri = util_uri(session, WT_HS_URI, "table")) == NULL) {
        return (1);
    }
    
    if (hs_verify && (dump_address || dump_blocks || 
        dump_layout || dump_offsets != NULL || dump_pages)) {
        (void)util_err(session, 69, "-d flags given on non -d call");
    }
    /* Build the configuration string as necessary. */
    if (hs_verify) {
        size = strlen("hs_verify")+1;
        // Copy pasted code, helper fnc? 
        if ((config = malloc(size)) == NULL) {
            ret = util_err(session, errno, NULL);
            goto err;
        }
        if ((ret = __wt_snprintf(config, size, "%s", "hs_verify" ))) {
            (void)util_err(session, ret, NULL);
            goto err;
        }
    }
    
    else if (dump_address || dump_blocks || dump_layout || dump_offsets != NULL || dump_pages) {
        size = strlen("dump_address,") + strlen("dump_blocks,") + strlen("dump_layout,") +
          strlen("dump_pages,") + strlen("dump_offsets[],") +
          (dump_offsets == NULL ? 0 : strlen(dump_offsets)) + 20;
        if ((config = malloc(size)) == NULL) {
            ret = util_err(session, errno, NULL);
            goto err;
        }
        if ((ret = __wt_snprintf(config, size, "%s%s%s%s%s%s%s%s%s",
               dump_address ? "dump_address," : "", dump_blocks ? "dump_blocks," : "",
               dump_history ? "dump_history," : "", dump_layout ? "dump_layout," : "",
               dump_offsets != NULL ? "dump_offsets=[" : "",
               dump_offsets != NULL ? dump_offsets : "", dump_offsets != NULL ? "]," : "",
               dump_pages ? "dump_pages," : "", stable_timestamp ? "stable_timestamp," : "")) !=
          0) {
            (void)util_err(session, ret, NULL);
            goto err;
        }
    }
    if ((ret = session->verify(session, uri, config)) != 0)
        (void)util_err(session, ret, "session.verify: %s", uri);
    else {
        /*
         * Verbose configures a progress counter, move to the next line.
         */
        if (verbose)
            printf("\n");
    }

err:
    free(config);
    free(uri);
    return (ret);
}

static int
usage(void)
{
    (void)fprintf(stderr,
      "usage: %s %s "
      "verify %s\n",
      progname, usage_prefix,
      "[-d dump_address | dump_blocks | dump_history | dump_layout | "
      "dump_offsets=#,# | dump_pages] [-S] uri");
    return (1);
}
