/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * The set of fields parsed out of a turtle blob. Each presence flag tracks
 * whether the corresponding field was found; the dump is lenient so that a
 * blob written by a newer or partly-incompatible writer still prints what we
 * can read.
 */
struct util_turtle_fields {
    bool have_metadata_lsn;
    uint64_t metadata_lsn;
    bool have_metadata_checksum;
    uint32_t metadata_checksum;
    bool have_database_size;
    uint64_t database_size;
    bool have_version;
    uint32_t version;
    bool have_compatible_version;
    uint32_t compatible_version;
};

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
 * parse_turtle --
 *     Lenient parse of the turtle blob. Unlike the production parser we do not
 *     enforce version compatibility -- a forensic tool must print whatever is
 *     on disk.
 */
static int
parse_turtle(
  WT_SESSION_IMPL *session, const char *buf, size_t buf_len, struct util_turtle_fields *out)
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    uint64_t hex_val;
    char *meta_str;

    meta_str = NULL;
    WT_CLEAR(*out);

    WT_ERR(__wt_strndup(session, buf, buf_len, &meta_str));

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "metadata_lsn", &cval), true);
    if (ret == 0 && cval.len != 0) {
        out->metadata_lsn = (uint64_t)cval.val;
        out->have_metadata_lsn = true;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "metadata_checksum", &cval), true);
    if (ret == 0 && cval.len != 0) {
        WT_ERR(__wt_conf_parse_hex(session, "metadata_checksum", &hex_val, &cval));
        if (hex_val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "metadata_checksum out of range: %" PRIx64, hex_val);
        out->metadata_checksum = (uint32_t)hex_val;
        out->have_metadata_checksum = true;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "database_size", &cval), true);
    if (ret == 0 && cval.len != 0) {
        out->database_size = (uint64_t)cval.val;
        out->have_database_size = true;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "version", &cval), true);
    if (ret == 0 && cval.len != 0) {
        if (cval.val < 0 || (uint64_t)cval.val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "version out of range: %" PRId64, cval.val);
        out->version = (uint32_t)cval.val;
        out->have_version = true;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "compatible_version", &cval), true);
    if (ret == 0 && cval.len != 0) {
        if (cval.val < 0 || (uint64_t)cval.val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "compatible_version out of range: %" PRId64, cval.val);
        out->compatible_version = (uint32_t)cval.val;
        out->have_compatible_version = true;
    }

    ret = 0;

err:
    __wt_free(session, meta_str);
    return (ret);
}

/*
 * print_turtle --
 *     Print the parsed turtle fields in key=value form. Missing fields print
 *     as <missing> so the dump shape is stable for tooling.
 */
static void
print_turtle(uint64_t lsn, const struct util_turtle_fields *t)
{
    printf("=== turtle ===\n");
    printf("lsn=%" PRIu64 "\n", lsn);
    if (t->have_metadata_lsn)
        printf("metadata_lsn=%" PRIu64 "\n", t->metadata_lsn);
    else
        printf("metadata_lsn=<missing>\n");
    if (t->have_metadata_checksum)
        printf("metadata_checksum=0x%08" PRIx32 "\n", t->metadata_checksum);
    else
        printf("metadata_checksum=<missing>\n");
    if (t->have_database_size)
        printf("database_size=%" PRIu64 "\n", t->database_size);
    else
        printf("database_size=<missing>\n");
    if (t->have_version)
        printf("version=%" PRIu32 "\n", t->version);
    else
        printf("version=<missing>\n");
    if (t->have_compatible_version)
        printf("compatible_version=%" PRIu32 "\n", t->compatible_version);
    else
        printf("compatible_version=<missing>\n");
}

/*
 * fetch_latest_turtle --
 *     Ask the connection's page log for the latest complete checkpoint. The
 *     blob ownership is transferred to the caller, which must __wt_buf_free
 *     it.
 */
static int
fetch_latest_turtle(WT_SESSION_IMPL *session, uint64_t *lsnp, WT_ITEM *meta)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_PAGE_LOG *page_log;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args;

    WT_CLEAR(args);

    conn = S2C(session);
    if (conn->disaggregated_storage.npage_log == NULL)
        WT_RET_MSG(session, EINVAL,
          "wt turtle requires a disaggregated-storage connection "
          "(-C disaggregated=(page_log=...,role=\"follower\"))");

    page_log = conn->disaggregated_storage.npage_log->page_log;
    if (page_log->pl_get_complete_checkpoint == NULL)
        WT_RET_MSG(session, ENOTSUP,
          "this page log does not implement pl_get_complete_checkpoint; "
          "pass -l <lsn> to dump the metadata page at a known LSN instead");

    /*
     * The page log may allocate into args.checkpoint_metadata before returning a
     * non-zero status. Mirror __wti_layered_get_disagg_checkpoint and free on
     * any non-success path; on success, transfer ownership to the caller.
     */
    ret = page_log->pl_get_complete_checkpoint(page_log, &session->iface, &args);
    if (ret == 0) {
        *lsnp = args.checkpoint_lsn;
        *meta = args.checkpoint_metadata;
        WT_CLEAR(args.checkpoint_metadata);
    } else
        __wt_buf_free(session, &args.checkpoint_metadata);

    return (ret);
}

/*
 * fetch_metadata_page --
 *     plh_get the shared metadata page at page_id=1 and the supplied LSN via the
 *     page log handle the connection already opened. Buffer ownership transfers
 *     to the caller. Returns WT_NOTFOUND if the page is absent at that LSN.
 */
static int
fetch_metadata_page(WT_SESSION_IMPL *session, uint64_t lsn, WT_ITEM *item)
{
    WT_CONNECTION_IMPL *conn;
    WT_PAGE_LOG_GET_ARGS get_args;
    WT_PAGE_LOG_HANDLE *plh;
    uint32_t count;

    conn = S2C(session);
    plh = conn->disaggregated_storage.page_log_meta;
    if (plh == NULL)
        WT_RET_MSG(session, EINVAL,
          "wt turtle requires a disaggregated-storage connection "
          "(-C disaggregated=(page_log=...,role=\"follower\"))");

    WT_CLEAR(get_args);
    get_args.lsn = lsn;
    count = 1;
    WT_RET(plh->plh_get(
      plh, &session->iface, WT_DISAGG_METADATA_MAIN_PAGE_ID, 0, &get_args, item, &count));
    if (count == 0)
        return (WT_NOTFOUND);
    return (0);
}

/*
 * print_metadata_page --
 *     Print the metadata page bytes verbatim. If a checksum is available from the
 *     turtle, verify and report rather than abort on mismatch.
 */
static void
print_metadata_page(
  uint64_t lsn, const WT_ITEM *page, bool have_expected_cksum, uint32_t expected_cksum)
{
    uint32_t actual;
    const char *bytes;

    printf("\n=== metadata page (table_id=2, page_id=1, lsn=%" PRIu64 ") ===\n", lsn);
    if (have_expected_cksum) {
        actual = __wt_checksum(page->data, page->size);
        if (actual == expected_cksum)
            printf("checksum=OK\n");
        else
            printf("checksum=MISMATCH (expected=0x%08" PRIx32 ", got=0x%08" PRIx32 ")\n",
              expected_cksum, actual);
    }
    bytes = (const char *)page->data;
    fwrite(bytes, 1, page->size, stdout);
    if (page->size == 0 || bytes[page->size - 1] != '\n')
        fputc('\n', stdout);
}

/*
 * util_turtle --
 *     The turtle command: dump the disaggregated-storage turtle blob (and chase to the
 *     shared metadata page it points at).
 */
int
util_turtle(WT_SESSION *session, int argc, char *argv[])
{
    WT_DECL_RET;
    WT_ITEM meta, page_item;
    WT_SESSION_IMPL *session_impl;
    struct util_turtle_fields fields;
    uint64_t lsn, lsn_arg;
    int ch;
    bool have_lsn_arg, suppress_util_err;

    session_impl = (WT_SESSION_IMPL *)session;
    WT_CLEAR(meta);
    WT_CLEAR(page_item);
    lsn = 0;
    lsn_arg = 0;
    have_lsn_arg = false;
    suppress_util_err = false;

    while ((ch = __wt_getopt(progname, argc, argv, "l:?")) != EOF)
        switch (ch) {
        case 'l':
            if (util_str2num(session, __wt_optarg, true, &lsn_arg) != 0)
                return (usage());
            if (lsn_arg == 0)
                return (usage());
            have_lsn_arg = true;
            break;
        case '?':
            usage();
            return (0);
        default:
            return (usage());
        }
    argc -= __wt_optind;
    argv += __wt_optind;

    if (argc != 0)
        return (usage());

    if (have_lsn_arg) {
        ret = fetch_metadata_page(session_impl, lsn_arg, &page_item);
        if (ret == WT_NOTFOUND) {
            printf("metadata page not found at lsn=%" PRIu64 " (may have been pruned)\n", lsn_arg);
            ret = 1;
            suppress_util_err = true;
        } else if (ret == 0) {
            print_metadata_page(
              lsn_arg, &page_item, /* have_expected_cksum */ false, /* expected_cksum */ 0);
        }
        goto err;
    }

    ret = fetch_latest_turtle(session_impl, &lsn, &meta);
    if (ret == WT_NOTFOUND) {
        printf("no complete checkpoint yet\n");
        ret = 0;
        goto err;
    }
    WT_ERR(ret);

    WT_ERR(parse_turtle(session_impl, (const char *)meta.data, meta.size, &fields));
    print_turtle(lsn, &fields);

    if (fields.have_metadata_lsn) {
        ret = fetch_metadata_page(session_impl, fields.metadata_lsn, &page_item);
        if (ret == WT_NOTFOUND) {
            printf("\nmetadata page not found at lsn=%" PRIu64 " (may have been pruned)\n",
              fields.metadata_lsn);
            ret = 1;
            suppress_util_err = true;
        } else if (ret == 0) {
            print_metadata_page(fields.metadata_lsn, &page_item, fields.have_metadata_checksum,
              fields.metadata_checksum);
        }
        WT_ERR(ret);
    }

err:
    __wt_buf_free(session_impl, &meta);
    __wt_buf_free(session_impl, &page_item);
    if (ret != 0 && !suppress_util_err)
        (void)util_err(session, ret, "turtle");
    return (ret == 0 ? 0 : 1);
}
