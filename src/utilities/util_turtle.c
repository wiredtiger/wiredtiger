/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * WT_DISAGG_CHECKPOINT_META is the production carrier for the disagg turtle blob's fields, and
 * already tracks metadata_checksum presence. The other four fields are required by the production
 * parser and have no presence flags there; the forensic dump must report them as <missing> when
 * absent, so track presence on the side.
 */
#define WT_TURTLE_HAS_METADATA_LSN 0x1u
#define WT_TURTLE_HAS_DATABASE_SIZE 0x2u
#define WT_TURTLE_HAS_VERSION 0x4u
#define WT_TURTLE_HAS_COMPATIBLE_VERSION 0x8u

/*
 * usage --
 *     Display a usage message for the turtle command.
 */
static int
usage(void)
{
    static const char *options[] = {"-l lsn",
      "disaggregated-storage only: dump the shared metadata page at this LSN instead of the "
      "latest turtle (decimal or 0x-prefixed hex)",
      "-?", "show this message", NULL, NULL};

    util_usage("turtle [-l lsn]", "options:", options);
    return (1);
}

/*
 * parse_turtle --
 *     Lenient parse of the disagg turtle blob. Unlike __wti_disagg_pick_up_checkpoint_meta, this
 *     does not enforce that required fields are present nor that the version is compatible; a
 *     forensic tool must print whatever is on disk.
 */
static int
parse_turtle(WT_SESSION_IMPL *session, const char *buf, size_t buf_len,
  WT_DISAGG_CHECKPOINT_META *metap, uint32_t *presentp)
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    uint64_t hex_val;
    char *meta_str;

    meta_str = NULL;
    WT_CLEAR(*metap);
    *presentp = 0;

    WT_ERR(__wt_strndup(session, buf, buf_len, &meta_str));

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "metadata_lsn", &cval), true);
    if (ret == 0 && cval.len != 0) {
        metap->metadata_lsn = (uint64_t)cval.val;
        *presentp |= WT_TURTLE_HAS_METADATA_LSN;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "metadata_checksum", &cval), true);
    if (ret == 0 && cval.len != 0) {
        WT_ERR(__wt_conf_parse_hex(session, "metadata_checksum", &hex_val, &cval));
        if (hex_val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "metadata_checksum out of range: %" PRIx64, hex_val);
        metap->metadata_checksum = (uint32_t)hex_val;
        metap->has_metadata_checksum = true;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "database_size", &cval), true);
    if (ret == 0 && cval.len != 0) {
        metap->database_size = (uint64_t)cval.val;
        *presentp |= WT_TURTLE_HAS_DATABASE_SIZE;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "version", &cval), true);
    if (ret == 0 && cval.len != 0) {
        if (cval.val < 0 || (uint64_t)cval.val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "version out of range: %" PRId64, cval.val);
        metap->version = (uint32_t)cval.val;
        *presentp |= WT_TURTLE_HAS_VERSION;
    }

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, meta_str, "compatible_version", &cval), true);
    if (ret == 0 && cval.len != 0) {
        if (cval.val < 0 || (uint64_t)cval.val > UINT32_MAX)
            WT_ERR_MSG(session, EINVAL, "compatible_version out of range: %" PRId64, cval.val);
        metap->compatible_version = (uint32_t)cval.val;
        *presentp |= WT_TURTLE_HAS_COMPATIBLE_VERSION;
    }

    ret = 0;

err:
    __wt_free(session, meta_str);
    return (ret);
}

/*
 * print_turtle --
 *     Print the parsed turtle fields in key=value form. Missing fields print as <missing> so the
 *     dump shape is stable for tooling.
 */
static void
print_turtle(uint64_t lsn, const WT_DISAGG_CHECKPOINT_META *meta, uint32_t present)
{
    printf("=== turtle ===\n");
    printf("lsn=%" PRIu64 "\n", lsn);
    if (present & WT_TURTLE_HAS_METADATA_LSN)
        printf("metadata_lsn=%" PRIu64 "\n", meta->metadata_lsn);
    else
        printf("metadata_lsn=<missing>\n");
    if (meta->has_metadata_checksum)
        printf("metadata_checksum=0x%08" PRIx32 "\n", meta->metadata_checksum);
    else
        printf("metadata_checksum=<missing>\n");
    if (present & WT_TURTLE_HAS_DATABASE_SIZE)
        printf("database_size=%" PRIu64 "\n", meta->database_size);
    else
        printf("database_size=<missing>\n");
    if (present & WT_TURTLE_HAS_VERSION)
        printf("version=%" PRIu32 "\n", meta->version);
    else
        printf("version=<missing>\n");
    if (present & WT_TURTLE_HAS_COMPATIBLE_VERSION)
        printf("compatible_version=%" PRIu32 "\n", meta->compatible_version);
    else
        printf("compatible_version=<missing>\n");
}

/*
 * fetch_latest_turtle --
 *     Ask the connection's page log for the latest complete checkpoint. The blob ownership is
 *     transferred to the caller, which must __wt_buf_free it.
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
     * The page log may allocate into args.checkpoint_metadata before returning a non-zero status.
     * Mirror __wti_layered_get_disagg_checkpoint and free on any non-success path; on success,
     * transfer ownership to the caller.
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
 *     plh_get the shared metadata page at page_id=1 and the supplied LSN via the page log handle
 *     the connection already opened. Buffer ownership transfers to the caller. Returns WT_NOTFOUND
 *     if the page is absent at that LSN.
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
 *     Print the metadata page bytes verbatim. If a checksum is available from the turtle, verify
 *     and report rather than abort on mismatch. Returns true on detected mismatch so the caller can
 *     surface a non-zero exit; the forensic mode passes have_expected_cksum=false and so never
 *     returns true.
 */
static bool
print_metadata_page(
  uint64_t lsn, const WT_ITEM *page, bool have_expected_cksum, uint32_t expected_cksum)
{
    uint32_t actual;
    const char *bytes;
    bool mismatch;

    mismatch = false;
    printf("\n=== metadata page (table_id=2, page_id=1, lsn=%" PRIu64 ") ===\n", lsn);
    if (have_expected_cksum) {
        actual = __wt_checksum(page->data, page->size);
        if (actual == expected_cksum)
            printf("checksum=OK\n");
        else {
            printf("checksum=MISMATCH (expected=0x%08" PRIx32 ", got=0x%08" PRIx32 ")\n",
              expected_cksum, actual);
            mismatch = true;
        }
    }
    bytes = (const char *)page->data;
    fwrite(bytes, 1, page->size, stdout);
    if (page->size == 0 || bytes[page->size - 1] != '\n')
        fputc('\n', stdout);
    return (mismatch);
}

/*
 * dump_asc_turtle --
 *     Dump the on-disk WiredTiger.turtle file from the home directory verbatim. Used when the
 *     connection has no disaggregated-storage page log; the turtle here is a local text file rather
 *     than a checkpoint metadata blob in the page log.
 */
static int
dump_asc_turtle(WT_SESSION_IMPL *session)
{
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_FSTREAM *fs;
    bool exist;

    fs = NULL;

    WT_RET(__wt_fs_exist(session, WT_METADATA_TURTLE, &exist));
    if (!exist) {
        printf("no %s in home directory\n", WT_METADATA_TURTLE);
        return (0);
    }

    WT_RET(__wt_fopen(session, WT_METADATA_TURTLE, 0, WT_STREAM_READ, &fs));
    WT_ERR(__wt_scr_alloc(session, 1024, &buf));

    printf("=== %s ===\n", WT_METADATA_TURTLE);
    for (;;) {
        WT_ERR(__wt_getline(session, fs, buf));
        if (buf->size == 0)
            break;
        printf("%s\n", (const char *)buf->data);
    }

err:
    WT_TRET(__wt_fclose(session, &fs));
    __wt_scr_free(session, &buf);
    return (ret);
}

/*
 * util_turtle --
 *     The turtle command. For a disaggregated-storage connection, dump the turtle blob from the
 *     page log and chase to the shared metadata page. For a plain (attached-storage) connection,
 *     dump the local WiredTiger.turtle file.
 */
int
util_turtle(WT_SESSION *session, int argc, char *argv[])
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_DISAGG_CHECKPOINT_META turtle_meta;
    WT_ITEM meta_blob, page_item;
    WT_SESSION_IMPL *session_impl;
    uint64_t lsn, lsn_arg;
    uint32_t present;
    int ch;
    bool have_lsn_arg, suppress_util_err;

    session_impl = (WT_SESSION_IMPL *)session;
    conn = S2C(session_impl);
    WT_CLEAR(turtle_meta);
    WT_CLEAR(meta_blob);
    WT_CLEAR(page_item);
    present = 0;
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

    if (argc != 0)
        return (usage());

    /*
     * Route on whether the connection has a disaggregated-storage page log. ASC homes have no page
     * log and store the turtle locally; -l is page-log-only.
     */
    if (conn->disaggregated_storage.npage_log == NULL) {
        if (have_lsn_arg) {
            fprintf(
              stderr, "%s: turtle: -l requires a disaggregated-storage connection\n", progname);
            return (usage());
        }
        ret = dump_asc_turtle(session_impl);
        if (ret != 0)
            (void)util_err(session, ret, "turtle");
        return (ret == 0 ? 0 : 1);
    }

    if (have_lsn_arg) {
        ret = fetch_metadata_page(session_impl, lsn_arg, &page_item);
        if (ret == WT_NOTFOUND) {
            printf("metadata page not found at lsn=%" PRIu64 " (may have been pruned)\n", lsn_arg);
            ret = 1;
            suppress_util_err = true;
        } else if (ret == 0) {
            (void)print_metadata_page(
              lsn_arg, &page_item, /* have_expected_cksum */ false, /* expected_cksum */ 0);
        }
        goto err;
    }

    ret = fetch_latest_turtle(session_impl, &lsn, &meta_blob);
    if (ret == WT_NOTFOUND) {
        printf("no complete checkpoint yet\n");
        ret = 0;
        goto err;
    }
    WT_ERR(ret);

    WT_ERR(parse_turtle(
      session_impl, (const char *)meta_blob.data, meta_blob.size, &turtle_meta, &present));
    print_turtle(lsn, &turtle_meta, present);

    if (present & WT_TURTLE_HAS_METADATA_LSN) {
        ret = fetch_metadata_page(session_impl, turtle_meta.metadata_lsn, &page_item);
        if (ret == WT_NOTFOUND) {
            printf(
              "\n"
              "metadata page not found at lsn=%" PRIu64 " (may have been pruned)\n",
              turtle_meta.metadata_lsn);
            ret = 1;
            suppress_util_err = true;
        } else if (ret == 0) {
            if (print_metadata_page(turtle_meta.metadata_lsn, &page_item,
                  turtle_meta.has_metadata_checksum, turtle_meta.metadata_checksum)) {
                ret = 1;
                suppress_util_err = true;
            }
        }
        WT_ERR(ret);
    }

err:
    __wt_buf_free(session_impl, &meta_blob);
    __wt_buf_free(session_impl, &page_item);
    if (ret != 0 && !suppress_util_err)
        (void)util_err(session, ret, "turtle");
    return (ret == 0 ? 0 : 1);
}
