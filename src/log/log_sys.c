/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_log_system_record --
 *     Write a system log record for the previous LSN.
 */
int
__wt_log_system_record(WT_SESSION_IMPL *session, WT_FH *log_fh, WT_LSN *lsn)
{
    WT_DECL_ITEM(logrec_buf);
    WT_DECL_RET;
    WT_LOG *log;
    WT_LOGSLOT tmp;
    WT_LOG_RECORD *logrec;
    WT_MYSLOT myslot;
    size_t recsize;
    uint32_t rectype;
    const char *fmt;

    log = S2C(session)->log;
    rectype = WT_LOGREC_SYSTEM;
    fmt = WT_UNCHECKED_STRING(I);

    WT_RET(__wt_logrec_alloc(session, log->allocsize, &logrec_buf));
    memset((uint8_t *)logrec_buf->mem, 0, log->allocsize);

    WT_ERR(__wt_struct_size(session, &recsize, fmt, rectype));
    WT_ERR(__wt_struct_pack(
      session, (uint8_t *)logrec_buf->data + logrec_buf->size, recsize, fmt, rectype));
    logrec_buf->size += recsize;
    WT_ERR(__wt_logop_prev_lsn_pack(session, logrec_buf, lsn));
    WT_ASSERT(session, logrec_buf->size <= log->allocsize);

    logrec = (WT_LOG_RECORD *)logrec_buf->mem;

    /*
     * We know system records are this size. And we have to adjust the size now because we're not
     * going through the normal log write path and the packing functions needed the correct offset
     * earlier.
     */
    logrec_buf->size = logrec->len = log->allocsize;

    /* We do not compress nor encrypt this record. */
    logrec->checksum = 0;
    logrec->flags = 0;
    __wt_log_record_byteswap(logrec);
    logrec->checksum = __wt_checksum(logrec, log->allocsize);
#ifdef WORDS_BIGENDIAN
    logrec->checksum = __wt_bswap32(logrec->checksum);
#endif
    WT_CLEAR(tmp);
    memset(&myslot, 0, sizeof(myslot));
    myslot.slot = &tmp;
    __wt_log_slot_activate(session, &tmp);
    /*
     * Override the file handle to the one we're using.
     */
    tmp.slot_fh = log_fh;
    WT_ERR(__wt_log_fill(session, &myslot, true, logrec_buf, NULL));
err:
    __wt_logrec_free(session, &logrec_buf);
    return (ret);
}

/*
 * __wt_log_recover_system --
 *     Process a system log record for the previous LSN in recovery.
 */
int
__wt_log_recover_system(
  WT_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, WT_LSN *lsnp)
{
    WT_DECL_RET;

    if ((ret = __wt_logop_prev_lsn_unpack(session, pp, end, lsnp)) != 0)
        WT_RET_MSG(session, ret, "log_recover_prevlsn: unpack failure");

    return (0);
}

/*
 * __wt_verbose_dump_log --
 *     Dump information about the logging subsystem.
 */
int
__wt_verbose_dump_log(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(msg);
    WT_DECL_RET;
    WT_LOG *log;
    uint64_t tmp_len;
    char tmp[1024];
    bool json_output, logging_enabled;

    conn = S2C(session);
    log = conn->log;
    json_output = FLD_ISSET(conn->json_output, WT_JSON_OUTPUT_MESSAGE);
    logging_enabled = FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED);
    tmp_len = sizeof(tmp);

    WT_ERR(__wt_snprintf(tmp, tmp_len, "%s", logging_enabled ? "yes" : "no"));
    if (json_output) {
        WT_ERR(__wt_scr_alloc(session, 0, &msg));
        WT_ERR(__wt_buf_catfmt(session, msg, "{"));
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Logging subsystem\":{"));
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Enabled\":\"%s\"", tmp));
    } else {
        WT_ERR(__wt_msg(session, "%s", WT_DIVIDER));
        WT_ERR(__wt_msg(session, "Logging subsystem: Enabled: %s", tmp));
    }

    if (!logging_enabled)
        goto done;
    else if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, ","));

    /*
     * Logging is enabled, print out the other information.
     */
    WT_ERR(__wt_snprintf(
      tmp, tmp_len, "%s", FLD_ISSET(conn->log_flags, WT_CONN_LOG_ARCHIVE) ? "yes" : "no"));
    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Archiving\":\"%s\",", tmp));
    else
        WT_ERR(__wt_msg(session, "Archiving: %s", tmp));

    WT_ERR(__wt_snprintf(
      tmp, tmp_len, "%s", FLD_ISSET(conn->log_flags, WT_CONN_LOG_DOWNGRADED) ? "yes" : "no"));
    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Running downgraded\":\"%s\",", tmp));
    else
        WT_ERR(__wt_msg(session, "Running downgraded: %s", tmp));

    WT_ERR(__wt_snprintf(
      tmp, tmp_len, "%s", FLD_ISSET(conn->log_flags, WT_CONN_LOG_ZERO_FILL) ? "yes" : "no"));
    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Zero fill files\":\"%s\",", tmp));
    else
        WT_ERR(__wt_msg(session, "Zero fill files: %s", tmp));

    WT_ERR(__wt_snprintf(tmp, tmp_len, "%s", conn->log_prealloc > 0 ? "yes" : "no"));
    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Pre-allocate files\":\"%s\",", tmp));
    else
        WT_ERR(__wt_msg(session, "Pre-allocate files: %s", tmp));

    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Logging directory\":\"%s\",", conn->log_path));
    else
        WT_ERR(__wt_msg(session, "Logging directory: %s", conn->log_path));

    if (json_output)
        WT_ERR(__wt_buf_catfmt(
          session, msg, "\"Logging maximum file size\":%" PRId64 ",", (int64_t)conn->log_file_max));
    else
        WT_ERR(
          __wt_msg(session, "Logging maximum file size: %" PRId64, (int64_t)conn->log_file_max));

    WT_ERR(__wt_snprintf(tmp, tmp_len, "%s",
      !FLD_ISSET(conn->txn_logsync, WT_LOG_SYNC_ENABLED) ? "none" :
        FLD_ISSET(conn->txn_logsync, WT_LOG_DSYNC)       ? "dsync" :
        FLD_ISSET(conn->txn_logsync, WT_LOG_FLUSH)       ? "write to OS" :
        FLD_ISSET(conn->txn_logsync, WT_LOG_FSYNC)       ? "fsync to disk" :
                                                           "unknown sync setting"));
    if (json_output)
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Log sync setting\":\"%s\",", tmp));
    else
        WT_ERR(__wt_msg(session, "Log sync setting: %s", tmp));

    if (json_output) {
        WT_ERR(__wt_buf_catfmt(
          session, msg, "\"Log record allocation alignment\":%" PRIu32 ",", log->allocsize));
        WT_ERR(
          __wt_buf_catfmt(session, msg, "\"Current log file number\":%" PRIu32 ",", log->fileid));
        WT_ERR(__wt_buf_catfmt(
          session, msg, "\"Current log version number\":%" PRIu16 ",", log->log_version));

        WT_ERR(__wt_buf_catfmt(session, msg, "\"Next allocation LSN\":[%" PRIu32 "][%" PRIu32 "],",
          log->alloc_lsn.l.file, log->alloc_lsn.l.offset));
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Last checkpoint LSN\":[%" PRIu32 "][%" PRIu32 "],",
          log->ckpt_lsn.l.file, log->ckpt_lsn.l.offset));
        WT_ERR(
          __wt_buf_catfmt(session, msg, "\"Last directory sync LSN\":[%" PRIu32 "][%" PRIu32 "],",
            log->sync_dir_lsn.l.file, log->sync_dir_lsn.l.offset));
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Last sync LSN\":[%" PRIu32 "][%" PRIu32 "],",
          log->sync_lsn.l.file, log->sync_lsn.l.offset));
        WT_ERR(
          __wt_buf_catfmt(session, msg, "\"Recovery truncate LSN\":[%" PRIu32 "][%" PRIu32 "],",
            log->trunc_lsn.l.file, log->trunc_lsn.l.offset));
        WT_ERR(__wt_buf_catfmt(session, msg, "\"Last written LSN\":[%" PRIu32 "][%" PRIu32 "],",
          log->write_lsn.l.file, log->write_lsn.l.offset));
        WT_ERR(
          __wt_buf_catfmt(session, msg, "\"Start of last written LSN\":[%" PRIu32 "][%" PRIu32 "]",
            log->write_start_lsn.l.file, log->write_start_lsn.l.offset));
    } else {
        WT_ERR(__wt_msg(session, "Log record allocation alignment: %" PRIu32, log->allocsize));
        WT_ERR(__wt_msg(session, "Current log file number: %" PRIu32, log->fileid));
        WT_ERR(__wt_msg(session, "Current log version number: %" PRIu16, log->log_version));
        WT_ERR(WT_LSN_MSG(&log->alloc_lsn, "Next allocation"));
        WT_ERR(WT_LSN_MSG(&log->ckpt_lsn, "Last checkpoint"));
        WT_ERR(WT_LSN_MSG(&log->sync_dir_lsn, "Last directory sync"));
        WT_ERR(WT_LSN_MSG(&log->sync_lsn, "Last sync"));
        WT_ERR(WT_LSN_MSG(&log->trunc_lsn, "Recovery truncate"));
        WT_ERR(WT_LSN_MSG(&log->write_lsn, "Last written"));
        WT_ERR(WT_LSN_MSG(&log->write_start_lsn, "Start of last written"));
    }

    /*
     * If we wanted a dump of the slots, it would go here. Walking the slot pool may not require a
     * lock since they're statically allocated, but output could be inconsistent without it.
     */

done:
    if (json_output) {
        WT_ERR(__wt_buf_catfmt(session, msg, "}}"));
        WT_ERR(__wt_msg(session, "%s", (const char *)msg->data));
    }
err:
    __wt_scr_free(session, &msg);
    return (ret);
}
