/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __logmgr_sync_cfg --
 *     Interpret the transaction_sync config.
 */
static int
__logmgr_sync_cfg(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    uint32_t txn_logsync;

    conn = S2C(session);

    /*
     * Collect all the flag settings into a local variable and then assign into the connection after
     * we're done so that there is no chance of another thread seeing an interim value while we're
     * processing during a reconfigure.
     */
    txn_logsync = 0;
    WT_RET(__wt_config_gets(session, cfg, "transaction_sync.enabled", &cval));
    if (cval.val)
        FLD_SET(txn_logsync, WT_LOG_SYNC_ENABLED);
    else
        FLD_CLR(txn_logsync, WT_LOG_SYNC_ENABLED);

    WT_RET(__wt_config_gets(session, cfg, "transaction_sync.method", &cval));
    if (WT_CONFIG_LIT_MATCH("dsync", cval))
        FLD_SET(txn_logsync, WT_LOG_DSYNC | WT_LOG_FLUSH);
    else if (WT_CONFIG_LIT_MATCH("fsync", cval))
        FLD_SET(txn_logsync, WT_LOG_FSYNC);
    else if (WT_CONFIG_LIT_MATCH("none", cval))
        FLD_SET(txn_logsync, WT_LOG_FLUSH);
    WT_RELEASE_WRITE_WITH_BARRIER(conn->txn_logsync, txn_logsync);
    return (0);
}

/*
 * __logmgr_force_remove --
 *     Force a checkpoint out and then force a removal, waiting for the first log to be removed up
 *     to the given log number.
 */
static int
__logmgr_force_remove(WT_SESSION_IMPL *session, uint32_t lognum)
{
    WT_CONNECTION_IMPL *conn;
    WT_LOG *log;
    WT_SESSION_IMPL *tmp_session;
    uint64_t sleep_usecs, yield_cnt;

    conn = S2C(session);
    log = conn->log;
    sleep_usecs = yield_cnt = 0;

    WT_RET(__wt_open_internal_session(conn, "compatibility-reconfig", true, 0, 0, &tmp_session));
    while (log->first_lsn.l.file < lognum) {
        /*
         * Force a checkpoint to be written in the new log file and force the removal of all
         * previous log files. We do the checkpoint in the loop because the checkpoint LSN in the
         * log record could still reflect the previous log file in cases such as the write LSN has
         * not yet advanced into the new log file due to another group of threads still in progress
         * with their slot copies or writes.
         */
        WT_RET(tmp_session->iface.checkpoint(&tmp_session->iface, "force=1"));
        /*
         * It's reasonable to start the back off prior to trying at all because the backoff is very
         * gradual.
         */
        __wt_spin_backoff(&yield_cnt, &sleep_usecs);
        WT_STAT_CONN_INCRV(session, log_force_remove_sleep, sleep_usecs);

        WT_RET(WT_SESSION_CHECK_PANIC(tmp_session));
        WT_RET(__wt_log_truncate_files(tmp_session, NULL, true));
    }
    WT_RET(__wt_session_close_internal(tmp_session));
    return (0);
}

/*
 * __logmgr_get_log_version --
 *     Get the log version required for the given WiredTiger version.
 */
static uint16_t
__logmgr_get_log_version(WT_VERSION version)
{
    if (!__wt_version_defined(version))
        return (WT_NO_VALUE);

    if (__wt_version_lt(version, WT_LOG_V2_VERSION))
        return (1);
    else if (__wt_version_lt(version, WT_LOG_V3_VERSION))
        return (2);
    else if (__wt_version_lt(version, WT_LOG_V4_VERSION))
        return (3);
    else if (__wt_version_lt(version, WT_LOG_V5_VERSION))
        return (4);
    else
        return (WT_LOG_VERSION);
}

/*
 * __wti_logmgr_compat_version --
 *     Set up the compatibility versions in the log manager. This is split out because it is called
 *     much earlier than log subsystem creation on startup so that we can verify the system state in
 *     files before modifying files.
 */
void
__wti_logmgr_compat_version(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    conn->log_req_max = __logmgr_get_log_version(conn->compat_req_max);
    conn->log_req_min = __logmgr_get_log_version(conn->compat_req_min);
}

/*
 * __logmgr_version --
 *     Set up the versions in the log manager.
 */
static int
__logmgr_version(WT_SESSION_IMPL *session, bool reconfig)
{
    WT_CONNECTION_IMPL *conn;
    WT_LOG *log;
    uint32_t first_record, lognum;
    uint16_t new_version;
    bool downgrade;

    conn = S2C(session);
    log = conn->log;
    if (log == NULL)
        return (0);

    /*
     * Set the log file format versions based on compatibility versions set in the connection. The
     * compatibility version must be set at this point. We must set this before we call log_open to
     * open or create a log file.
     */
    WT_ASSERT(session, __wt_version_defined(conn->compat_version));
    new_version = __logmgr_get_log_version(conn->compat_version);

    if (new_version > 1)
        first_record = WT_LOG_END_HEADER + log->allocsize;
    else
        first_record = WT_LOG_END_HEADER;

    __wti_logmgr_compat_version(session);

    /*
     * If the version is the same, there is nothing to do.
     */
    if (log->log_version == new_version)
        return (0);

    /*
     * Note: downgrade in this context means the new version is not the latest possible version. It
     * does not mean the direction of change from the release we may be running currently.
     */
    downgrade = new_version != WT_LOG_VERSION;

    /*
     * If we are reconfiguring and at a new version we need to force the log file to advance so that
     * we write out a log file at the correct version. When we are downgrading we must force a
     * checkpoint and finally log removal, even if disabled, so that all new version log files are
     * gone.
     *
     * All of the version changes must be handled with locks on reconfigure because other threads
     * may be changing log files, using pre-allocated files.
     */
    /*
     * Set the version. If it is a live change the logging subsystem will do other work as well to
     * move to a new log file.
     */
    WT_RET(__wt_log_set_version(session, new_version, first_record, downgrade, reconfig, &lognum));
    if (reconfig && FLD_ISSET(conn->log_flags, WT_CONN_LOG_DOWNGRADED))
        WT_RET(__logmgr_force_remove(session, lognum));
    return (0);
}

/*
 * __wti_logmgr_config --
 *     Parse and setup the logging server options.
 */
int
__wti_logmgr_config(WT_SESSION_IMPL *session, const char **cfg, bool reconfig)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    bool enabled;

    /*
     * A note on reconfiguration: the standard "is this configuration string allowed" checks should
     * fail if reconfiguration has invalid strings, for example, "log=(enabled)", or
     * "statistics_log=(path=XXX)", because the connection reconfiguration method doesn't allow
     * those strings. Additionally, the base configuration values during reconfiguration are the
     * currently configured values (so we don't revert to default values when repeatedly
     * reconfiguring), and configuration processing of a currently set value should not change the
     * currently set value.
     *
     * In this code path, log server reconfiguration does not stop/restart the log server, so
     * there's no point in re-evaluating configuration strings that cannot be reconfigured, risking
     * bugs in configuration setup, and depending on evaluation of currently set values to always
     * result in the currently set value. Skip tests for any configuration strings which don't make
     * sense during reconfiguration, but don't worry about error reporting because it should never
     * happen.
     */

    conn = S2C(session);

    WT_RET(__wt_config_gets(session, cfg, "log.enabled", &cval));
    enabled = cval.val != 0;

    /*
     * If we're reconfiguring, enabled must match the already existing setting.
     *
     * If it is off and the user it turning it on, or it is on and the user is turning it off,
     * return an error.
     *
     * See above: should never happen.
     */
    if (reconfig &&
      ((enabled && !FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED)) ||
        (!enabled && FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED))))
        WT_RET_MSG(
          session, EINVAL, "log manager reconfigure: enabled mismatch with existing setting");

    /* Logging is incompatible with in-memory */
    if (enabled) {
        WT_RET(__wt_config_gets(session, cfg, "in_memory", &cval));
        if (cval.val != 0)
            WT_RET_MSG(
              session, EINVAL, "In-memory configuration incompatible with log=(enabled=true)");
    }

    if (enabled)
        FLD_SET(conn->log_flags, WT_CONN_LOG_CONFIG_ENABLED);
    else
        FLD_CLR(conn->log_flags, WT_CONN_LOG_CONFIG_ENABLED);

    /*
     * Setup a log path and compression even if logging is disabled in case we are going to print a
     * log. Only do this on creation. Once a compressor or log path are set they cannot be changed.
     *
     * See above: should never happen.
     */
    if (!reconfig) {
        conn->log_compressor = NULL;
        WT_RET(__wt_config_gets_none(session, cfg, "log.compressor", &cval));
        WT_RET(__wt_compressor_config(session, &cval, &conn->log_compressor));

        conn->log_path = NULL;
        WT_RET(__wt_config_gets(session, cfg, "log.path", &cval));
        WT_RET(__wt_strndup(session, cval.str, cval.len, &conn->log_path));
    }

    /* We are done if logging isn't enabled. */
    if (!FLD_ISSET(conn->log_flags, WT_CONN_LOG_CONFIG_ENABLED))
        return (0);

    /*
     * The configuration string log.archive is deprecated, only take it if it's explicitly set by
     * the application, that is, ignore its default value. Look for an explicit log.remove setting,
     * then an explicit log.archive setting, then the default log.remove setting.
     */
    if (__wt_config_gets(session, cfg + 1, "log.remove", &cval) != 0 &&
      __wt_config_gets(session, cfg + 1, "log.archive", &cval) != 0)
        WT_RET(__wt_config_gets(session, cfg, "log.remove", &cval));
    if (cval.val != 0)
        FLD_SET(conn->log_flags, WT_CONN_LOG_REMOVE);

    /*
     * The file size cannot be reconfigured. The amount of memory allocated to the log slots may be
     * based on the log file size at creation and we don't want to re-allocate that memory while
     * running.
     *
     * See above: should never happen.
     */
    if (!reconfig) {
        WT_RET(__wt_config_gets(session, cfg, "log.file_max", &cval));
        conn->log_file_max = (wt_off_t)cval.val;
        if (FLD_ISSET(conn->direct_io, WT_DIRECT_IO_LOG))
            conn->log_file_max = (wt_off_t)WT_ALIGN(conn->log_file_max, conn->buffer_alignment);
        /*
         * With the default log file extend configuration or if the log file extension size is
         * larger than the configured maximum log file size, set the log file extension size to the
         * configured maximum log file size.
         */
        if (conn->log_extend_len == WT_CONFIG_UNSET || conn->log_extend_len > conn->log_file_max)
            conn->log_extend_len = conn->log_file_max;
        WT_STAT_CONN_SET(session, log_max_filesize, conn->log_file_max);
    }

    WT_RET(__wt_config_gets(session, cfg, "log.os_cache_dirty_pct", &cval));
    if (cval.val != 0)
        conn->log_dirty_max = (conn->log_file_max * cval.val) / 100;

    /*
     * If pre-allocation is configured, set the initial number to a few. We'll adapt as load
     * dictates.
     */
    WT_RET(__wt_config_gets(session, cfg, "log.prealloc", &cval));
    if (cval.val != 0) {
        WT_RET(__wt_config_gets(session, cfg, "log.prealloc_init_count", &cval));
        conn->log_prealloc = (uint32_t)cval.val;
        conn->log_prealloc_init_count = (uint32_t)cval.val;
        WT_ASSERT(session, conn->log_prealloc > 0);
    }

    WT_RET(__wt_config_gets(session, cfg, "log.force_write_wait", &cval));
    if (cval.val != 0)
        conn->log_force_write_wait = (uint32_t)cval.val;

    /*
     * Note it's meaningless to reconfigure this value during runtime, it only matters on create
     * before recovery runs.
     *
     * See above: should never happen.
     */
    if (!reconfig) {
        WT_RET(__wt_config_gets_def(session, cfg, "log.recover", 0, &cval));
        if (WT_CONFIG_LIT_MATCH("error", cval))
            FLD_SET(conn->log_flags, WT_CONN_LOG_RECOVER_ERR);
    }

    WT_RET(__wt_config_gets(session, cfg, "log.zero_fill", &cval));
    if (cval.val != 0) {
        if (F_ISSET(conn, WT_CONN_READONLY))
            WT_RET_MSG(
              session, EINVAL, "Read-only configuration incompatible with zero-filling log files");
        FLD_SET(conn->log_flags, WT_CONN_LOG_ZERO_FILL);
    }

    WT_RET(__logmgr_sync_cfg(session, cfg));
    if (conn->log_cond != NULL)
        __wt_cond_signal(session, conn->log_cond);
    return (0);
}

/*
 * __wti_logmgr_reconfig --
 *     Reconfigure logging.
 */
int
__wti_logmgr_reconfig(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_RET(__wti_logmgr_config(session, cfg, true));
    return (__logmgr_version(session, true));
}

/*
 * __log_prealloc_once --
 *     Perform one iteration of log pre-allocation.
 */
static int
__log_prealloc_once(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LOG *log;
    u_int i, reccount;
    char **recfiles;

    conn = S2C(session);
    log = conn->log;
    reccount = 0;
    recfiles = NULL;

    /*
     * Allocate up to the maximum number, accounting for any existing files that may not have been
     * used yet.
     */
    WT_ERR(__wt_fs_directory_list(session, conn->log_path, WT_LOG_PREPNAME, &recfiles, &reccount));

    /*
     * Adjust the number of files to pre-allocate if we find that the critical path had to allocate
     * them since we last ran.
     */
    if (log->prep_missed > 0) {
        conn->log_prealloc += log->prep_missed;
        __wt_verbose(session, WT_VERB_LOG, "Missed %" PRIu32 ". Now pre-allocating up to %" PRIu32,
          log->prep_missed, conn->log_prealloc);
    } else if (reccount > conn->log_prealloc / 2 &&
      conn->log_prealloc > conn->log_prealloc_init_count) {
        /*
         * If we used less than half, then start adjusting down.
         */
        --conn->log_prealloc;
        __wt_verbose(session, WT_VERB_LOG,
          "Adjust down. Did not use %" PRIu32 ". Now pre-allocating %" PRIu32, reccount,
          conn->log_prealloc);
    }

    WT_STAT_CONN_SET(session, log_prealloc_max, conn->log_prealloc);
    /*
     * Allocate up to the maximum number that we just computed and detected.
     */
    for (i = reccount; i < (u_int)conn->log_prealloc; i++) {
        WT_ERR(__wt_log_allocfile(session, ++log->prep_fileid, WT_LOG_PREPNAME));
        WT_STAT_CONN_INCR(session, log_prealloc_files);
    }
    /*
     * Reset the missed count now. If we missed during pre-allocating the log files, it means the
     * allocation is not keeping up, not that we didn't allocate enough. So we don't just want to
     * keep adding in more.
     */
    log->prep_missed = 0;

    if (0)
err:
        __wt_err(session, ret, "log pre-alloc server error");
    WT_TRET(__wt_fs_directory_list_free(session, &recfiles, reccount));
    return (ret);
}

/*
 * __log_file_server --
 *     The log file server thread. This worker thread manages log file operations such as closing
 *     and syncing.
 */
static WT_THREAD_RET
__log_file_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_FH *close_fh;
    WT_LOG *log;
    WT_LSN close_end_lsn;
    WT_SESSION_IMPL *session;
    uint32_t filenum;

    session = arg;
    conn = S2C(session);
    log = conn->log;
    while (FLD_ISSET(conn->server_flags, WT_CONN_SERVER_LOG)) {
        /*
         * If there is a log file to close, make sure any outstanding write operations have
         * completed, then fsync and close it.
         *
         * The read from the log close file handle is ordered with the read from the log close lsn.
         * Writers will set the log close lsn first and then the log close file handle, so we need
         * to read them in the reverse order to see a consistent state.
         */
        WT_ACQUIRE_READ_WITH_BARRIER(close_fh, log->log_close_fh);
        if (close_fh != NULL) {
            WT_ERR(__wt_log_extract_lognum(session, close_fh->name, &filenum));
            /*
             * The closing file handle should have a correct close LSN.
             */
            WT_ASSERT(session, log->log_close_lsn.l.file == filenum);

            if (__wt_log_cmp(&log->write_lsn, &log->log_close_lsn) >= 0) {
                /*
                 * We've copied the file handle, clear out the one in the log structure to allow it
                 * to be set again. Copy the LSN before clearing the file handle. Use a barrier to
                 * make sure the compiler does not reorder the following two statements.
                 */
                WT_ASSIGN_LSN(&close_end_lsn, &log->log_close_lsn);
                WT_FULL_BARRIER();
                log->log_close_fh = NULL;
                /*
                 * Set the close_end_lsn to the LSN immediately after ours. That is, the beginning
                 * of the next log file. We need to know the LSN file number of our own close in
                 * case earlier calls are still in progress and the next one to move the sync_lsn
                 * into the next file for later syncs.
                 */
                WT_ERR(__wt_fsync(session, close_fh, true));

                /*
                 * We want to have the file size reflect actual data with minimal pre-allocated
                 * zeroed space. We can't truncate the file during hot backup, or the underlying
                 * file system may not support truncate: both are OK, it's just more work during
                 * cursor traversal.
                 */
                if (__wt_atomic_load64(&conn->hot_backup_start) == 0 && conn->log_cursors == 0) {
                    WT_WITH_HOTBACKUP_READ_LOCK(session,
                      ret = __wt_ftruncate(session, close_fh, __wt_lsn_offset(&close_end_lsn)),
                      NULL);
                    WT_ERR_ERROR_OK(ret, ENOTSUP, false);
                }
                WT_SET_LSN(&close_end_lsn, close_end_lsn.l.file + 1, 0);
                __wt_spin_lock(session, &log->log_sync_lock);
                WT_ERR(__wt_close(session, &close_fh));
                WT_ASSERT(session, __wt_log_cmp(&close_end_lsn, &log->sync_lsn) >= 0);
                WT_ASSIGN_LSN(&log->sync_lsn, &close_end_lsn);
                __wt_cond_signal(session, log->log_sync_cond);
                __wt_spin_unlock(session, &log->log_sync_lock);
            }
        }

        /* Wait until the next event. */
        __wt_cond_wait(session, conn->log_file_cond, 100 * WT_THOUSAND, NULL);
    }

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "log close server error"));
    }
    __wt_spin_unlock_if_owned(session, &log->log_sync_lock);
    return (WT_THREAD_RET_VALUE);
}

/*
 * __log_wrlsn_server --
 *     The log wrlsn server thread.
 */
static WT_THREAD_RET
__log_wrlsn_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LOG *log;
    WT_LSN prev;
    WT_SESSION_IMPL *session;
    int yield;
    bool did_work;

    session = arg;
    conn = S2C(session);
    log = conn->log;
    yield = 0;
    WT_INIT_LSN(&prev);
    while (FLD_ISSET(conn->server_flags, WT_CONN_SERVER_LOG)) {
        /*
         * Write out any log record buffers if anything was done since last time. Only call the
         * function to walk the slots if the system is not idle. On an idle system the alloc_lsn
         * will not advance and the written lsn will match the alloc_lsn.
         */
        if (__wt_log_cmp(&prev, &log->alloc_lsn) != 0 ||
          __wt_log_cmp(&log->write_lsn, &log->alloc_lsn) != 0)
            __wt_log_wrlsn(session, &yield);
        else
            WT_STAT_CONN_INCR(session, log_write_lsn_skip);
        prev = log->alloc_lsn;
        did_work = (yield == 0);

        /*
         * If __wt_log_wrlsn did work we want to yield instead of sleep.
         */
        if (yield++ < WT_THOUSAND)
            __wt_yield();
        else
            __wt_cond_auto_wait(session, conn->log_wrlsn_cond, did_work, NULL);
    }
    /*
     * On close we need to do this one more time because there could be straggling log writes that
     * need to be written.
     */
    WT_ERR(__wt_log_force_write(session, true, NULL));
    __wt_log_wrlsn(session, NULL);
    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "log wrlsn server error"));
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __log_server --
 *     The log server thread.
 */
static WT_THREAD_RET
__log_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LOG *log;
    WT_SESSION_IMPL *session;
    uint64_t force_write_time_start, force_write_timediff;
    uint64_t time_start, time_stop, timediff;
    bool did_work, signalled;

    session = arg;
    conn = S2C(session);
    log = conn->log;
    force_write_timediff = 0;
    signalled = false;

    /*
     * Set this to the number of milliseconds we want to run log force write, remove and
     * pre-allocation. Start it so that we run on the first time through.
     */
    timediff = WT_THOUSAND;
    force_write_time_start = time_start = __wt_clock(session);

    /*
     * The log server thread does a variety of work. It forces out any buffered log writes. It
     * pre-allocates log files and it performs log removal. The reason the wrlsn thread does not
     * force out the buffered writes is because we want to process and move the write_lsn forward as
     * quickly as possible. The same reason applies to why the log file server thread does not force
     * out the writes. That thread does fsync calls which can take a long time and we don't want log
     * records sitting in the buffer over the time it takes to sync out an earlier file.
     */
    did_work = true;
    while (FLD_ISSET(conn->server_flags, WT_CONN_SERVER_LOG)) {
        /*
         * Slots depend on future activity. Force out buffered writes in case we are idle. This
         * cannot be part of the wrlsn thread because of interaction advancing the write_lsn and a
         * buffer may need to wait for the write_lsn to advance in the case of a synchronous buffer.
         * We end up with a hang.
         */
        if (conn->log_force_write_wait == 0 ||
          force_write_timediff >= conn->log_force_write_wait * WT_THOUSAND) {
            WT_ERR_ERROR_OK(__wt_log_force_write(session, false, &did_work), EBUSY, false);
            force_write_time_start = __wt_clock(session);
        }
        /*
         * We don't want to remove or pre-allocate files as often as we want to force out log
         * buffers. Only do it once per second or if the condition was signalled.
         */
        if (timediff >= WT_THOUSAND || signalled) {

            /*
             * Perform log pre-allocation.
             */
            if (conn->log_prealloc > 0) {
                /*
                 * Log file pre-allocation is disabled when a hot backup cursor is open because we
                 * have agreed not to rename or remove any files in the database directory.
                 */
                WT_WITH_HOTBACKUP_READ_LOCK(session, ret = __log_prealloc_once(session), NULL);
                WT_ERR(ret);
            }

            /*
             * Perform the removal.
             */
            if (FLD_ISSET(conn->log_flags, WT_CONN_LOG_REMOVE)) {
                if (__wt_try_writelock(session, &log->log_remove_lock) == 0) {
                    ret = __wt_log_remove_once(session, 0);
                    __wt_writeunlock(session, &log->log_remove_lock);
                    WT_ERR(ret);
                } else
                    __wt_verbose(session, WT_VERB_LOG, "%s",
                      "log_remove: Blocked due to open log cursor holding remove lock");
            }
            time_start = __wt_clock(session);
        }

        /* Wait until the next event. */
        __wt_cond_auto_wait_signal(session, conn->log_cond, did_work, NULL, &signalled);
        time_stop = __wt_clock(session);
        timediff = WT_CLOCKDIFF_MS(time_stop, time_start);
        force_write_timediff = WT_CLOCKDIFF_MS(time_stop, force_write_time_start);
    }

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "log server error"));
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wti_logmgr_create --
 *     Initialize the log subsystem (before running recovery).
 */
int
__wti_logmgr_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_LOG *log;
    uint64_t now;

    conn = S2C(session);

    /*
     * Logging configuration is parsed early on for compatibility checking. It is separated from
     * turning on the subsystem. We only need to proceed here if logging is enabled.
     */
    if (!FLD_ISSET(conn->log_flags, WT_CONN_LOG_CONFIG_ENABLED))
        return (0);

    FLD_SET(conn->log_flags, WT_CONN_LOG_ENABLED);
    /*
     * Logging is on, allocate the WT_LOG structure and open the log file.
     */
    WT_RET(__wt_calloc_one(session, &conn->log));
    log = conn->log;
    WT_RET(__wt_spin_init(session, &log->log_lock, "log"));
    WT_RET(__wt_spin_init(session, &log->log_fs_lock, "log files"));
    WT_RET(__wt_spin_init(session, &log->log_slot_lock, "log slot"));
    WT_RET(__wt_spin_init(session, &log->log_sync_lock, "log sync"));
    WT_RET(__wt_spin_init(session, &log->log_writelsn_lock, "log write LSN"));
    WT_RET(__wt_rwlock_init(session, &log->log_remove_lock));
    if (FLD_ISSET(conn->direct_io, WT_DIRECT_IO_LOG))
        log->allocsize = (uint32_t)WT_MAX(conn->buffer_alignment, WT_LOG_ALIGN);
    else
        log->allocsize = WT_LOG_ALIGN;
    WT_INIT_LSN(&log->alloc_lsn);
    WT_INIT_LSN(&log->ckpt_lsn);
    WT_INIT_LSN(&log->first_lsn);
    WT_INIT_LSN(&log->sync_lsn);
    /*
     * We only use file numbers for directory sync, so this needs to initialized to zero.
     */
    WT_ZERO_LSN(&log->sync_dir_lsn);
    WT_INIT_LSN(&log->trunc_lsn);
    WT_INIT_LSN(&log->write_lsn);
    WT_INIT_LSN(&log->write_start_lsn);
    log->fileid = 0;
    WT_RET(__logmgr_version(session, false));

    WT_RET(__wt_cond_alloc(session, "log sync", &log->log_sync_cond));
    WT_RET(__wt_cond_alloc(session, "log write", &log->log_write_cond));
    WT_RET(__wt_log_open(session));
    WT_RET(__wt_log_slot_init(session, true));

    /* Write the start log record on creation, which is before recovery is run. */
    __wt_seconds(session, &now);
    WT_RET(__wt_log_printf(session, "SYSTEM: Log manager created at %" PRIu64, now));
    return (0);
}

/*
 * __wti_logmgr_open --
 *     Start the log service threads.
 */
int
__wti_logmgr_open(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t now;
    uint32_t session_flags;

    conn = S2C(session);

    /* If no log thread services are configured, we're done. */
    if (!FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED))
        return (0);

    FLD_SET(conn->server_flags, WT_CONN_SERVER_LOG);

    /*
     * Start the log close thread. It is not configurable. If logging is enabled, this thread runs.
     */
    session_flags = WT_SESSION_NO_DATA_HANDLES;
    WT_RET(__wt_open_internal_session(
      conn, "log-close-server", false, session_flags, 0, &conn->log_file_session));
    WT_RET(__wt_cond_alloc(conn->log_file_session, "log close server", &conn->log_file_cond));

    /*
     * Start the log file close thread.
     */
    WT_RET(__wt_thread_create(
      conn->log_file_session, &conn->log_file_tid, __log_file_server, conn->log_file_session));
    conn->log_file_tid_set = true;

    /*
     * Start the log write LSN thread. It is not configurable. If logging is enabled, this thread
     * runs.
     */
    WT_RET(__wt_open_internal_session(
      conn, "log-wrlsn-server", false, session_flags, 0, &conn->log_wrlsn_session));
    WT_RET(__wt_cond_auto_alloc(conn->log_wrlsn_session, "log write lsn server", 10 * WT_THOUSAND,
      WT_MILLION, &conn->log_wrlsn_cond));
    WT_RET(__wt_thread_create(
      conn->log_wrlsn_session, &conn->log_wrlsn_tid, __log_wrlsn_server, conn->log_wrlsn_session));
    conn->log_wrlsn_tid_set = true;

    /*
     * If a log server thread exists, the user may have reconfigured removal or pre-allocation.
     * Signal the thread. Otherwise the user wants removal and/or allocation and we need to start up
     * the thread.
     */
    if (conn->log_session != NULL) {
        WT_ASSERT(session, conn->log_cond != NULL);
        WT_ASSERT(session, conn->log_tid_set == true);
        __wt_cond_signal(session, conn->log_cond);
    } else {
        /* The log server gets its own session. */
        WT_RET(__wt_open_internal_session(
          conn, "log-server", false, session_flags, 0, &conn->log_session));
        WT_RET(__wt_cond_auto_alloc(
          conn->log_session, "log server", 50 * WT_THOUSAND, WT_MILLION, &conn->log_cond));

        /*
         * Start the thread.
         */
        WT_RET(
          __wt_thread_create(conn->log_session, &conn->log_tid, __log_server, conn->log_session));
        conn->log_tid_set = true;
    }

    /* Write another startup log record with timestamp after recovery completes. */
    __wt_seconds(session, &now);
    WT_RET(__wt_log_printf(
      session, "SYSTEM: Log manager threads started post-recovery at %" PRIu64, now));
    return (0);
}

/*
 * __wti_logmgr_destroy --
 *     Destroy the log removal server thread and logging subsystem.
 */
int
__wti_logmgr_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    FLD_CLR(conn->server_flags, WT_CONN_SERVER_LOG);

    if (!FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED)) {
        /*
         * We always set up the log_path so printlog can work without recovery. Therefore, always
         * free it, even if logging isn't on.
         */
        __wt_free(session, conn->log_path);
        return (0);
    }
    if (conn->log_tid_set) {
        __wt_cond_signal(session, conn->log_cond);
        WT_TRET(__wt_thread_join(session, &conn->log_tid));
        conn->log_tid_set = false;
    }
    if (conn->log_file_tid_set) {
        __wt_cond_signal(session, conn->log_file_cond);
        WT_TRET(__wt_thread_join(session, &conn->log_file_tid));
        conn->log_file_tid_set = false;
    }
    if (conn->log_file_session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->log_file_session));
        conn->log_file_session = NULL;
    }
    if (conn->log_wrlsn_tid_set) {
        __wt_cond_signal(session, conn->log_wrlsn_cond);
        WT_TRET(__wt_thread_join(session, &conn->log_wrlsn_tid));
        conn->log_wrlsn_tid_set = false;
    }
    if (conn->log_wrlsn_session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->log_wrlsn_session));
        conn->log_wrlsn_session = NULL;
    }

    WT_TRET(__wt_log_slot_destroy(session));
    WT_TRET(__wt_log_close(session));

    /* Close the server thread's session. */
    if (conn->log_session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->log_session));
        conn->log_session = NULL;
    }

    /* Destroy the condition variables now that all threads are stopped */
    __wt_cond_destroy(session, &conn->log_cond);
    __wt_cond_destroy(session, &conn->log_file_cond);
    __wt_cond_destroy(session, &conn->log_wrlsn_cond);

    __wt_cond_destroy(session, &conn->log->log_sync_cond);
    __wt_cond_destroy(session, &conn->log->log_write_cond);
    __wt_rwlock_destroy(session, &conn->log->log_remove_lock);
    __wt_spin_destroy(session, &conn->log->log_lock);
    __wt_spin_destroy(session, &conn->log->log_fs_lock);
    __wt_spin_destroy(session, &conn->log->log_slot_lock);
    __wt_spin_destroy(session, &conn->log->log_sync_lock);
    __wt_spin_destroy(session, &conn->log->log_writelsn_lock);
    __wt_free(session, conn->log_path);
    __wt_free(session, conn->log);
    return (ret);
}
