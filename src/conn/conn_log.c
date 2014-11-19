/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __logmgr_sync_cfg --
 *	Interpret the transaction_sync config.
 */
static int
__logmgr_sync_cfg(WT_SESSION_IMPL *session, const char **cfg)
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	WT_RET(
	    __wt_config_gets(session, cfg, "transaction_sync.enabled", &cval));
	if (cval.val)
		FLD_SET(conn->txn_logsync, WT_LOG_FLUSH);
	else
		FLD_CLR(conn->txn_logsync, WT_LOG_FLUSH);

	WT_RET(
	    __wt_config_gets(session, cfg, "transaction_sync.method", &cval));
	FLD_CLR(conn->txn_logsync, WT_LOG_DSYNC | WT_LOG_FSYNC);
	if (WT_STRING_MATCH("dsync", cval.str, cval.len))
		FLD_SET(conn->txn_logsync, WT_LOG_DSYNC);
	else if (WT_STRING_MATCH("fsync", cval.str, cval.len))
		FLD_SET(conn->txn_logsync, WT_LOG_FSYNC);
	return (0);
}

/*
 * __logmgr_config --
 *	Parse and setup the logging server options.
 */
static int
__logmgr_config(WT_SESSION_IMPL *session, const char **cfg, int *runp)
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * The logging configuration is off by default.
	 */
	WT_RET(__wt_config_gets(session, cfg, "log.enabled", &cval));
	*runp = cval.val != 0;
	if (*runp == 0)
		return (0);

	WT_RET(__wt_config_gets(session, cfg, "log.archive", &cval));
	conn->archive = cval.val != 0;

	WT_RET(__wt_config_gets(session, cfg, "log.file_max", &cval));
	conn->log_file_max = (wt_off_t)cval.val;
	WT_STAT_FAST_CONN_SET(session, log_max_filesize, conn->log_file_max);

	WT_RET(__wt_config_gets(session, cfg, "log.path", &cval));
	WT_RET(__wt_strndup(session, cval.str, cval.len, &conn->log_path));

	WT_RET(__logmgr_sync_cfg(session, cfg));
	return (0);
}

/*
 * __log_archive_once --
 *	Perform one iteration of log archiving.  Must be called with the
 *	log archive lock held.
 */
static int
__log_archive_once(WT_SESSION_IMPL *session, uint32_t backup_file)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	uint32_t lognum, min_lognum;
	u_int i, locked, logcount;
	char **logfiles;

	conn = S2C(session);
	log = conn->log;
	logcount = 0;
	logfiles = NULL;

	/*
	 * If we're coming from a backup cursor we want the smaller of
	 * the last full log file copied in backup or the checkpoint LSN.
	 */
	if (backup_file != 0)
		min_lognum = WT_MIN(log->ckpt_lsn.file, backup_file);
	else
		min_lognum = log->ckpt_lsn.file;
	WT_RET(__wt_verbose(session, WT_VERB_LOG,
	    "log_archive: archive to LSN %" PRIu32, min_lognum));

	/*
	 * Main archive code.  Get the list of all log files and
	 * remove any earlier than the checkpoint LSN.
	 */
	WT_RET(__wt_dirlist(session, conn->log_path,
	    WT_LOG_FILENAME, WT_DIRLIST_INCLUDE, &logfiles, &logcount));

	/*
	 * We can only archive files if a hot backup is not in progress or
	 * if we are the backup.
	 */
	__wt_spin_lock(session, &conn->hot_backup_lock);
	locked = 1;
	if (conn->hot_backup == 0 || backup_file != 0) {
		for (i = 0; i < logcount; i++) {
			WT_ERR(__wt_log_extract_lognum(
			    session, logfiles[i], &lognum));
			if (lognum < min_lognum)
				WT_ERR(
				    __wt_log_remove(session, lognum));
		}
	}
	__wt_spin_unlock(session, &conn->hot_backup_lock);
	locked = 0;
	__wt_log_files_free(session, logfiles, logcount);
	logfiles = NULL;
	logcount = 0;

	/*
	 * Indicate what is our new earliest LSN.  It is the start
	 * of the log file containing the last checkpoint.
	 */
	log->first_lsn.file = min_lognum;
	log->first_lsn.offset = 0;

	if (0) {
err:		__wt_err(session, ret, "log archive server error");
	}
	if (locked)
		__wt_spin_unlock(session, &conn->hot_backup_lock);
	if (logfiles != NULL)
		__wt_log_files_free(session, logfiles, logcount);
	return (ret);
}

/*
 * __wt_log_truncate_files --
 *	Truncate log files via archive once. Requires that the server is not
 *	currently running.
 */
int
__wt_log_truncate_files(
    WT_SESSION_IMPL *session, WT_CURSOR *cursor, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	uint32_t backup_file, locked;

	WT_UNUSED(cfg);
	conn = S2C(session);
	log = conn->log;
	if (F_ISSET(conn, WT_CONN_SERVER_RUN) && conn->archive)
		WT_RET_MSG(session, EINVAL,
		    "Attempt to archive manually while a server is running");

	backup_file = 0;
	if (cursor != NULL)
		backup_file = WT_CURSOR_BACKUP_ID(cursor);
	WT_ASSERT(session, backup_file <= log->alloc_lsn.file);
	WT_RET(__wt_verbose(session, WT_VERB_LOG,
	    "log_truncate: Archive once up to %" PRIu32,
	    backup_file));
	WT_RET(__wt_writelock(session, log->log_archive_lock));
	locked = 1;
	WT_ERR(__log_archive_once(session, backup_file));
	WT_ERR(__wt_writeunlock(session, log->log_archive_lock));
	locked = 0;
err:
	if (locked)
		WT_RET(__wt_writeunlock(session, log->log_archive_lock));
	return (ret);
}

/*
 * __log_archive_server --
 *	The log archiving server thread.
 */
static void *
__log_archive_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_LOG *log;
	WT_SESSION_IMPL *session;
	u_int locked;

	session = arg;
	conn = S2C(session);
	log = conn->log;
	locked = 0;
	while (F_ISSET(conn, WT_CONN_SERVER_RUN)) {
		/*
		 * If archiving is reconfigured and turned off, wait until it
		 * gets turned back on and check again.  Don't wait forever: if
		 * a notification gets lost during close, we want to find out
		 * eventually.
		 */
		if (conn->archive == 0 ||
		    __wt_try_writelock(session, log->log_archive_lock) != 0) {
			if (conn->archive != 0) {
				WT_ERR(__wt_verbose(session, WT_VERB_LOG,
				    "log_archive: Blocked due to open log "
				    "cursor holding archive lock"));
			}
			WT_ERR(
			    __wt_cond_wait(session, conn->arch_cond, 1000000));
			continue;
		}
		locked = 1;
		/*
		 * Perform the archive.
		 */
		WT_ERR(__log_archive_once(session, 0));
		WT_ERR(__wt_writeunlock(session, log->log_archive_lock));
		locked = 0;

		/* Wait until the next event. */
		WT_ERR(__wt_cond_wait(session, conn->arch_cond, 1000000));
	}

	if (0) {
err:		__wt_err(session, ret, "log archive server error");
	}
	if (locked)
		(void)__wt_writeunlock(session, log->log_archive_lock);
	return (NULL);
}

/*
 * __wt_logmgr_create --
 *	Start the log subsystem and archive server thread.
 */
int
__wt_logmgr_create(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_LOG *log;
	int run;

	conn = S2C(session);

	/* Handle configuration. */
	WT_RET(__logmgr_config(session, cfg, &run));

	/* If logging is not configured, we're done. */
	if (!run)
		return (0);

	conn->logging = 1;
	/*
	 * Logging is on, allocate the WT_LOG structure and open the log file.
	 */
	WT_RET(__wt_calloc(session, 1, sizeof(WT_LOG), &conn->log));
	log = conn->log;
	WT_RET(__wt_spin_init(session, &log->log_lock, "log"));
	WT_RET(__wt_spin_init(session, &log->log_slot_lock, "log slot"));
	WT_RET(__wt_spin_init(session, &log->log_sync_lock, "log sync"));
	WT_RET(__wt_rwlock_alloc(session,
	    &log->log_archive_lock, "log archive lock"));
	if (FLD_ISSET(conn->direct_io, WT_FILE_TYPE_LOG))
		log->allocsize =
		    WT_MAX((uint32_t)conn->buffer_alignment, LOG_ALIGN);
	else
		log->allocsize = LOG_ALIGN;
	INIT_LSN(&log->alloc_lsn);
	INIT_LSN(&log->ckpt_lsn);
	INIT_LSN(&log->first_lsn);
	INIT_LSN(&log->sync_lsn);
	/*
	 * We only use file numbers for directory sync, so this needs to
	 * initialized to zero.
	 */
	ZERO_LSN(&log->sync_dir_lsn);
	INIT_LSN(&log->trunc_lsn);
	INIT_LSN(&log->write_lsn);
	log->fileid = 0;
	WT_RET(__wt_cond_alloc(session, "log sync", 0, &log->log_sync_cond));
	WT_RET(__wt_log_open(session));
	WT_RET(__wt_log_slot_init(session));

	/* If archiving is not configured, we're done. */ 
	if (!conn->archive)
		return (0);

	/*
	 * If an archive thread exists, the user may have reconfigured the
	 * archive thread.  Signal the thread.  Otherwise the user wants
	 * archiving and we need to start up the thread.
	 */
	if (conn->arch_session != NULL) {
		WT_ASSERT(session, conn->arch_cond != NULL);
		WT_ASSERT(session, conn->arch_tid_set != 0);
		WT_RET(__wt_cond_signal(session, conn->arch_cond));
	} else {
		/* The log archive server gets its own session. */
		WT_RET(__wt_open_internal_session(
		    conn, "archive-server", 0, 0, &conn->arch_session));
		WT_RET(__wt_cond_alloc(conn->arch_session,
		    "log archiving server", 0, &conn->arch_cond));

		/*
		 * Start the thread.
		 */
		WT_RET(__wt_thread_create(conn->arch_session,
		    &conn->arch_tid, __log_archive_server, conn->arch_session));
		conn->arch_tid_set = 1;
	}

	return (0);
}

/*
 * __wt_logmgr_destroy --
 *	Destroy the log archiving server thread and logging subsystem.
 */
int
__wt_logmgr_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	if (!conn->logging)
		return (0);
	if (conn->arch_tid_set) {
		WT_TRET(__wt_cond_signal(session, conn->arch_cond));
		WT_TRET(__wt_thread_join(session, conn->arch_tid));
		conn->arch_tid_set = 0;
	}
	WT_TRET(__wt_cond_destroy(session, &conn->arch_cond));

	WT_TRET(__wt_log_close(session));

	__wt_free(session, conn->log_path);

	/* Close the server thread's session. */
	if (conn->arch_session != NULL) {
		wt_session = &conn->arch_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));
		conn->arch_session = NULL;
	}

	WT_TRET(__wt_log_slot_destroy(session));
	WT_TRET(__wt_cond_destroy(session, &conn->log->log_sync_cond));
	WT_TRET(__wt_rwlock_destroy(session, &conn->log->log_archive_lock));
	__wt_spin_destroy(session, &conn->log->log_lock);
	__wt_spin_destroy(session, &conn->log->log_slot_lock);
	__wt_spin_destroy(session, &conn->log->log_sync_lock);
	__wt_free(session, conn->log);

	return (ret);
}
