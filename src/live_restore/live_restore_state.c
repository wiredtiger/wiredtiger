/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include "live_restore_private.h"

/*
 * __live_restore_state_to_string --
 *     Convert a live restore state to its string representation.
 */
static void
__live_restore_state_to_string(WTI_LIVE_RESTORE_STATE state, char *state_strp)
{
    switch (state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        strcpy(state_strp, "WTI_LIVE_RESTORE_STATE_NONE");
        break;
    case WTI_LIVE_RESTORE_STATE_LOG_COPY:
        strcpy(state_strp, "WTI_LIVE_RESTORE_STATE_LOG_COPY");
        break;
    case WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION:
        strcpy(state_strp, "WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION");
        break;
    case WTI_LIVE_RESTORE_STATE_CLEAN_UP:
        strcpy(state_strp, "WTI_LIVE_RESTORE_STATE_CLEAN_UP");
        break;
    case WTI_LIVE_RESTORE_STATE_COMPLETE:
        strcpy(state_strp, "WTI_LIVE_RESTORE_STATE_COMPLETE");
        break;
    }
}

/*
 * __live_restore_state_from_string --
 *     Convert a string to its live restore state.
 */
static int
__live_restore_state_from_string(
  WT_SESSION_IMPL *session, char *state_str, WTI_LIVE_RESTORE_STATE *statep)
{

    if (strcmp(state_str, "WTI_LIVE_RESTORE_STATE_NONE") == 0)
        *statep = WTI_LIVE_RESTORE_STATE_NONE;
    else if (strcmp(state_str, "WTI_LIVE_RESTORE_STATE_LOG_COPY") == 0)
        *statep = WTI_LIVE_RESTORE_STATE_LOG_COPY;
    else if (strcmp(state_str, "WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION") == 0)
        *statep = WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION;
    else if (strcmp(state_str, "WTI_LIVE_RESTORE_STATE_CLEAN_UP") == 0)
        *statep = WTI_LIVE_RESTORE_STATE_CLEAN_UP;
    else if (strcmp(state_str, "WTI_LIVE_RESTORE_STATE_COMPLETE") == 0)
        *statep = WTI_LIVE_RESTORE_STATE_COMPLETE;
    else
        WT_RET_MSG(session, EINVAL, "Invalid state string: '%s' ", state_str);

    return (0);
}

/*
 * __live_restore_get_state_file_path --
 *     Given a directory return the path of a live restore state file inside the directory and
 *     whether the file exists.
 */
static int
__live_restore_get_state_file_path(WT_SESSION_IMPL *session, const char *directory,
  WT_FILE_SYSTEM *fs, WT_ITEM **state_file_namep, bool *file_existsp)
{
    WT_DECL_RET;

    WT_RET(__wt_scr_alloc(session, 0, state_file_namep));
    WT_ERR(__wt_filename_construct(
      session, directory, WTI_LIVE_RESTORE_STATE_FILE, UINTMAX_MAX, UINT32_MAX, *state_file_namep));

    if (file_existsp != NULL) {
        WT_ERR(
          fs->fs_exist(fs, (WT_SESSION *)session, (char *)(*state_file_namep)->data, file_existsp));
    }

    if (0) {
err:
        __wt_scr_free(session, state_file_namep);
    }

    return (ret);
}

/*
 * __live_restore_get_state_from_file --
 *     Read the live restore state from the on-disk file. If it doesn't exist we return NONE. The
 *     caller must already hold the live restore state lock. This function takes a *non-live
 *     restore* file system, for example, the backing posix file system, used when accessing the
 *     source or destination directly.
 */
static int
__live_restore_get_state_from_file(WT_SESSION_IMPL *session, WT_FILE_SYSTEM *fs,
  const char *backing_folder, WTI_LIVE_RESTORE_STATE *statep)
{
    WT_DECL_RET;

    WT_DECL_ITEM(state_file_name);
    WT_FILE_HANDLE *fh = NULL;
    bool state_file_exists = false;

    WT_ERR(__live_restore_get_state_file_path(
      session, backing_folder, fs, &state_file_name, &state_file_exists));

    if (!state_file_exists)
        *statep = WTI_LIVE_RESTORE_STATE_NONE;
    else {
        char state_str[128];

        WT_ERR(fs->fs_open_file(fs, (WT_SESSION *)session, (char *)state_file_name->data,
          WT_FS_OPEN_FILE_TYPE_REGULAR, WT_FS_OPEN_EXCLUSIVE, &fh));

        wt_off_t file_size;
        WT_ERR(fs->fs_size(fs, (WT_SESSION *)session, (char *)state_file_name->data, &file_size));
        WT_ERR(fh->fh_read(fh, (WT_SESSION *)session, 0, (size_t)file_size, state_str));

        WT_ERR(__live_restore_state_from_string(session, state_str, statep));
    }

err:
    if (fh != NULL)
        WT_TRET(fh->close(fh, &session->iface));

    __wt_scr_free(session, &state_file_name);

    return (ret);
}

/*
 * __live_restore_report_state_to_application --
 *     WiredTiger reports a simplified live restore state to the application which lets it know it
 *     can restart on completion of live restore.
 */
static void
__live_restore_report_state_to_application(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_STATE state)
{
    switch (state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_INIT);
        break;
    case WTI_LIVE_RESTORE_STATE_LOG_COPY:
    case WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION:
    case WTI_LIVE_RESTORE_STATE_CLEAN_UP:
        WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_IN_PROGRESS);
        break;
    case WTI_LIVE_RESTORE_STATE_COMPLETE:
        WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_COMPLETE);
        break;
    }
}

/*
 * __wti_live_restore_set_state --
 *     Update the live restore state in memory and persist it to the on-disk state file.
 */
int
__wti_live_restore_set_state(
  WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs, WTI_LIVE_RESTORE_STATE new_state)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh = NULL;
    bool state_file_exists = false;

    __wt_writelock(session, &lr_fs->state_lock);

    /*
     * State should always be initialized on start up. If we ever try to set state without first
     * reading it something's gone wrong.
     */
    WT_ASSERT_ALWAYS(
      session, lr_fs->state != WTI_LIVE_RESTORE_STATE_NONE, "Live restore state not initialized!");

    /*
     * Validity checking. There is a defined transition of states and we should never skip or repeat
     * a state.
     */
    switch (new_state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        /*  We should never transition to NONE. This is a placeholder when state is not set. */
        WT_ASSERT_ALWAYS(session, false, "Attempting to set Live Restore state to NONE!");
        break;
    case WTI_LIVE_RESTORE_STATE_LOG_COPY:
        WT_ASSERT(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_NONE);
        break;
    case WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION:
        WT_ASSERT(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_LOG_COPY);
        break;
    case WTI_LIVE_RESTORE_STATE_CLEAN_UP:
        WT_ASSERT(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION);
        break;
    case WTI_LIVE_RESTORE_STATE_COMPLETE:
        WT_ASSERT(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_CLEAN_UP);
        break;
    }

    WT_DECL_ITEM(state_file_name);
    WT_ERR(__live_restore_get_state_file_path(session, lr_fs->destination.home,
      lr_fs->os_file_system, &state_file_name, &state_file_exists));

    /*
     * The state file is either already present or created on live restore initialization. If it's
     * not present we've called set state too early.
     */
    WT_ASSERT_ALWAYS(session, state_file_exists, "State file doesn't exist!");

    char state_to_write[128];
    __live_restore_state_to_string(new_state, state_to_write);
    WT_ERR(lr_fs->os_file_system->fs_open_file(lr_fs->os_file_system, (WT_SESSION *)session,
      (char *)state_file_name->data, WT_FS_OPEN_FILE_TYPE_REGULAR, WT_FS_OPEN_EXCLUSIVE, &fh));
    WT_ERR(fh->fh_write(fh, (WT_SESSION *)session, 0, 128, state_to_write));

    lr_fs->state = new_state;
    __live_restore_report_state_to_application(session, new_state);

err:
    __wt_writeunlock(session, &lr_fs->state_lock);

    if (fh != NULL)
        WT_TRET(fh->close(fh, &session->iface));

    __wt_scr_free(session, &state_file_name);

    return (ret);
}

/*
 * __wti_live_restore_init_state --
 *     Initialize the live restore state. Read the state from file if it exists, otherwise we start
 *     in the log copy state and need to create the file on disk.
 */
int
__wti_live_restore_init_state(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh = NULL;

    __wt_writelock(session, &lr_fs->state_lock);

    WT_ASSERT_ALWAYS(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_NONE,
      "Attempting to initialize already initialized state!");

    WT_DECL_ITEM(state_file_name);
    WTI_LIVE_RESTORE_STATE state;
    WT_RET(__live_restore_get_state_from_file(
      session, lr_fs->os_file_system, lr_fs->destination.home, &state));

    if (state != WTI_LIVE_RESTORE_STATE_NONE) {
        lr_fs->state = state;
    } else {
        /*
         * The state file doesn't exist which means we're starting a brand new live restore. Create
         * the state file in the log copy state.
         */
        char state_to_write[128];
        __live_restore_state_to_string(WTI_LIVE_RESTORE_STATE_LOG_COPY, state_to_write);

        WT_ERR(__live_restore_get_state_file_path(
          session, lr_fs->destination.home, NULL, &state_file_name, NULL));

        WT_ERR(lr_fs->os_file_system->fs_open_file(lr_fs->os_file_system, (WT_SESSION *)session,
          (char *)state_file_name->data, WT_FS_OPEN_FILE_TYPE_REGULAR,
          WT_FS_OPEN_CREATE | WT_FS_OPEN_EXCLUSIVE, &fh));

        fh->fh_write(fh, (WT_SESSION *)session, 0, 128, state_to_write);

        lr_fs->state = WTI_LIVE_RESTORE_STATE_LOG_COPY;
    }

err:

    __wt_writeunlock(session, &lr_fs->state_lock);
    __wt_scr_free(session, &state_file_name);

    if (fh != NULL)
        WT_TRET(fh->close(fh, &session->iface));

    return (ret);
}

/*
 * __wti_live_restore_get_state --
 *     Get the live restore state.
 */
WTI_LIVE_RESTORE_STATE
__wti_live_restore_get_state(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WTI_LIVE_RESTORE_STATE state;
    __wt_readlock(session, &lr_fs->state_lock);
    state = lr_fs->state;
    __wt_readunlock(session, &lr_fs->state_lock);

    /* We initialize state on startup. This shouldn't be possible. */
    WT_ASSERT_ALWAYS(session, state != WTI_LIVE_RESTORE_STATE_NONE, "State not initialized!");

    return (state);
}

/*
 * __wti_live_restore_get_state_no_lock --
 *     Get the live restore state without taking a lock. The caller must hold the state lock when
 *     calling this function.
 */
WTI_LIVE_RESTORE_STATE
__wti_live_restore_get_state_no_lock(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WTI_LIVE_RESTORE_STATE state = lr_fs->state;

    /* We initialize state on startup. This shouldn't be possible. */
    WT_ASSERT_ALWAYS(session, state != WTI_LIVE_RESTORE_STATE_NONE, "State not initialized!");

    return (state);
}

/*
 * __wt_live_restore_delete_complete_state_file --
 *     If the state file in the given directory is in the COMPLETE state, then it can be deleted.
 *     This function takes a non-live restore backing file system.
 */
int
__wt_live_restore_delete_complete_state_file(
  WT_SESSION_IMPL *session, WT_FILE_SYSTEM *fs, const char *directory)
{
    WT_DECL_RET;

    WT_DECL_ITEM(lr_state_file);
    bool lr_state_file_exists = false;
    WT_ERR(__live_restore_get_state_file_path(
      session, directory, fs, &lr_state_file, &lr_state_file_exists));

    if (lr_state_file_exists) {
        WTI_LIVE_RESTORE_STATE source_state;
        __live_restore_get_state_from_file(session, fs, directory, &source_state);
        if (source_state == WTI_LIVE_RESTORE_STATE_COMPLETE)
            WT_ERR(fs->fs_remove(fs, (WT_SESSION *)session, (char *)lr_state_file->data, 0));
    }

err:
    __wt_scr_free(session, &lr_state_file);
    return (ret);
}

/*
 * __wti_live_restore_validate_directories --
 *     Validate the source and destination directories are in the correct state on startup.
 */
int
__wti_live_restore_validate_directories(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WT_DECL_RET;

    char **dirlist_source = NULL;
    uint32_t num_source_files = 0;

    char **dirlist_dest = NULL;
    uint32_t num_dest_files = 0;

    /* First check that the source doesn't contain any live restore metadata files. */
    WT_ERR(lr_fs->os_file_system->fs_directory_list(lr_fs->os_file_system, (WT_SESSION *)session,
      lr_fs->source.home, "", &dirlist_source, &num_source_files));

    if (num_source_files == 0) {
        WT_ERR_MSG(session, EINVAL, "Source directory is empty. Nothing to restore!");
    }

    for (uint32_t i = 0; i < num_source_files; ++i) {
        if (WT_SUFFIX_MATCH(dirlist_source[i], WTI_LIVE_RESTORE_STOP_FILE_SUFFIX)) {
            WT_ERR_MSG(session, EINVAL,
              "Source directory contains live restore stop file: %s. This implies it is a "
              "destination directory that hasn't finished restoration",
              dirlist_source[i]);
        }

        /*
         * FIXME-WT-14107 For now the validation check ignores a state file in the COMPLETE state.
         * On completion of WT-14017 we can instead error out when a state file is found in the
         * source folder.
         */
        if (strcmp(dirlist_source[i], WTI_LIVE_RESTORE_STATE_FILE) == 0) {
            WTI_LIVE_RESTORE_STATE state;
            WT_ERR(__live_restore_get_state_from_file(
              session, lr_fs->os_file_system, lr_fs->source.home, &state));
            if (state != WTI_LIVE_RESTORE_STATE_COMPLETE)
                WT_ERR_MSG(session, EINVAL,
                  "Source directory contains live restore state file %s that is not in the "
                  "complete state. This implies it is a destination directory that hasn't finished "
                  "restoration",
                  dirlist_source[i]);
        }
    }

    /* Now check the destination folder */
    WTI_LIVE_RESTORE_STATE state;
    __wt_readlock(session, &lr_fs->state_lock);
    WT_ERR(__live_restore_get_state_from_file(
      session, lr_fs->os_file_system, lr_fs->destination.home, &state));
    __wt_readunlock(session, &lr_fs->state_lock);

    WT_ERR(lr_fs->os_file_system->fs_directory_list(lr_fs->os_file_system, (WT_SESSION *)session,
      lr_fs->destination.home, "", &dirlist_dest, &num_dest_files));

    switch (state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        /*
         * Ideally we'd prevent live restore from starting when there are any files already present
         * in the destination, but we can't control for everything that the user might put into the
         * folder. Instead only check for WiredTiger files.
         */
        for (uint32_t i = 0; i < num_dest_files; ++i) {
            if (WT_PREFIX_MATCH(dirlist_dest[i], WT_WIREDTIGER) ||
              WT_SUFFIX_MATCH(dirlist_dest[i], ".wt"))
                WT_ERR_MSG(session, EINVAL,
                  "Attempting to begin a live restore on a directory that already contains "
                  "WiredTiger files '%s'! It's possible this file will be overwritten.",
                  dirlist_dest[i]);
        }
        break;
    case WTI_LIVE_RESTORE_STATE_LOG_COPY:
        for (uint32_t i = 0; i < num_dest_files; ++i) {
            if (!WT_SUFFIX_MATCH(dirlist_dest[i], ".log") &&
              strcmp(dirlist_dest[i], WTI_LIVE_RESTORE_STATE_FILE) != 0)
                WT_ERR_MSG(session, EINVAL,
                  "Live restore state is in log copy phase but the destination contains files "
                  "other than logs or the state file: %s",
                  dirlist_dest[i]);
        }
        break;
    case WTI_LIVE_RESTORE_STATE_BACKGROUND_MIGRATION:
    case WTI_LIVE_RESTORE_STATE_CLEAN_UP:
        /* There's no invalid state to check in these cases. */
        break;
    case WTI_LIVE_RESTORE_STATE_COMPLETE:
        for (uint32_t i = 0; i < num_dest_files; ++i) {
            if (WT_SUFFIX_MATCH(dirlist_dest[i], WTI_LIVE_RESTORE_STOP_FILE_SUFFIX))
                WT_ERR_MSG(session, EINVAL,
                  "Live restore is complete but live restore stop file '%s' still exists!",
                  dirlist_dest[i]);
        }
        break;
    }

err:
    WT_TRET(lr_fs->os_file_system->fs_directory_list_free(
      lr_fs->os_file_system, (WT_SESSION *)session, dirlist_source, num_source_files));

    WT_TRET(lr_fs->os_file_system->fs_directory_list_free(
      lr_fs->os_file_system, (WT_SESSION *)session, dirlist_dest, num_dest_files));

    return (ret);
}

/*
 * __wt_live_restore_init_stats --
 *     Initialize the live restore stats.
 */
void
__wt_live_restore_init_stats(WT_SESSION_IMPL *session)
{
    if (F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS)) {
        /*
         * The live restore external state is known on initialization, but at that time the stat
         * server hasn't begun so we can't actually set the state. This must be called after the
         * stat server starts.
         */
        WTI_LIVE_RESTORE_FS *lr_fs = ((WTI_LIVE_RESTORE_FS *)S2C(session)->file_system);
        WTI_LIVE_RESTORE_STATE state = __wti_live_restore_get_state(session, lr_fs);
        __live_restore_report_state_to_application(session, state);
    }
}
