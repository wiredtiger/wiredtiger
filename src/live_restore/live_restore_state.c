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
__live_restore_state_to_string(WT_LIVE_RESTORE_STATE state, char *state_strp)
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
  WT_SESSION_IMPL *session, char *state_str, WT_LIVE_RESTORE_STATE *statep)
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
 * __live_restore_get_state_from_file --
 *     Read the live restore state from the on-disk file. If it doesn't exist we return NONE. The
 *     caller must already hold the live restore state lock. This function takes a *non-live
 *     restore* file system, for example the backing posix file system used when accessing the
 *     source of destination directly.
 */
static int
__live_restore_get_state_from_file(WT_SESSION_IMPL *session, WT_FILE_SYSTEM *fs,
  const char *backing_folder, WT_LIVE_RESTORE_STATE *statep)
{
    WT_DECL_RET;

    WT_DECL_ITEM(state_file_name);

    WT_FILE_HANDLE *fh = NULL;

    WT_RET(__wt_scr_alloc(session, 0, &state_file_name));

    bool state_file_exists = false;

    WT_ERR(__wt_filename_construct(session, backing_folder, WT_LIVE_RESTORE_STATE_FILE, UINTMAX_MAX,
      UINT32_MAX, state_file_name));
    WT_ERR(
      fs->fs_exist(fs, (WT_SESSION *)session, (char *)state_file_name->data, &state_file_exists));

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

    if (state_file_name != NULL)
        __wt_scr_free(session, &state_file_name);

    return (ret);
}

/*
 * __wti_live_restore_set_state --
 *     Update the live restore state in memory and persist it to the on-disk state file.
 */
int
__wti_live_restore_set_state(
  WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs, WT_LIVE_RESTORE_STATE new_state)
{

    WT_DECL_RET;
    WT_FILE_HANDLE *fh = NULL;

    __wt_writelock(session, &lr_fs->state_lock);

    /* State should always be initialized on start up. If we ever try to set state without first
     * reading it something's gone wrong. */
    WT_ASSERT_ALWAYS(
      session, lr_fs->state != WTI_LIVE_RESTORE_STATE_NONE, "Live restore state not initialized!");

    /* Validity checking. There is a defined transition of states and we should never skip or repeat
     * a state. */
    switch (new_state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        /* We should never manually transition to NONE. This is a placeholder for when state is not
         * set. */
        WT_ASSERT(session, false);
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
    WT_ERR(__wt_scr_alloc(session, 0, &state_file_name));

    bool state_file_exists = false;

    WT_ERR(__wt_filename_construct(session, lr_fs->destination.home, WT_LIVE_RESTORE_STATE_FILE,
      UINTMAX_MAX, UINT32_MAX, state_file_name));
    WT_ERR(lr_fs->os_file_system->fs_exist(lr_fs->os_file_system, (WT_SESSION *)session,
      (char *)state_file_name->data, &state_file_exists));

    /* This should be created on live restore start up. If it's not present we've called set state
     * too early. */
    WT_ASSERT_ALWAYS(session, state_file_exists, "State file doesn't exist!");

    char state_to_write[128];
    __live_restore_state_to_string(new_state, state_to_write);
    WT_ERR(lr_fs->os_file_system->fs_open_file(lr_fs->os_file_system, (WT_SESSION *)session,
      (char *)state_file_name->data, WT_FS_OPEN_FILE_TYPE_REGULAR, WT_FS_OPEN_EXCLUSIVE, &fh));
    WT_ERR(fh->fh_write(fh, (WT_SESSION *)session, 0, 128, state_to_write));

    lr_fs->state = new_state;

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
    bool state_file_exists = false;
    WT_DECL_ITEM(state_file_name);

    __wt_writelock(session, &lr_fs->state_lock);

    WT_ASSERT_ALWAYS(session, lr_fs->state == WTI_LIVE_RESTORE_STATE_NONE,
      "Attempting to initialize already initialized state!");

    WT_RET(__wt_scr_alloc(session, 0, &state_file_name));

    WT_ERR(__wt_filename_construct(session, lr_fs->destination.home, WT_LIVE_RESTORE_STATE_FILE,
      UINTMAX_MAX, UINT32_MAX, state_file_name));
    WT_ERR(lr_fs->os_file_system->fs_exist(lr_fs->os_file_system, (WT_SESSION *)session,
      (char *)state_file_name->data, &state_file_exists));

    WT_LIVE_RESTORE_STATE state;

    WT_ERR(__live_restore_get_state_from_file(
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
 *     Get the live restore state. If it's not available in memory read it from the on-disk state
 *     file.
 */
WT_LIVE_RESTORE_STATE
__wti_live_restore_get_state(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WT_LIVE_RESTORE_STATE state;
    __wt_readlock(session, &lr_fs->state_lock);
    state = lr_fs->state;
    __wt_readunlock(session, &lr_fs->state_lock);

    /* We initialize state on startup. This shouldn't be possible. */
    WT_ASSERT_ALWAYS(session, state != WTI_LIVE_RESTORE_STATE_NONE, "State not initialized!");

    return (state);
}

/*
 * __wt_live_restore_delete_complete_state_file --
 *     Provided with a folder delete any live restore state file contained within, provided that the
 *     file is in the COMPLETE state. This function takes a *non-live restore* file system as it is
 *     expected to be called by non-live restore file systems after live restore has completed and
 *     WiredTiger has stated in non-live restore mode
 */
int
__wt_live_restore_delete_complete_state_file(
  WT_SESSION_IMPL *session, WT_FILE_SYSTEM *fs, const char *folder)
{
    WT_DECL_RET;

    WT_DECL_ITEM(lr_state_file);
    WT_RET(__wt_scr_alloc(session, 0, &lr_state_file));
    bool lr_state_file_exists = false;

    WT_ERR(__wt_filename_construct(
      session, folder, WT_LIVE_RESTORE_STATE_FILE, UINTMAX_MAX, UINT32_MAX, lr_state_file));
    WT_ERR(
      fs->fs_exist(fs, (WT_SESSION *)session, (char *)lr_state_file->data, &lr_state_file_exists));

    if (lr_state_file_exists) {
        WT_LIVE_RESTORE_STATE source_state;
        __live_restore_get_state_from_file(session, fs, folder, &source_state);
        if (source_state == WTI_LIVE_RESTORE_STATE_COMPLETE)
            WT_ERR(fs->fs_remove(fs, (WT_SESSION *)session, (char *)lr_state_file->data, 0));
    }

err:
    __wt_scr_free(session, &lr_state_file);
    return (ret);
}

/*
 * __wti_live_restore_validate_directories --
 *     Validate the source and destination directories are in a valid state on startup.
 */
int
__wti_live_restore_validate_directories(WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FS *lr_fs)
{
    WT_DECL_RET;

    char **dirlist_source = NULL;
    uint32_t num_source_files = 0;

    char **dirlist_dest = NULL;
    uint32_t num_dest_files = 0;

    /*
     * If live restore has completed but a backup is taken before WiredTiger is restarted in
     * non-live restore mode then the source may contain a state file in the COMPLETE state. This
     * can be ignored and deleted.
     */
    // TODO - this breaks the "source" is read-only assumption. Discuss this
    WT_RET(__wt_live_restore_delete_complete_state_file(
      session, lr_fs->os_file_system, lr_fs->source.home));

    /* First up: Check that the source doesn't contain any live restore metadata files. */
    WT_ERR(lr_fs->os_file_system->fs_directory_list(lr_fs->os_file_system, (WT_SESSION *)session,
      lr_fs->source.home, "", &dirlist_source, &num_source_files));

    if (num_source_files == 0) {
        WT_ERR_MSG(session, EINVAL, "Source directory is empty. Nothing to restore!");
    }

    for (uint32_t i = 0; i < num_source_files; ++i) {
        if (WT_SUFFIX_MATCH(dirlist_source[i], WTI_LIVE_RESTORE_STOP_FILE_SUFFIX) ||
          strcmp(dirlist_source[i], WT_LIVE_RESTORE_STATE_FILE) == 0) {
            WT_ERR_MSG(session, EINVAL,
              "Source directory contains live restore metadata file: %s. This implies it is a "
              "destination directory that hasn't finished restoration",
              dirlist_source[i]);
        }
    }

    /* Now check the destination folder */
    WT_LIVE_RESTORE_STATE state;
    __wt_readlock(session, &lr_fs->state_lock);
    WT_ERR(__live_restore_get_state_from_file(
      session, lr_fs->os_file_system, lr_fs->destination.home, &state));
    __wt_readunlock(session, &lr_fs->state_lock);

    WT_ERR(lr_fs->os_file_system->fs_directory_list(lr_fs->os_file_system, (WT_SESSION *)session,
      lr_fs->destination.home, "", &dirlist_dest, &num_dest_files));

    // TODO - run Sean's server test

    // TODO - make sure we have tests that restart during the log copy stage

    // TODO - New catch2 test explicitly for states

    switch (state) {
    case WTI_LIVE_RESTORE_STATE_NONE:
        /* This is a brand new live restore. The destination folder shouldn't contain anything. If
         * it does there's a risk we're overwriting a valid database. */
        if (num_dest_files > 0) {
            WT_ERR_MSG(session, EINVAL,
              "Live restore state is about to start but destination directory is not empty!");
        }
        break;
    case WTI_LIVE_RESTORE_STATE_LOG_COPY:
        for (uint32_t i = 0; i < num_dest_files; ++i) {
            if (!WT_SUFFIX_MATCH(dirlist_dest[i], ".log") &&
              strcmp(dirlist_dest[i], WT_LIVE_RESTORE_STATE_FILE) != 0)
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
    if (dirlist_source != NULL)
        WT_TRET(lr_fs->os_file_system->fs_directory_list_free(
          lr_fs->os_file_system, (WT_SESSION *)session, dirlist_source, num_source_files));

    if (dirlist_dest != NULL)
        WT_TRET(lr_fs->os_file_system->fs_directory_list_free(
          lr_fs->os_file_system, (WT_SESSION *)session, dirlist_dest, num_source_files));

    return (ret);
}
