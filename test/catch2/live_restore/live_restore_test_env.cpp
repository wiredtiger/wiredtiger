/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "live_restore_test_env.h"

#include <fstream>
#include <iostream>
namespace utils {

/*
 * This class sets up and tears down the testing environment for Live Restore. Developers are
 * expected to create the respective files in the these folders manually.
 */
live_restore_test_env::live_restore_test_env()
{
    // Clean up any pre-existing folders. Make sure an empty DB_SOURCE exists
    // as it need to exist to open the connection in live restore mode.
    testutil_recreate_dir(DB_DEST.c_str());
    testutil_recreate_dir(DB_SOURCE.c_str());

    static std::string cfg_string =
      "create=true,live_restore=(enabled=true, path=" + DB_SOURCE + ",threads_max=0)";
    conn = std::make_unique<connection_wrapper>(DB_DEST.c_str(), cfg_string.c_str());

    session = conn->create_session();
    lr_fs = (WTI_LIVE_RESTORE_FS *)conn->get_wt_connection_impl()->file_system;
    lr_fs->finished = false;
}

std::string
live_restore_test_env::dest_file_path(const std::string &file_name)
{
    return DB_DEST + "/" + file_name;
}

std::string
live_restore_test_env::source_file_path(const std::string &file_name)
{
    return DB_SOURCE + "/" + file_name;
}

std::string
live_restore_test_env::tombstone_file_path(const std::string &file_name)
{
    // Tombstone files only exist in the destination folder.
    return DB_DEST + "/" + file_name + WTI_LIVE_RESTORE_STOP_FILE_SUFFIX;
}

} // namespace utils.
