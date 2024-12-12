/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "utils_live_restore.h"

#include <catch2/catch.hpp>
#include "../utils.h"
#include "wt_internal.h"
#include "test_util.h"

#include <iostream>
namespace utils {

LiveRestoreTestEnv::LiveRestoreTestEnv(bool verbose)
{

    // Clean up any pre-existing folders. Make sure an empty DB_SRC exists as we need it for
    // connection_open.
    testutil_remove(_DB_DEST.c_str());
    testutil_remove(_DB_HACKY_BACKUP.c_str());
    testutil_recreate_dir(_DB_SOURCE.c_str());

    // We're using a connection to setup the file system and let us print WT traces, but
    // all of our tests will use empty folders where we create files manually.
    // The issue here is wiredtiger_open will create metadata and turtle files on open
    // and expect that it needs to remove them on close. Move these files to a temp location.
    static std::string cfg_string = "live_restore=(enabled=true, path=" + _DB_SOURCE + ")";
    _conn = std::make_unique<connection_wrapper>(_DB_DEST.c_str(), cfg_string.c_str());
    testutil_copy(_DB_DEST.c_str(), _DB_HACKY_BACKUP.c_str());
    testutil_recreate_dir(_DB_DEST.c_str());

    _session = _conn->create_session();
    _lr_fs = (WT_LIVE_RESTORE_FS *)_conn->get_wt_connection_impl()->file_system;
}

LiveRestoreTestEnv::~LiveRestoreTestEnv()
{
    // Clean up directories on close.
    testutil_remove(_DB_SOURCE.c_str());
    testutil_move(_DB_HACKY_BACKUP.c_str(), _DB_DEST.c_str());
}

std::string
LiveRestoreTestEnv::dest_file_path(std::string file_name)
{
    return _DB_DEST + "/" + file_name;
}

std::string
LiveRestoreTestEnv::source_file_path(std::string file_name)
{
    return _DB_SOURCE + "/" + file_name;
}

} // namespace utils.
