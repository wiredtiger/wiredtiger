/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Tests of the Live Restore file systems fs_open function. [live_restore_fs_open]
 */

#include "../utils_live_restore.h"

using namespace utils;

static WT_LIVE_RESTORE_FILE_HANDLE *
open_file(live_restore_test_env &env, std::string file_name, WT_FS_OPEN_FILE_TYPE file_type,
  int expect_ret = 0)
{
    WT_SESSION *wt_session = reinterpret_cast<WT_SESSION *>(env.session);
    WT_LIVE_RESTORE_FS *lr_fs = env.lr_fs;
    WT_LIVE_RESTORE_FILE_HANDLE *lr_fh = nullptr;

    int ret = lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
      env.dest_file_path(file_name).c_str(), file_type, 0, (WT_FILE_HANDLE **)&lr_fh);
    REQUIRE(ret == expect_ret);

    return lr_fh;
}

TEST_CASE("Live Restore fs_open", "[live_restore],[live_restore_fs_open]")
{
    /*
     * Note: this code runs once per SECTION so we're creating a brand new WT database for each
     * section. If this gets slow we can make it static and manually clear the destination and
     * source for each section.
     */
    live_restore_test_env env;

    std::string file_1 = "file1.txt";
    std::string subfolder = "subfolder";

    SECTION("fs_open")
    {
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR, WT_NOTFOUND);
        WT_UNUSED(lr_fh);
    }
}
