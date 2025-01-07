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
  int expect_ret = 0, int flags = 0)
{
    WT_SESSION *wt_session = reinterpret_cast<WT_SESSION *>(env.session);
    WT_LIVE_RESTORE_FS *lr_fs = env.lr_fs;
    WT_LIVE_RESTORE_FILE_HANDLE *lr_fh = nullptr;

    int ret = lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
      env.dest_file_path(file_name).c_str(), file_type, flags, (WT_FILE_HANDLE **)&lr_fh);
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

    SECTION("fs_open - File")
    {
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        // If the file doesn't exist return ENOENT.
        lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR, ENOENT);

        // However if we provide the WT_FS_OPEN_CREATE flag it will be created in the destination.
        lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR, 0, WT_FS_OPEN_CREATE);
        REQUIRE(testutil_exists(".", env.dest_file_path(file_1).c_str()));
        testutil_remove(env.dest_file_path(file_1).c_str());

        // The file only exists in the destination. Open is successful.
        create_file(env.dest_file_path(file_1));
        lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR);

        // The file only exists in the source. Open is successful.
        testutil_remove(env.dest_file_path(file_1).c_str());
        create_file(env.source_file_path(file_1));
        lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR);

        // The file exists in both source and destination. Open is successful.
        testutil_remove(env.dest_file_path(file_1).c_str());
        testutil_remove(env.source_file_path(file_1).c_str());

        create_file(env.dest_file_path(file_1));
        create_file(env.source_file_path(file_1));
        lr_fh = open_file(env, file_1, WT_FS_OPEN_FILE_TYPE_REGULAR);

        WT_UNUSED(lr_fh);
    }

    SECTION("fs_open - Directory")
    {
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        // The file doesn't exist. Return ENOENT.
        lr_fh = open_file(env, subfolder, WT_FS_OPEN_FILE_TYPE_DIRECTORY, ENOENT);

        // However if we provide the WT_FS_OPEN_CREATE flag it will be created in the destination.
        lr_fh = open_file(env, subfolder, WT_FS_OPEN_FILE_TYPE_DIRECTORY, 0, WT_FS_OPEN_CREATE);
        REQUIRE(testutil_exists(".", env.dest_file_path(subfolder).c_str()));
        testutil_remove(env.dest_file_path(subfolder).c_str());

        // The file only exists in the destination. Open is successful.
        testutil_mkdir(env.dest_file_path(subfolder).c_str());
        REQUIRE(testutil_exists(".", env.dest_file_path(subfolder).c_str()));
        lr_fh = open_file(env, subfolder, WT_FS_OPEN_FILE_TYPE_DIRECTORY);

        // The file only exists in the source. Open is successful and the directory is created in
        // the destination.
        testutil_remove(env.dest_file_path(subfolder).c_str());
        testutil_mkdir(env.source_file_path(subfolder).c_str());
        lr_fh = open_file(env, subfolder, WT_FS_OPEN_FILE_TYPE_DIRECTORY);
        REQUIRE(testutil_exists(".", env.dest_file_path(subfolder).c_str()));

        // The file exists in both source and destination. Open is successful.
        testutil_remove(env.dest_file_path(subfolder).c_str());
        testutil_remove(env.source_file_path(subfolder).c_str());

        testutil_mkdir(env.dest_file_path(subfolder).c_str());
        testutil_mkdir(env.source_file_path(subfolder).c_str());
        lr_fh = open_file(env, subfolder, WT_FS_OPEN_FILE_TYPE_DIRECTORY);

        WT_UNUSED(lr_fh);
    }
}
