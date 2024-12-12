/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Tests of the Live Restore extent lists. This list tracks "holes" in a file representing ranges
 * where data needs to be read in from the source directory instead of the from the destination.
 * [live_restore_extent_list]
 */

#include "../utils_live_restore.h"

using namespace utils;

TEST_CASE("Live Restore Extent Lists: Creation", "[live_restore],[live_restore_extent_list]")
{
    // Slow: This code runs once per SECTION so we're creating a brand new test directory each time.
    live_restore_test_env env(false);

    WT_LIVE_RESTORE_FS *lr_fs = env._lr_fs;
    WT_SESSION_IMPL *session = env._session;
    WT_SESSION *wt_session = (WT_SESSION *)session;

    std::string file_name = "MY_FILE.txt";
    std::string source_file = env.source_file_path(file_name);
    std::string dest_file = env.dest_file_path(file_name);

    SECTION("Open a new unbacked file")
    {
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        lr_fs->iface.fs_open_file(&lr_fs->iface, wt_session, dest_file.c_str(),
          WT_FS_OPEN_FILE_TYPE_REGULAR, 0, (WT_FILE_HANDLE **)&lr_fh);

        // There's no backing file, so no extent list to track.
        REQUIRE(extent_list_str(lr_fh) == "");
    }

    SECTION("Open a new backed file")
    {
        create_file(source_file.c_str(), 1000);

        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);

        // We've created a new file in the destination backed by a file in source.
        // We haven't read or written anything so the file is one big hole.
        REQUIRE(extent_list_in_order(lr_fh));
        REQUIRE(extent_list_str(lr_fh) == "(0-999)");
    }

    SECTION("Open a backed file whose size differs from the source")
    {
        create_file(source_file.c_str(), 110);

        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);

        // We've created a new file in the destination backed by a file in source.
        // As we haven't read or written to the file it's one big hole.
        REQUIRE(extent_list_in_order(lr_fh));
        REQUIRE(extent_list_str(lr_fh) == "(0-109)");
    }

    SECTION("Hole list can't be larger than the dest file size")
    {
        // TODO
    }

    SECTION("Open a backed, complete file")
    {
        create_file(source_file.c_str(), 110);

        // Copy the file to DEST manually. This is a full copy.
        testutil_copy(source_file.c_str(), dest_file.c_str());

        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);
        REQUIRE(extent_list_str(lr_fh) == "");

        // We've tested when there's no file in the destination. Now test when there is a file in
        // the destination, but no content has been copied yet.
        lr_fh->iface.close((WT_FILE_HANDLE *)lr_fh, wt_session);
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);
        REQUIRE(extent_list_str(lr_fh) == "");
    }

    SECTION("Open a backed, partially copied file")
    {
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        char buf[4096];

        create_file(source_file.c_str(), 8192);
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);

        // Use a promote read to partially copy the file.
        lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, wt_session, 0, 4096, buf);

        // Close the file and reopen it to generate the extent list from holes in the dest file
        lr_fh->iface.close((WT_FILE_HANDLE *)lr_fh, wt_session);
        open_lr_fh(&env, dest_file.c_str(), &lr_fh);

        // We've written 4KB to the start of the file. There should only be a hole at the end.
        REQUIRE(extent_list_in_order(lr_fh));
        REQUIRE(extent_list_str(lr_fh) == "(4096-8191)");
    }
}
