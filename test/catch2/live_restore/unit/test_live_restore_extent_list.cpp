/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * FIXME-WT-13871 - Expand on this comment.
 * [live_restore_extent_list]
 */

#include <catch2/catch.hpp>
#include <iostream>
#include <fstream>

extern "C" {
#include "wt_internal.h"
#include "test_util.h"
#include "../../../../src/live_restore/live_restore_private.h"
}

#include "../../utils.h"
#include "../utils_live_restore_extent_list.h"
#include "../../wrappers/config_parser.h"
#include "../../wrappers/mock_session.h"
#include "../../wrappers/connection_wrapper.h"

using namespace utils;

/* FIXME-WT-13871 Move helper functions into the util file. */

/* TO DISCUSS - these string reps are inclusive byte ranges on both ends. 0-10 means there are 11
 * bytes (0 up to and including 10) that are in the hole. Is this intuitive for devs new to the test
 * code?
 */
std::string
build_str_from_extent(WT_LIVE_RESTORE_HOLE_NODE *ext)
{
    std::string str = "";
    while (ext != NULL) {
        str += "(" + std::to_string(ext->off) + "-" + std::to_string(WT_EXTENT_END(ext)) + "), ";
        ext = ext->next;
    }

    // Remove the trailing ", "
    if (str.size() > 0) {
        str.erase(str.size() - 2);
    }
    return str;
}

void
populate_file(std::string filepath)
{
    std::fstream file(filepath, std::ios::in | std::ios::out | std::ios::ate);

    testutil_assert(file);

    std::streampos offset = 10;
    file.seekp(offset);

    std::string data = "abcdef";
    file.write(data.c_str(), data.length());

    file.close();
}

/* Write to a range in a file. */
void
write_to_file(std::string filepath, int len)
{
    std::ofstream file(filepath, std::ios::out);
    std::string data_str = std::string(len, 'A');
    file << data_str;
    file.close();
}

TEST_CASE("Live Restore Extent Lists: XXXXXXX", "[live_restore_extent_list]")
{

    // catch2 wants to run this code once for every section. I want it run once on startup.
    // Compromise: Make everything static and get what I want.
    static bool setupDone = false;
    static const std::string DB_DEST = "WT_DEST_DIR";
    static const std::string DB_SOURCE = "WT_LR_SOURCE";

    static WT_LIVE_RESTORE_FS *lr_fs;

    testutil_recreate_dir(DB_SOURCE.c_str());
    static std::string cfg_string = "live_restore=(enabled=true, path=" + DB_SOURCE + ")";
    static connection_wrapper conn(DB_DEST.c_str(), cfg_string.c_str());
    conn.clear_do_cleanup();

    static WT_SESSION_IMPL *session = NULL;
    static WT_SESSION *wt_session = NULL;

    if (!setupDone) {
        // During startup we'll create a bunch of WT files and then delete them so we can test just
        // the live restore system with dummy files. As such don't turn on verbose until after we've
        // opened the connection to remove junk traces for creating the metadata/turtle files etc.
        // std::string verbose_cfg = "verbose=(fileops:3)";
        // conn.get_wt_connection()->reconfigure(conn.get_wt_connection(), verbose_cfg.c_str());

        // Make sure DB_DEST doesn't exist (we're creating it) and DB_SOURCE exists.
        testutil_remove(DB_DEST.c_str());
        testutil_recreate_dir(DB_SOURCE.c_str());

        session = conn.create_session();
        wt_session = &session->iface;

        lr_fs = (WT_LIVE_RESTORE_FS *)conn.get_wt_connection_impl()->file_system;

        setupDone = true;
    }

    // Clear out any previous files from creation or prior SECTIONS
    testutil_recreate_dir(DB_DEST.c_str());
    testutil_recreate_dir(DB_SOURCE.c_str());

    SECTION("Open a new unbacked file")
    {
        std::string dest_file = DB_DEST + "/MY_FILE.txt";

        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
          (DB_DEST + "/" + "MY_FILE.txt").c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
          (WT_FILE_HANDLE **)&lr_fh);

        // There's no back file and so no extent list to track. It will be null
        REQUIRE(lr_fh->destination.hole_list_head == nullptr);
    }

    SECTION("Open a new backed file")
    {

        std::string source_file = DB_SOURCE + "/MY_FILE.txt";
        std::string dest_file = DB_DEST + "/MY_FILE.txt";

        // Create the backing file
        testutil_touch_file(source_file.c_str());
        write_to_file(source_file.c_str(), 110);

        // Open the now-backed file
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
          (DB_DEST + "/" + "MY_FILE.txt").c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
          (WT_FILE_HANDLE **)&lr_fh);

        WT_LIVE_RESTORE_HOLE_NODE *ext = lr_fh->destination.hole_list_head;
        std::string extent_string = build_str_from_extent(ext);
        // We've created a new file in destination backed by a file in source.
        // The hole list is one big hole the entire size of the source file.
        std::string expected_extent = "(0-109)";
        REQUIRE(extent_string == expected_extent);
    }

    SECTION("Open a backed, complete file")
    {

        std::string source_file = DB_SOURCE + "/MY_FILE.txt";
        std::string dest_file = DB_DEST + "/MY_FILE.txt";

        // Create the backing file
        testutil_touch_file(source_file.c_str());
        write_to_file(source_file.c_str(), 110);

        // Copy the file to DEST manually
        testutil_copy(source_file.c_str(), dest_file.c_str());

        // Open the now-backed file
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
          (DB_DEST + "/" + "MY_FILE.txt").c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
          (WT_FILE_HANDLE **)&lr_fh);

        WT_LIVE_RESTORE_HOLE_NODE *ext = lr_fh->destination.hole_list_head;
        REQUIRE(ext == NULL);
    }

    // FIXME-WT-13971 The fils system will always write a minimum block size (typically 4KB) even if
    // we only write a single byte. This means the minimum write size for users of live restore FS
    // must always write at least these many bytes. Make sure we have code to enforce this before
    // merging this branch.
    SECTION("Open a backed, partially copied file")
    {

        std::string source_file = DB_SOURCE + "/MY_FILE.txt";
        std::string dest_file = DB_DEST + "/MY_FILE.txt";

        // Create the backing file
        testutil_touch_file(source_file.c_str());
        write_to_file(source_file.c_str(), 8192);

        // Partially copy the file.
        // NOTE: Using promote_read for this. Is this an issue with dogfooding?
        WT_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
          (DB_DEST + "/" + "MY_FILE.txt").c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
          (WT_FILE_HANDLE **)&lr_fh);

        char buf[4096];
        lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, wt_session, 0, 4096, buf);

        // Close the file and reopen it to generate the extent list from holes in the dest file
        lr_fh->iface.close((WT_FILE_HANDLE *)lr_fh, wt_session);
        lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session,
          (DB_DEST + "/" + "MY_FILE.txt").c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
          (WT_FILE_HANDLE **)&lr_fh);

        // We've written 4KB to the start of the file. There should only be a hole at the end.
        WT_LIVE_RESTORE_HOLE_NODE *ext = lr_fh->destination.hole_list_head;
        std::string extent_string = build_str_from_extent(ext);
        std::string expected_extent = "(4096-8191)";
        REQUIRE(extent_string == expected_extent);
    }

    // Clean up
    testutil_remove(DB_DEST.c_str());
    testutil_remove(DB_SOURCE.c_str());
}
