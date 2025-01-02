/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Tests of the Live Restore file systems, directory list functions.
 * These functions report which files exist in the unified live restore directory, hiding whether they're in the destination, source, or both backing directories.
 * [live_restore_directory_list]
 */

#include "../utils_live_restore.h"
#include <set>

using namespace utils;

// Require that a directory_list call using `prefix` returns exactly the list in `expected_files`.
static bool directory_list_is(live_restore_test_env& env, const std::string& prefix, const std::set<std::string>& expected_files) {

    WT_SESSION *wt_session = reinterpret_cast<WT_SESSION *>(env.session);
    WT_LIVE_RESTORE_FS *lr_fs = env.lr_fs;

    char **dirlist = NULL;
    uint32_t count;

    lr_fs->iface.fs_directory_list(
        (WT_FILE_SYSTEM *)lr_fs, wt_session, env.DB_DEST.c_str(), prefix.c_str(), &dirlist, &count);

    REQUIRE(count == expected_files.size());
    std::set<std::string> found_files{};
    for (int i = 0; i < count; i++) {
        std::string found_file(dirlist[i]);
        found_files.insert(found_file);
    }

    bool match = found_files == expected_files;
    lr_fs->iface.fs_directory_list_free((WT_FILE_SYSTEM *)lr_fs, wt_session, dirlist, count);
    return match;

}

TEST_CASE("Live Restore Directory List", "[live_restore],[live_restore_directory_list]")
{
    /*
     * Note: this code runs once per SECTION so we're creating a brand new WT database for each
     * section. If this gets slow we can make it static and manually clear the destination and
     * source for each section.
     */
    live_restore_test_env env;

    std::string file_1 = "file1.txt";
    std::string file_2 = "file2.txt";
    std::string file_3 = "file3.txt";
    std::string file_4 = "file4.txt";

    std::string subfolder = "subfolder";
    std::string subfolder_dest_path = env.DB_DEST + "/" + subfolder;
    std::string subfolder_source_path = env.DB_SOURCE + "/" + subfolder;

    SECTION("Directory list - Test when files only exist in the destination")
    {
        // Start with an empty directory.
        REQUIRE(directory_list_is(env, "", {}));

        // Progressively add files
        create_file(env.dest_file_path(file_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1}));

        create_file(env.dest_file_path(file_2).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2}));

        create_file(env.dest_file_path(file_3).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.dest_file_path(file_2).c_str());
        REQUIRE(directory_list_is(env, "", {file_1, file_3}));

        testutil_remove(env.dest_file_path(file_1).c_str());
        REQUIRE(directory_list_is(env, "", {file_3}));

        testutil_remove(env.dest_file_path(file_3).c_str());
        REQUIRE(directory_list_is(env, "", {}));
    }

    SECTION("Directory list - Test when files only exist in the source")
    {
        // Start with an empty directory.
        REQUIRE(directory_list_is(env, "", {}));

        // Progressively add files
        create_file(env.source_file_path(file_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1}));

        create_file(env.source_file_path(file_2).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2}));

        create_file(env.source_file_path(file_3).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.source_file_path(file_2).c_str());
        REQUIRE(directory_list_is(env, "", {file_1, file_3}));

        testutil_remove(env.source_file_path(file_1).c_str());
        REQUIRE(directory_list_is(env, "", {file_3}));

        testutil_remove(env.source_file_path(file_3).c_str());
        REQUIRE(directory_list_is(env, "", {}));
    }

    SECTION("Directory list - Test when files exist in both source and destination")
    {
        // Start with an empty directory.
        REQUIRE(directory_list_is(env, "", {}));

        // Progressively add files
        create_file(env.dest_file_path(file_1).c_str(), 1000);
        create_file(env.source_file_path(file_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1}));

        create_file(env.dest_file_path(file_2).c_str(), 1000);
        create_file(env.source_file_path(file_2).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2}));

        create_file(env.dest_file_path(file_3).c_str(), 1000);
        create_file(env.source_file_path(file_3).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.dest_file_path(file_2).c_str());
        testutil_remove(env.source_file_path(file_2).c_str());
        REQUIRE(directory_list_is(env, "", {file_1, file_3}));

        testutil_remove(env.dest_file_path(file_1).c_str());
        testutil_remove(env.source_file_path(file_1).c_str());
        REQUIRE(directory_list_is(env, "", {file_3}));

        testutil_remove(env.dest_file_path(file_3).c_str());
        testutil_remove(env.source_file_path(file_3).c_str());
        REQUIRE(directory_list_is(env, "", {}));
    }

    SECTION("Directory list - Test when files exist either in source or destination, but not both")
    {
        // Add one file to the source.
        create_file(env.source_file_path(file_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1}));

        // And now the destination.
        create_file(env.dest_file_path(file_2).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2}));
    }

    SECTION("Directory list - Test a file isn't reported when there's a tombstone in the destination")
    {
        // Add some files to the source.
        create_file(env.source_file_path(file_1).c_str(), 1000);
        create_file(env.source_file_path(file_2).c_str(), 1000);
        create_file(env.source_file_path(file_3).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_2, file_3}));

        // Now progressively add tombstones. The files are no longer reported.
        create_file(env.tombstone_file_path(file_2).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_1, file_3}));

        create_file(env.tombstone_file_path(file_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {file_3}));

        create_file(env.tombstone_file_path(file_3).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {}));

        // Now add the tombstone before the file to confirm it isn't reported.
        create_file(env.tombstone_file_path(file_4).c_str(), 1000);
        create_file(env.source_file_path(file_4).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {}));
    }

    SECTION("Directory list - Test directory list reports subfolders")
    {
        // Only in the destination
        testutil_mkdir(subfolder_dest_path.c_str());
        REQUIRE(directory_list_is(env, "", {subfolder}));

        // And then deleted
        testutil_remove(subfolder_dest_path.c_str());
        REQUIRE(directory_list_is(env, "", {}));

        // Only in the source
        testutil_mkdir(subfolder_source_path.c_str());
        REQUIRE(directory_list_is(env, "", {subfolder}));

        // Now in both
        testutil_mkdir(subfolder_dest_path.c_str());
        REQUIRE(directory_list_is(env, "", {subfolder}));

        // Check that we *don't* report the contents, just the subfolder itself
        std::string subfile_1 = subfolder + "/" + file_1;
        create_file(env.dest_file_path(subfile_1).c_str(), 1000);
        REQUIRE(directory_list_is(env, "", {subfolder}));
    }
}
