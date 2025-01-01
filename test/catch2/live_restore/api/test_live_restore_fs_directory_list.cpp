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

static bool all_expected_files_found(live_restore_test_env& env, const std::string& prefix, const std::set<std::string>& expected_files) {

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

    SECTION("Directory list - Test files that only exist in the destination")
    {
        std::string file_1 = "file1.txt";
        std::string file_2 = "file2.txt";
        std::string file_3 = "file3.txt";

        // Start with an empty directory.
        REQUIRE(all_expected_files_found(env, "", {}));

        // Progressively add files
        create_file(env.dest_file_path(file_1).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1}));

        create_file(env.dest_file_path(file_2).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2}));

        create_file(env.dest_file_path(file_3).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.dest_file_path(file_2).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_1, file_3}));

        testutil_remove(env.dest_file_path(file_1).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_3}));

        testutil_remove(env.dest_file_path(file_3).c_str());
        REQUIRE(all_expected_files_found(env, "", {}));
    }

    SECTION("Directory list - Test files that only exist in the source")
    {
        std::string file_1 = "file1.txt";
        std::string file_2 = "file2.txt";
        std::string file_3 = "file3.txt";

        // Start with an empty directory.
        REQUIRE(all_expected_files_found(env, "", {}));

        // Progressively add files
        create_file(env.source_file_path(file_1).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1}));

        create_file(env.source_file_path(file_2).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2}));

        create_file(env.source_file_path(file_3).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.source_file_path(file_2).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_1, file_3}));

        testutil_remove(env.source_file_path(file_1).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_3}));

        testutil_remove(env.source_file_path(file_3).c_str());
        REQUIRE(all_expected_files_found(env, "", {}));
    }

    SECTION("Directory list - Test files when files are present in both source and destination")
    {
        std::string file_1 = "file1.txt";
        std::string file_2 = "file2.txt";
        std::string file_3 = "file3.txt";

        // Start with an empty directory.
        REQUIRE(all_expected_files_found(env, "", {}));

        // Progressively add files
        create_file(env.dest_file_path(file_1).c_str(), 1000);
        create_file(env.source_file_path(file_1).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1}));

        create_file(env.dest_file_path(file_2).c_str(), 1000);
        create_file(env.source_file_path(file_2).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2}));

        create_file(env.dest_file_path(file_3).c_str(), 1000);
        create_file(env.source_file_path(file_3).c_str(), 1000);
        REQUIRE(all_expected_files_found(env, "", {file_1, file_2, file_3}));

        // And then delete them
        testutil_remove(env.dest_file_path(file_2).c_str());
        testutil_remove(env.source_file_path(file_2).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_1, file_3}));

        testutil_remove(env.dest_file_path(file_1).c_str());
        testutil_remove(env.source_file_path(file_1).c_str());
        REQUIRE(all_expected_files_found(env, "", {file_3}));

        testutil_remove(env.dest_file_path(file_3).c_str());
        testutil_remove(env.source_file_path(file_3).c_str());
        REQUIRE(all_expected_files_found(env, "", {}));
    }
}
