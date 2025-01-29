/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Test the live restore extent import and export functionality. [live_restore_extent_import_export]
 */

#include "../utils_live_restore.h"

using namespace utils;

TEST_CASE("Live Restore extent import", "[live_restore],[live_restore_extent_import_export]")
{
    /*
     * Note: this code runs once per SECTION so we're creating a brand new WT database for each
     * section. If this gets slow we can make it static and manually clear the destination and
     * source for each section.
     */
    live_restore_test_env env;
    WT_SESSION_IMPL *session = env.session;
    std::string file_name = "MY_FILE.txt";
    std::string source_file = env.source_file_path(file_name);
    std::string dest_file = env.dest_file_path(file_name);
    SECTION("Directory list - Test when files only exist in the destination")
    {
        WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
        create_file(source_file.c_str(), 1000);
        testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
        REQUIRE(__wt_live_restore_fh_import_extents_from_string(
                  session, (WT_FILE_HANDLE *)lr_fh, "0-4096") == 0);
        REQUIRE(extent_list_str(lr_fh) == "(0-4096)");
    }
}
