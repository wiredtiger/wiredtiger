/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "utils_live_restore_extent_list.h"

#include <string>
#include <fstream>
#include <iostream>

#include <catch2/catch.hpp>
#include "../utils.h"

namespace utils {

/* TO DISCUSS - these string reps are inclusive byte ranges on both ends. 0-10 means there are 11
 * bytes (0 up to and including 10) that are in the hole. Is this intuitive for devs new to the test
 * code?
 */
std::string
build_str_from_extent(WT_LIVE_RESTORE_HOLE_NODE *ext)
{
    std::string str = "";

    if (ext == NULL) {
        // NULL is an empty list
        return "";
    }

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

bool
extent_list_is(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh, std::string expected_extent)
{
    WT_LIVE_RESTORE_HOLE_NODE *ext = lr_fh->destination.hole_list_head;
    std::string extent_string = build_str_from_extent(ext);
    if (extent_string != expected_extent) {
        std::cout << "Expected: " << expected_extent << std::endl;
        std::cout << "Actual: " << extent_string << std::endl;
    }
    return extent_string == expected_extent;
}

int
open_file(LiveRestoreTestEnv *env, std::string dest_file, WT_LIVE_RESTORE_FILE_HANDLE **lr_fhp)
{
    WT_LIVE_RESTORE_FS *lr_fs = env->_lr_fs;
    WT_SESSION *wt_session = (WT_SESSION*)env->_session;
    // Make sure we're always opening the file in the destination directory.
    REQUIRE(strncmp(dest_file.c_str(), env->_DB_DEST.c_str(), env->_DB_DEST.size()) == 0);
    testutil_check(lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, wt_session, dest_file.c_str(),
        WT_FS_OPEN_FILE_TYPE_REGULAR, 0, (WT_FILE_HANDLE **)lr_fhp));
    return 0;
}

bool
extent_list_in_order(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh)
{
    WT_LIVE_RESTORE_HOLE_NODE *prev_node, *node;
    prev_node = NULL;
    node = lr_fh->destination.hole_list_head;
    while (node != NULL) {
        if (prev_node != NULL) {
            if (prev_node->off >= node->off || WT_EXTENT_END(prev_node) >= node->off) {
                return false;
            }
        }
        prev_node = node;
        node = node->next;
    }
    return true;
}

/* Write to a range in a file. */
void
create_file(std::string filepath, int len)
{
    REQUIRE(not testutil_exists(NULL, filepath.c_str()));
    std::ofstream file(filepath, std::ios::out);
    std::string data_str = std::string(len, 'A');
    file << data_str;
    file.close();
}

} // namespace utils.
