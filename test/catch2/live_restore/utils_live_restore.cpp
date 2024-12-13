/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "utils_live_restore.h"
#include <fstream>
namespace utils {

std::string
extent_list_str(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh)
{
    WT_LIVE_RESTORE_HOLE_NODE *ext = lr_fh->destination.hole_list_head;

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

void
open_lr_fh(const live_restore_test_env &env, const std::string &dest_file,
  WT_LIVE_RESTORE_FILE_HANDLE **lr_fhp)
{
    WT_LIVE_RESTORE_FS *lr_fs = env._lr_fs;
    WT_SESSION *wt_session = reinterpret_cast<WT_SESSION *>(env._session);
    // Make sure we're always opening the file in the destination directory.
    REQUIRE(strncmp(dest_file.c_str(), env._DB_DEST.c_str(), env._DB_DEST.size()) == 0);
    testutil_check(lr_fs->iface.fs_open_file(reinterpret_cast<WT_FILE_SYSTEM *>(lr_fs), wt_session,
      dest_file.c_str(), WT_FS_OPEN_FILE_TYPE_REGULAR, 0,
      reinterpret_cast<WT_FILE_HANDLE **>(lr_fhp)));
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
create_file(const std::string &filepath, int len)
{
    REQUIRE(not testutil_exists(NULL, filepath.c_str()));
    std::ofstream file(filepath, std::ios::out);
    std::string data_str = std::string(len, 'A');
    file << data_str;
    file.close();
}

} // namespace utils.
