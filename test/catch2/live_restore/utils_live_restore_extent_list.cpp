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
extent_list_in_order(WT_LIVE_RESTORE_HOLE_NODE *head)
{
    WT_LIVE_RESTORE_HOLE_NODE *prev_node, *node;
    prev_node = NULL;
    node = head;
    while (node != NULL) {
        if (prev_node != NULL) {
            if(prev_node->off >= node->off || WT_EXTENT_END(prev_node) >= node->off) {
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
write_to_file(std::string filepath, int len)
{
    std::ofstream file(filepath, std::ios::out);
    std::string data_str = std::string(len, 'A');
    file << data_str;
    file.close();
}

} // namespace utils.
