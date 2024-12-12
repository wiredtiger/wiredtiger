/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include <string>

#include "wt_internal.h"
#include "../../../src/live_restore/live_restore_private.h"
#include "utils_live_restore.h"
#include "test_util.h"

namespace utils {

std::string build_str_from_extent(WT_LIVE_RESTORE_HOLE_NODE *ext);
bool extent_list_in_order(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh);
bool extent_list_is(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh, std::string expected_extent);

void create_file(std::string filepath, int len);
int open_file(LiveRestoreTestEnv *env, std::string dest_file, WT_LIVE_RESTORE_FILE_HANDLE **lr_fhp);


} // namespace utils.
