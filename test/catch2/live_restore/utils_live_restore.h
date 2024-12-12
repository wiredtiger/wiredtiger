/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "live_restore_test_env.h"

namespace utils {

// Extent list helpers
bool extent_list_in_order(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh);
std::string extent_list_str(WT_LIVE_RESTORE_FILE_HANDLE *lr_fh);

// File op helpers
void create_file(std::string filepath, int len);
void open_lr_fh(
  live_restore_test_env *env, std::string dest_file, WT_LIVE_RESTORE_FILE_HANDLE **lr_fhp);

} // namespace utils.
