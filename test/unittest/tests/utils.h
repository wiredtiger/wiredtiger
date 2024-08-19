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

#define DB_HOME "test_db"

namespace utils {
void ext_print_list(WT_EXT **head);
void extlist_print_off(WT_EXTLIST &extlist);
void throwIfNonZero(int result);
void wiredtigerCleanup(const std::string &db_home);
} // namespace utils
