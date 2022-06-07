/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <string>
#include <filesystem>

namespace utils {
const std::basic_string UnitTestDatabaseHome = "test_db";

inline bool isSuccessResult(int result) { return result == 0; };

void throwIfNonZero(int result);
void wiredtigerCleanup(std::string const& db_home);
} // namespace utils
