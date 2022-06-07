/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <cstdio>
#include <string>
#include <stdexcept>

#include "utils.h"

namespace utils {

void
throwIfNonZero(int result)
{
    if (result != 0) {
        std::string errorMessage("Error result is " + std::to_string(result));
        throw std::runtime_error(errorMessage);
    }
}

int
remove_wrapper(std::string const &path)
{
    return std::remove(path.c_str());
}

void
wiredtigerCleanup(std::string const& db_home)
{
    // Ignoring errors here; we don't mind if something doesn't exist.
    remove_wrapper(db_home + "/WiredTiger");
    remove_wrapper(db_home + "/WiredTiger.basecfg");
    remove_wrapper(db_home + "/WiredTiger.lock");
    remove_wrapper(db_home + "/WiredTiger.turtle");
    remove_wrapper(db_home + "/WiredTiger.wt");
    remove_wrapper(db_home + "/WiredTigerHS.wt");
    remove_wrapper(db_home + "/access.wt");
    remove_wrapper(db_home + "/access1.wt");
    remove_wrapper(db_home + "/access2.wt");

    remove_wrapper(db_home);
}

} // namespace utils
