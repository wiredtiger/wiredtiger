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
remove_wrapper(const std::string &path)
{
    return std::remove(path.c_str());
}

void
wiredtigerCleanup(const std::string &home)
{
    // Ignoring errors here; we don't mind if something doesn't exist.
    remove_wrapper(home + "/WiredTiger");
    remove_wrapper(home + "/WiredTiger.basecfg");
    remove_wrapper(home + "/WiredTiger.lock");
    remove_wrapper(home + "/WiredTiger.turtle");
    remove_wrapper(home + "/WiredTiger.wt");
    remove_wrapper(home + "/WiredTigerHS.wt");

    remove_wrapper(home);
}

} // namespace utils
