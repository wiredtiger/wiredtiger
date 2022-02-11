/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <string>
#include <stdexcept>
#include "error_handler.h"

void
ErrorHandler::throwIfNonZero(int result)
{
    if (result != 0) {
        std::string errorMessage("Error result in _error_handler.cpp is " + std::to_string(result));
        throw std::runtime_error(errorMessage);
    }
}
