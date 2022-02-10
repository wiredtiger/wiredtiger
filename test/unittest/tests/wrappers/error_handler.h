/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef WT_ERROR_HANDLER_H
#define WT_ERROR_HANDLER_H

class ErrorHandler {
    public:
    static void throwIfNonZero(int result);

    private:
    ErrorHandler() = delete;
};

#endif // WT_ERROR_HANDLER_H
