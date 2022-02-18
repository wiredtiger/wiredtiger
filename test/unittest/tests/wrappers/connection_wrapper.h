/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef WT_CONNECTION_WRAPPER_H
#define WT_CONNECTION_WRAPPER_H

#include <memory>
#include "wt_internal.h"

class ConnectionWrapper {
    public:
    ConnectionWrapper();
    ~ConnectionWrapper();

    WT_SESSION_IMPL *createSession();

    WT_CONNECTION_IMPL *getWtConnectionImpl() const;
    WT_CONNECTION *getWtConnection() const;

    private:
    // This class is implemented such that it owns, and is responsible for freeing,
    // this pointer
    WT_CONNECTION_IMPL *_conn_impl;
    WT_CONNECTION *_conn;
};

#endif // WT_CONNECTION_WRAPPER_H
