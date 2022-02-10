/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef WT_SESSION_H
#define WT_SESSION_H

#include <memory>
#include "wt_internal.h"
#include "connection_wrapper.h"

class SessionWrapper {
    public:
    ~SessionWrapper();
    WT_SESSION_IMPL *
    getWtSessionImpl()
    {
        return _sessionImpl;
    };

    static std::shared_ptr<SessionWrapper> buildTestSessionWrapper();

    private:
    explicit SessionWrapper(
      WT_SESSION_IMPL *sessionImpl, std::shared_ptr<ConnectionWrapper> connectionWrapper = nullptr);

    std::shared_ptr<ConnectionWrapper> _connectionWrapper;

    // This class is implemented such that it owns, and is responsible for freeing, this pointer
    WT_SESSION_IMPL *_sessionImpl;
};

#endif // WT_SESSION_H
