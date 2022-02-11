/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "session_wrapper.h"
#include <utility>
#include "connection_wrapper.h"
#include "error_handler.h"

SessionWrapper::SessionWrapper(
  WT_SESSION_IMPL *session, std::shared_ptr<ConnectionWrapper> connectionWrapper)
    : _sessionImpl(session), _connectionWrapper(std::move(connectionWrapper))
{
}

SessionWrapper::~SessionWrapper()
{
    __wt_free(nullptr, _sessionImpl);
}

std::shared_ptr<SessionWrapper>
SessionWrapper::buildTestSessionWrapper()
{
    auto connectionWrapper = ConnectionWrapper::buildTestConnectionWrapper();

    WT_SESSION_IMPL *sessionImpl = nullptr;
    ErrorHandler::throwIfNonZero(__wt_calloc(nullptr, 1, sizeof(WT_SESSION_IMPL), &sessionImpl));
    sessionImpl->iface.connection = connectionWrapper->getWtConnection();

    // Construct an object that will now own the two pointers passed in.
    return std::shared_ptr<SessionWrapper>(new SessionWrapper(sessionImpl, connectionWrapper));
}
