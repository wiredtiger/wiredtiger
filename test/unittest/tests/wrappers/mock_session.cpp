/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <utility>

#include "mock_session.h"
#include "mock_connection.h"
#include "../utils.h"

MockSession::MockSession(
  WT_SESSION_IMPL *session, WT_BLOCK *block, std::shared_ptr<MockConnection> mockConnection)
    : _sessionImpl(session), _block(block), _mockConnection(std::move(mockConnection))
{
}

MockSession::~MockSession()
{
    __wt_free(nullptr, _sessionImpl);
    __wt_free(nullptr, _block);
}

std::shared_ptr<MockSession>
MockSession::buildTestMockSession()
{
    auto mockConnection = MockConnection::buildTestMockConnection();

    WT_SESSION_IMPL *sessionImpl = nullptr;
    WT_BLOCK *block = nullptr;
    utils::throwIfNonZero(__wt_calloc(nullptr, 1, sizeof(WT_SESSION_IMPL), &sessionImpl));
    utils::throwIfNonZero(__wt_calloc(nullptr, 1, sizeof(WT_BLOCK), &block));
    sessionImpl->iface.connection = mockConnection->getWtConnection();

    // Construct an object that will now own the two pointers passed in.
    return std::shared_ptr<MockSession>(new MockSession(sessionImpl, block, mockConnection));
}
