/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <cstdio>

#include "wiredtiger.h"
#include "wt_internal.h"

#include "connection_wrapper.h"
#include "../utils.h"

ConnectionWrapper::ConnectionWrapper() : _conn_impl(nullptr), _conn(nullptr)
{
    utils::throwIfNonZero(wiredtiger_open(nullptr, nullptr, "create", &_conn));
}

ConnectionWrapper::~ConnectionWrapper()
{
    utils::throwIfNonZero(_conn->close(_conn, ""));

    utils::wiredtigerCleanup();
}

WT_SESSION_IMPL *
ConnectionWrapper::createSession()
{
    WT_SESSION *sess;
    _conn->open_session(_conn, nullptr, nullptr, &sess);

    auto sess_impl = (WT_SESSION_IMPL *)sess;
    _conn_impl = S2C(sess_impl);

    return sess_impl;
}

WT_CONNECTION_IMPL *
ConnectionWrapper::getWtConnectionImpl() const
{
    return _conn_impl;
}

WT_CONNECTION *
ConnectionWrapper::getWtConnection() const
{
    return _conn;
}
