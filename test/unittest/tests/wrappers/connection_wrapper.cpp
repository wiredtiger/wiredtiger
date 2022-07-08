/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <cstdio>
#include <utility>

#include "wiredtiger.h"
#include "wt_internal.h"

#include "connection_wrapper.h"
#include "../utils.h"

ConnectionWrapper::ConnectionWrapper(std::string db_home)
    : _conn_impl(nullptr), _conn(nullptr), _db_home(std::move(db_home))
{
    initConnection();
}

ConnectionWrapper::ConnectionWrapper(std::string db_home, std::shared_ptr<EventHandler>& eventHandler)
    : _conn_impl(nullptr), _conn(nullptr), _db_home(std::move(db_home)), _eventHandler(eventHandler)
{
    initConnection();
}

WT_EVENT_HANDLER* ConnectionWrapper::getWtEventHandler()
{
    if (_eventHandler == nullptr)
        return nullptr;
    return _eventHandler->getWtEventHandler();
}


void ConnectionWrapper::initConnection()
{
    utils::throwIfNonZero(mkdir(_db_home.c_str(), 0700));
    utils::throwIfNonZero(wiredtiger_open(_db_home.c_str(), getWtEventHandler(), "create,statistics=[all,clear],debug_mode=[eviction],cache_size=50MB,eviction_target=10,eviction_dirty_target=1", &_conn));
}

ConnectionWrapper::~ConnectionWrapper()
{
    utils::throwIfNonZero(_conn->close(_conn, ""));
    utils::wiredtigerCleanup(_db_home);
}

WT_SESSION_IMPL *
ConnectionWrapper::createSession(const char* config)
{
    WT_SESSION* sess = nullptr;
    WT_EVENT_HANDLER* eventHandler = getWtEventHandler();
    _conn->open_session(_conn, eventHandler, config, &sess);

    auto sess_impl = (WT_SESSION_IMPL *)sess;

    if (sess_impl != nullptr)
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
