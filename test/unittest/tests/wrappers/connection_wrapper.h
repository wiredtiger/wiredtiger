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
#include <string>
#include "wt_internal.h"
#include "event_handler.h"

/*
 * Prefer a "real" class over a mock class when you need a fully fleshed-out connection or session.
 * There's a speed cost to this, since it will write a bunch of files to disk during the test, which
 * also need to be removed.
 */
class ConnectionWrapper {
    public:
    explicit ConnectionWrapper(std::string db_home);
    ConnectionWrapper(std::string db_home, std::shared_ptr<EventHandler>& eventHandler);
    ~ConnectionWrapper();

    /*
     * The memory backing the returned session is owned by the connection it was opened on, and gets
     * cleaned up when that connection is closed. Neither this class nor its users need to clean it
     * up.
     */
    WT_SESSION_IMPL* createSession(const char* config = nullptr);

    WT_CONNECTION_IMPL *getWtConnectionImpl() const;
    WT_CONNECTION *getWtConnection() const;

    private:
    void initConnection();
    WT_EVENT_HANDLER *getWtEventHandler();

    WT_CONNECTION_IMPL *_conn_impl;
    WT_CONNECTION *_conn;
    std::string _db_home;
    std::shared_ptr<EventHandler> _eventHandler;
};

#endif // WT_CONNECTION_WRAPPER_H
