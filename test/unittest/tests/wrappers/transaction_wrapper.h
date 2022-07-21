/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef WIREDTIGER_TRANSACTION_WRAPPER_H
#define WIREDTIGER_TRANSACTION_WRAPPER_H

#include <string>
#include "wiredtiger.h"

class TransactionWrapper {
    public:
    TransactionWrapper(WT_SESSION* session, std::string config);
    ~TransactionWrapper();
    void commit(std::string const& commitConfig);

    private:
    WT_SESSION* _session;
    std::string _config;
    bool _rollbackInDestructor;
};

#endif // WIREDTIGER_TRANSACTION_WRAPPER_H
