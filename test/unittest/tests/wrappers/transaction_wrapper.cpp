/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "transaction_wrapper.h"

#include <utility>
#include "../utils.h"

TransactionWrapper::TransactionWrapper(WT_SESSION *session, std::string config)
    : _session(session), _config(std::move(config)), _rollbackInDestructor(false)
{
    utils::throwIfNonZero(_session->begin_transaction(_session, _config.c_str()));
    _rollbackInDestructor = true;
}



TransactionWrapper::~TransactionWrapper()
{
    if (_rollbackInDestructor)
        utils::throwIfNonZero(_session->rollback_transaction(_session, _config.c_str()));
}


void
TransactionWrapper::commit()
{
    utils::throwIfNonZero(_session->commit_transaction(_session, _config.c_str()));
    _rollbackInDestructor = false;
}
