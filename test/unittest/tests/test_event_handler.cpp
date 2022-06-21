/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*      All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#include <catch2/catch.hpp>

#include "wt_internal.h"

#include "wrappers/connection_wrapper.h"
#include "wrappers/event_handler.h"
#include "utils.h"

TEST_CASE("Event handler: simple", "[event_handler]")
{
    auto eventHandler = std::make_shared<EventHandler>();
    ConnectionWrapper conn(utils::UnitTestDatabaseHome, eventHandler);
    WT_SESSION_IMPL *session = conn.createSession("isolation=invalid");
}
