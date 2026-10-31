/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../../utils.h"
#include "../../wrappers/connection_wrapper.h"

TEST_CASE("Session reset frees unpositioned cursor buffers", "[session_reset]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    REQUIRE(session->iface.create(
              &session->iface, "table:test_reset", "key_format=S,value_format=S") == 0);

    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->iface.open_cursor(
              &session->iface, "table:test_reset", nullptr, nullptr, &cursor) == 0);

    REQUIRE(__wt_buf_setstr(session, &cursor->key, "sample_key_data_string") == 0);
    REQUIRE(__wt_buf_setstr(session, &cursor->value, "sample_value_data_string") == 0);

    REQUIRE(session->ncursors == 0);
    REQUIRE(cursor->key.mem != nullptr);
    REQUIRE(cursor->value.mem != nullptr);

    REQUIRE(session->iface.reset(&session->iface) == 0);

    CHECK(cursor->key.mem == nullptr);
    CHECK(cursor->value.mem == nullptr);

    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->iface.drop(&session->iface, "table:test_reset", "force=true") == 0);
}
