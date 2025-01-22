/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"
#include "../utils.h"

/*
 * [drop_conflict]: test_drop_conflict.cpp
 * Tests the drop workflows that lead to EBUSY errors, and ensure that the correct sub level error
 * codes and messages are stored.
 */

TEST_CASE("Test WT_CONFLICT_BACKUP and WT_CONFLICT_DHANDLE", "[drop_conflict]")
{
    const char *uri = "table:test_error";

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();

    WT_SESSION *session;
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_ERROR_INFO *err_info = &(session_impl->err_info);

    REQUIRE(session->create(session, uri, "key_format=S,value_format=S") == 0);

    SECTION("Test WT_CONFLICT_BACKUP")
    {
        /* Open a backup cursor on a table, then attempt to drop the table. */
        WT_CURSOR *backup_cursor;
        REQUIRE(session->open_cursor(session, "backup:", NULL, NULL, &backup_cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_BACKUP,
          "the table is currently performing backup and cannot be dropped");
    }

    SECTION("Test WT_CONFLICT_DHANDLE")
    {
        /* Open a cursor on a table, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, uri, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE,
          "another thread is currently holding the data handle of the table");
    }
}
