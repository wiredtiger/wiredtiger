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

/*
 * Prepare the session and error_info structure to be used by the drop conflict tests.
 */
void
prepare_session_and_error(
  connection_wrapper *conn_wrapper, WT_SESSION **session, WT_ERROR_INFO **err_info, const char *uri)
{
    WT_CONNECTION *conn = conn_wrapper->get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, session) == 0);
    REQUIRE((*session)->create(*session, uri, "key_format=S,value_format=S") == 0);
    *err_info = &(((WT_SESSION_IMPL *)(*session))->err_info);
}

TEST_CASE("Test WT_CONFLICT_BACKUP and WT_CONFLICT_DHANDLE", "[drop_conflict]")
{
    WT_SESSION *session = NULL;
    WT_ERROR_INFO *err_info = NULL;
    const char *uri = "table:test_drop_conflict";

    SECTION("Test WT_CONFLICT_BACKUP")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info, uri);

        /* Open a backup cursor on a table, then attempt to drop the table. */
        WT_CURSOR *backup_cursor;
        REQUIRE(session->open_cursor(session, "backup:", NULL, NULL, &backup_cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_BACKUP,
          "the table is currently performing backup and cannot be dropped");
    }

    SECTION("Test WT_CONFLICT_DHANDLE")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info, uri);

        /* Open a cursor on a table, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, uri, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE,
          "another thread is currently holding the data handle of the table");
    }

    SECTION("Test WT_CONFLICT_DHANDLE with tiered storage")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create,tiered_storage=()");
        prepare_session_and_error(&conn_wrapper, &session, &err_info, uri);

        /* Open a cursor on a table that uses tiered storage, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, uri, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE,
          "another thread is currently holding the data handle of the table");
    }
}
