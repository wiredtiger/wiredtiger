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

/*
 * [drop_conflict]: test_drop_conflict.cpp
 * Tests the drop workflows that lead to EBUSY errors, and ensure that the correct sub level error
 * codes and messages are stored.
 */

TEST_CASE("Test WT_CONFLICT_BACKUP and WT_CONFLICT_DHANDLE", "[drop_conflict]")
{
    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();

    WT_SESSION *session;
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    const char *uri = "table:test_error";
    const char *cfg = "key_format=S,value_format=S";

    SECTION("Test WT_CONFLICT_BACKUP")
    {
        /* Open a backup cursor on a new table, then attempt to drop the table. */
        WT_CURSOR *backup_cursor;
        REQUIRE(session->create(session, uri, cfg) == 0);
        REQUIRE(session->open_cursor(session, "backup:", NULL, NULL, &backup_cursor) == 0);
        REQUIRE(session->drop(session, "table:test_error", NULL) == EBUSY);

        /* Check that the proper error/sub-level error and message were stored. */
        CHECK(session_impl->err_info.err == EBUSY);
        CHECK(session_impl->err_info.sub_level_err == WT_CONFLICT_BACKUP);
        CHECK(strcmp(session_impl->err_info.err_msg,
                "the table is currently performing backup and cannot be dropped") == 0);
    }

    SECTION("Test WT_CONFLICT_DHANDLE")
    {
        /* Open a cursor on a new table, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->create(session, uri, cfg) == 0);
        REQUIRE(session->open_cursor(session, uri, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, uri, NULL) == EBUSY);

        /* Check that the proper error/sub-level error and message were stored. */
        CHECK(session_impl->err_info.err == EBUSY);
        CHECK(session_impl->err_info.sub_level_err == WT_CONFLICT_DHANDLE);
        CHECK(strcmp(session_impl->err_info.err_msg,
                "another thread is currently holding the data handle of the table") == 0);
    }
}
