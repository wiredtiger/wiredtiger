/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../../wrappers/connection_wrapper.h"
#include "../utils_sub_level_error.h"
#include "../../../utility/test_util.h"

/*
 * [sub_level_error_drop_conflict]: test_sub_level_error_drop_conflict.cpp
 * Tests the drop workflows that lead to EBUSY errors, and ensure that the correct sub level error
 * codes and messages are stored.
 */

#define URI "table:test_drop_conflict"
#define UNCOMMITTED_DATA_MSG "the table has uncommitted data and cannot be dropped yet"
#define DIRTY_DATA_MSG "the table has dirty data and can not be dropped yet"

/*
 * Prepare a session and error_info struct to be used by the drop conflict tests.
 */
void
prepare_session_and_error(connection_wrapper *conn_wrapper, WT_SESSION **session,
  WT_ERROR_INFO **err_info, std::string &config)
{
    WT_CONNECTION *conn = conn_wrapper->get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, session) == 0);
    REQUIRE((*session)->create(*session, URI, config.c_str()) == 0);
    *err_info = &(((WT_SESSION_IMPL *)(*session))->err_info);
}

/*
 * This test case covers EBUSY errors resulting from drop before committing/checkpointing changes.
 */
TEST_CASE("Test WT_UNCOMMITTED_DATA and WT_DIRTY_DATA", "[sub_level_error_drop_conflict]")
{
    std::string config = "key_format=S,value_format=S";
    WT_SESSION *session = NULL;
    WT_ERROR_INFO *err_info = NULL;

    SECTION("Test WT_UNCOMMITTED_DATA")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info, config);

        /* Make an update, then attempt to drop the table without committing the transaction. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, URI, NULL, NULL, &cursor) == 0);
        REQUIRE(session->begin_transaction(session, NULL) == 0);
        cursor->set_key(cursor, "key");
        cursor->set_value(cursor, "value");
        REQUIRE(cursor->update(cursor) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_UNCOMMITTED_DATA, UNCOMMITTED_DATA_MSG);
    }

    SECTION("Test WT_DIRTY_DATA")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info, config);

        /* Commit an update, then attempt to drop the table without checkpointing. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, URI, NULL, NULL, &cursor) == 0);
        REQUIRE(session->begin_transaction(session, NULL) == 0);
        cursor->set_key(cursor, "key");
        cursor->set_value(cursor, "value");
        REQUIRE(cursor->update(cursor) == 0);
        REQUIRE(session->commit_transaction(session, NULL) == 0);
        REQUIRE(cursor->close(cursor) == 0);

        /* Give time for the oldest txn id to update before dropping the table. */
        sleep(1);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_DIRTY_DATA, DIRTY_DATA_MSG);
    }
}
