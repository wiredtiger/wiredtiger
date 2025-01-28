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

/*
 * [sub_level_error_drop_conflict]: test_sub_level_error_rollback.cpp
 * Tests the error handling for rollback workflows.
 */

#define URI "table:test_drop_conflict"
#define CONFLICT_SCHEMA_LOCK_MSG "another thread is currently accessing the schema"
#define CONFLICT_TABLE_LOCK_MSG "another thread is currently accessing the table"

/*
 * Prepare the session, session_impl, and error_info struct to be used by the drop conflict tests.
 */
void
prepare_session_and_error(connection_wrapper *conn_wrapper, WT_SESSION **session_a,
  WT_SESSION_IMPL **session_b_impl, WT_ERROR_INFO **err_info_a, std::string &config)
{
    WT_SESSION *session_b = NULL;
    WT_CONNECTION *conn = conn_wrapper->get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, session_a) == 0);
    REQUIRE(conn->open_session(conn, NULL, NULL, &session_b) == 0);
    REQUIRE((*session_a)->create(*session_a, URI, config.c_str()) == 0);
    *session_b_impl = (WT_SESSION_IMPL *)(session_b);
    *err_info_a = &(((WT_SESSION_IMPL *)(*session_a))->err_info);
}

TEST_CASE("Test CONFLICT_SCHEMA_LOCK and CONFLICT_TABLE_LOCK", "[drop_conflict]")
{
    std::string config = "key_format=S,value_format=S";
    WT_SESSION *session_a = NULL;
    WT_SESSION_IMPL *session_b_impl = NULL;
    WT_ERROR_INFO *err_info_a = NULL;

    SECTION("Test CONFLICT_SCHEMA_LOCK")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session_a, &session_b_impl, &err_info_a, config);

        /* Attempt to drop the table while another session holds the schema lock. */
        WT_WITH_SCHEMA_LOCK(
          session_b_impl, REQUIRE(session_a->drop(session_a, URI, "lock_wait=0") == EBUSY));

        utils::check_error_info(
          err_info_a, EBUSY, WT_CONFLICT_SCHEMA_LOCK, CONFLICT_SCHEMA_LOCK_MSG);
    }

    SECTION("Test CONFLICT_TABLE_LOCK")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session_a, &session_b_impl, &err_info_a, config);

        /* Attempt to drop the table while another session holds the table write lock. */
        WT_WITH_TABLE_WRITE_LOCK(
          session_b_impl, REQUIRE(session_a->drop(session_a, URI, "lock_wait=0") == EBUSY););

        utils::check_error_info(err_info_a, EBUSY, WT_CONFLICT_TABLE_LOCK, CONFLICT_TABLE_LOCK_MSG);
    }
}
