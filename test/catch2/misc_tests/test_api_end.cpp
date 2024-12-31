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
 * [api_end]: test_api_end.cpp
 * Tests that successful API calls are recorded as "successful" in the session error_info struct.
 */

int
api_call_with_no_error(WT_SESSION_IMPL *session_impl)
{
    WT_DECL_RET;
    SESSION_API_CALL_NOCONF(session_impl, log_printf);
err:
    API_END_RET(session_impl, ret);
}

int
api_call_with_error(
  WT_SESSION_IMPL *session_impl, int err, int sub_level_err, const char *err_msg_content)
{
    WT_DECL_RET;
    SESSION_API_CALL_NOCONF(session_impl, log_printf);

    session_impl->api_call_no_errs = false;
    WT_IGNORE_RET(__wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content));
err:
    API_END_RET(session_impl, ret);
}

int
txn_api_call_with_no_error(WT_SESSION_IMPL *session_impl)
{
    WT_DECL_RET;
    SESSION_TXN_API_CALL(session_impl, ret, log_printf, NULL, cfg);
    WT_UNUSED(cfg);
err:
    TXN_API_END(session_impl, ret, false);
    return (ret);
}

int
txn_api_call_with_error(
  WT_SESSION_IMPL *session_impl, int err, int sub_level_err, const char *err_msg_content)
{
    WT_DECL_RET;
    SESSION_TXN_API_CALL(session_impl, ret, log_printf, NULL, cfg);
    WT_UNUSED(cfg);

    session_impl->api_call_no_errs = false;
    WT_IGNORE_RET(__wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content));
err:
    TXN_API_END(session_impl, ret, false);
    return (ret);
}

TEST_CASE("API_END_RET/TXN_API_END - test that the API call result is stored.", "[api_end]")
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;

    const char *err_msg_content;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);
    session_impl = (WT_SESSION_IMPL *)session;

    SECTION("Test API_END_RET with no error")
    {
        err_msg_content = "last API call was successful";

        WT_IGNORE_RET(api_call_with_no_error(session_impl));

        CHECK(session_impl->err_info.err == 0);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }

    SECTION("Test API_END_RET with EINVAL")
    {
        err_msg_content = "Some EINVAL error";

        WT_IGNORE_RET(api_call_with_error(session_impl, EINVAL, WT_NONE, err_msg_content));

        CHECK(session_impl->err_info.err == EINVAL);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }

    SECTION("Test TXN_API_END with no error")
    {
        err_msg_content = "last API call was successful";

        WT_IGNORE_RET(txn_api_call_with_no_error(session_impl));

        CHECK(session_impl->err_info.err == 0);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }

    SECTION("Test TXN_API_END with EINVAL")
    {
        err_msg_content = "Some EINVAL error";

        WT_IGNORE_RET(txn_api_call_with_error(session_impl, EINVAL, WT_NONE, err_msg_content));

        CHECK(session_impl->err_info.err == EINVAL);
        CHECK(session_impl->err_info.sub_level_err == WT_NONE);
        CHECK(strcmp(session_impl->err_info.err_msg, err_msg_content) == 0);
    }
}
