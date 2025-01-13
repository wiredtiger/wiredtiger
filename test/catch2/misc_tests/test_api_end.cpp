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
api_call_with_error(
  WT_SESSION_IMPL *session_impl, int err, int sub_level_err, const char *err_msg_content)
{
    WT_DECL_RET;
    SESSION_API_CALL_NOCONF(session_impl, log_printf);

    ret = err;
    if (err != 0 && err_msg_content != NULL)
        WT_IGNORE_RET(
          __wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content));
err:
    API_END_RET(session_impl, ret);
}

int
api_call_with_no_error(WT_SESSION_IMPL *session_impl)
{
    return (api_call_with_error(session_impl, 0, WT_NONE, NULL));
}

int
txn_api_call_with_error(
  WT_SESSION_IMPL *session_impl, int err, int sub_level_err, const char *err_msg_content)
{
    WT_DECL_RET;
    SESSION_TXN_API_CALL(session_impl, ret, log_printf, NULL, cfg);
    WT_UNUSED(cfg);

    ret = err;
    if (err != 0 && err_msg_content != NULL)
        WT_IGNORE_RET(
          __wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content));
err:
    TXN_API_END(session_impl, ret, false);
    return (ret);
}

int
txn_api_call_with_no_error(WT_SESSION_IMPL *session_impl)
{
    return (txn_api_call_with_error(session_impl, 0, WT_NONE, NULL));
}

void
check_err_info(WT_ERROR_INFO err_info, int err, int sub_level_err, const char *err_msg_content)
{
    CHECK(err_info.err == err);
    CHECK(err_info.sub_level_err == sub_level_err);
    CHECK(strcmp(err_info.err_msg, err_msg_content) == 0);
}

TEST_CASE("API_END_RET/TXN_API_END - test that the API call result is stored.", "[api_end]")
{
    WT_SESSION *session;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;

    SECTION("Test API_END_RET with no error")
    {
        REQUIRE(api_call_with_no_error(session_impl) == 0);
        check_err_info(session_impl->err_info, 0, WT_NONE, WT_ERROR_INFO_SUCCESS);
    }

    SECTION("Test API_END_RET with EINVAL (error code only)")
    {
        REQUIRE(api_call_with_error(session_impl, EINVAL, WT_NONE, NULL) == EINVAL);
        check_err_info(session_impl->err_info, EINVAL, WT_NONE, WT_ERROR_INFO_EMPTY);
    }

    SECTION("Test API_END_RET with EINVAL (with message)")
    {
        const char *err_msg_content = "Some EINVAL error";
        REQUIRE(api_call_with_error(session_impl, EINVAL, WT_NONE, err_msg_content) == EINVAL);
        check_err_info(session_impl->err_info, EINVAL, WT_NONE, err_msg_content);
    }

    SECTION("Test TXN_API_END with no error")
    {
        REQUIRE(txn_api_call_with_no_error(session_impl) == 0);
        check_err_info(session_impl->err_info, 0, WT_NONE, WT_ERROR_INFO_SUCCESS);
    }

    SECTION("Test TXN_API_END with EINVAL (error code only)")
    {
        REQUIRE(txn_api_call_with_error(session_impl, EINVAL, WT_NONE, NULL) == EINVAL);
        check_err_info(session_impl->err_info, EINVAL, WT_NONE, WT_ERROR_INFO_EMPTY);
    }

    SECTION("Test TXN_API_END with EINVAL (with message)")
    {
        const char *err_msg_content = "Some EINVAL error";
        REQUIRE(txn_api_call_with_error(session_impl, EINVAL, WT_NONE, err_msg_content) == EINVAL);
        check_err_info(session_impl->err_info, EINVAL, WT_NONE, err_msg_content);
    }
}
