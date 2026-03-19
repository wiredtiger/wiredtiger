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
 * [sub_level_error_nested_api_calls]: test_sub_level_error_nested_api_calls.cpp
 * Tests that nested API calls record errors in the session error_info struct only when necessary.
 */

using namespace utils;

static int
cursor_api_call_with_notfound(WT_CURSOR *cursor, WT_SESSION_IMPL *session_impl)
{
    WT_DECL_RET;
    CURSOR_API_CALL(cursor, session_impl, ret, next, NULL);
    ret = WT_NOTFOUND;
err:
    API_END_RET(session_impl, ret);
}

static int
cursor_api_call_with_set_einval(WT_CURSOR *cursor, WT_SESSION_IMPL *session_impl)
{
    WT_DECL_RET;
    CURSOR_API_CALL(cursor, session_impl, ret, next, NULL);
    ret = EINVAL;
    __wt_session_set_last_error(session_impl, ret, WT_NONE, WT_ERROR_INFO_EMPTY);
err:
    API_END_RET(session_impl, ret);
}

static int
api_call_nested_with_notfound(WT_SESSION_IMPL *session_impl, WT_CURSOR *cursor, int err,
  int sub_level_err, const char *err_msg_content, bool notfound_ok)
{
    WT_DECL_RET;
    SESSION_API_CALL_NOCONF(session_impl, log_printf);

    if (notfound_ok)
        WT_ERR_NOTFOUND_OK(cursor_api_call_with_notfound(cursor, session_impl), true);
    else
        WT_ERR(cursor_api_call_with_notfound(cursor, session_impl));

    ret = err;
    if (err != 0 && err_msg_content != NULL)
        __wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content);
err:
    API_END_RET(session_impl, ret);
}

static int
api_call_nested_with_einval(WT_SESSION_IMPL *session_impl, WT_CURSOR *cursor, int err,
  int sub_level_err, const char *err_msg_content)
{
    WT_DECL_RET;
    SESSION_API_CALL_NOCONF(session_impl, log_printf);

    WT_ERR(cursor_api_call_with_set_einval(cursor, session_impl));

    ret = err;
    if (err != 0 && err_msg_content != NULL)
        __wt_session_set_last_error(session_impl, err, sub_level_err, err_msg_content);
err:
    API_END_RET(session_impl, ret);
}

TEST_CASE("API_END_RET nested - test that nested API calls only keep explicitly set errors",
  "[sub_level_error_nested_api_calls],[sub_level_error]")
{
    WT_SESSION *session;
    std::string uri = "table:cursor_test";
    std::string file = "file:cursor_test.wt";

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    WT_CONNECTION *conn = conn_wrapper.get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, &session) == 0);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WT_ERROR_INFO *err_info = &(session_impl->err_info);

    WT_CURSOR *cursor = NULL;
    REQUIRE(session->create(session, uri.c_str(), "key_format=S,value_format=S") == 0);
    REQUIRE(session->open_cursor(session, uri.c_str(), NULL, NULL, &cursor) == 0);

    SECTION("Test nested API call with WT_NOTFOUND inside WT_ERR_NOTFOUND_OK(cursor API)")
    {
        REQUIRE(api_call_nested_with_notfound(session_impl, cursor, 0, WT_NONE, NULL, true) == 0);
        check_error_info(err_info, 0, WT_NONE, WT_ERROR_INFO_SUCCESS);
    }

    SECTION("Test nested API call with WT_NOTFOUND inside WT_ERR(cursor API)")
    {
        REQUIRE(api_call_nested_with_notfound(session_impl, cursor, 0, WT_NONE, NULL, false) ==
          WT_NOTFOUND);
        check_error_info(err_info, WT_NOTFOUND, WT_NONE, WT_ERROR_INFO_EMPTY);
    }

    SECTION("Test nested API call with EINVAL set inside cursor API")
    {
        REQUIRE(api_call_nested_with_einval(session_impl, cursor, 0, WT_NONE, NULL) == EINVAL);
        check_error_info(err_info, EINVAL, WT_NONE, WT_ERROR_INFO_EMPTY);
    }
}
