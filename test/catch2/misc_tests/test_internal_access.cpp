/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"

TEST_CASE("Internal access is a diagnostic guardrail with a debug exception", "[internal_access]")
{
    auto mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();
    session->api_call_counter = 1;
    const char *uri =
      GENERATE("file:WiredTiger.wt", "table:WiredTigerApp", "colgroup:orders:WiredTigerGroup");
#ifdef HAVE_DIAGNOSTIC
    const int expected = EINVAL;
#else
    const int expected = 0;
#endif
    CAPTURE(uri);
    REQUIRE(__wt_uri_name_check(session, "table:ordinary", false) == 0);
    REQUIRE(__wt_uri_name_check(session, uri, false) == expected);
    F_SET(session, WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS);
    REQUIRE(__wt_uri_name_check(session, uri, false) == 0);
}
