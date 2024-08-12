/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "wrappers/mock_session.h"


TEST_CASE("Set cache_max_wait_ms", "[session_config]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();

    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, "cache_max_wait_ms=2000") == 0);
    REQUIRE(session_impl->cache_max_wait_us == 2000 * WT_THOUSAND);

    /* Test that setting to zero works correctly. */
    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, "cache_max_wait_ms=0") == 0);
    REQUIRE(session_impl->cache_max_wait_us == 0);

    /*
     * This call should not error out or return WT_NOTFOUND with invalid strings. We should depend
     * __wt_config_getones unit tests for correctness. Currently that test does not exist.
     */
    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, NULL) == 0);
    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, "") == 0);
    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, "foo=10000") == 0);
    REQUIRE(session_impl->cache_max_wait_us == 0);

    /*
     * WiredTiger config strings accept negative values, but the session variable is a uint64_t.
     * Overflow is allowed.
     */
    REQUIRE(__ut_session_config_cache_max_wait_ms(session_impl, "cache_max_wait_ms=-1") == 0);
    REQUIRE(session_impl->cache_max_wait_us == 0xfffffffffffffc18);
}
