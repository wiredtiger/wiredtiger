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

/*
 * Check a basic configuration that sets and clears a flag.
 */
#define FLAG_TEST(config_param, flag)                                                    \
    TEST_CASE(config_param, "[session_config]")                                          \
    {                                                                                    \
        std::shared_ptr<MockSession> session_mock = MockSession::buildTestMockSession(); \
        WT_SESSION_IMPL *session = session_mock->getWtSessionImpl();                     \
        session->flags = 0;                                                              \
        REQUIRE(__ut_session_config_int(session, config_param "=true") == 0);            \
        REQUIRE(F_ISSET(session, flag));                                                 \
        REQUIRE(__ut_session_config_int(session, config_param "=false") == 0);           \
        REQUIRE(!F_ISSET(session, flag));                                                \
        REQUIRE(__ut_session_config_int(session, config_param "=true") == 0);            \
        REQUIRE(__ut_session_config_int(session, "") == 0);                              \
        REQUIRE(F_ISSET(session, flag));                                                 \
    }

FLAG_TEST("ignore_cache_size", WT_SESSION_IGNORE_CACHE_SIZE);
FLAG_TEST("cache_cursors", WT_SESSION_CACHE_CURSORS);
FLAG_TEST("debug.checkpoint_fail_before_turtle_update",
  WT_SESSION_DEBUG_CHECKPOINT_FAIL_BEFORE_TURTLE_UPDATE);
FLAG_TEST("debug.release_evict_page", WT_SESSION_DEBUG_RELEASE_EVICT);

TEST_CASE("cache_max_wait_ms", "[session_config]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> session_mock = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = session_mock->getWtSessionImpl();

    REQUIRE(__ut_session_config_int(session, "cache_max_wait_ms=2000") == 0);
    REQUIRE(session->cache_max_wait_us == 2000 * WT_THOUSAND);

    /* Test that setting to zero works correctly. */
    REQUIRE(__ut_session_config_int(session, "cache_max_wait_ms=0") == 0);
    REQUIRE(session->cache_max_wait_us == 0);

    /*
     * This call should not error out or return WT_NOTFOUND with invalid strings. We should depend
     * __wt_config_getones unit tests for correctness. Currently that test does not exist.
     */
    REQUIRE(__ut_session_config_int(session, NULL) == 0);
    REQUIRE(__ut_session_config_int(session, "") == 0);
    REQUIRE(__ut_session_config_int(session, "foo=10000") == 0);
    REQUIRE(session->cache_max_wait_us == 0);

    /*
     * WiredTiger config strings accept negative values, but the session variable is a uint64_t.
     * Overflow is allowed.
     */
    REQUIRE(__ut_session_config_int(session, "cache_max_wait_ms=-1") == 0);
    REQUIRE(session->cache_max_wait_us == 0xfffffffffffffc18);
}
