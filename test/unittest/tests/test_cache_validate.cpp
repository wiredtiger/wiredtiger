/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "wrappers/connection_wrapper.h"
#include "utils.h"

TEST_CASE("Test cache config validation: validate_cache_config", "[VALIDATE_CACHE_CONFIG]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session_impl = conn.createSession();

    const char *cfg[] = {"eviction_target=60", "eviction_trigger=61"};

    CHECK(__ut_validate_cache_config(session_impl, cfg, true) == 0);
    // CHECK(__ut_validate_cache_config(session_impl, cfg, true) == 1);
}
