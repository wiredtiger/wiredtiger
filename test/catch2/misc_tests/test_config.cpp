/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wiredtiger.h"
#include "wt_internal.h"

#include "wrappers/connection_wrapper.h"
#include "utils.h"

#include <catch2/catch.hpp>

TEST_CASE("Config tests", "[config]")
{
    connection_wrapper conn{DB_HOME, "create,in_memory"};
    WT_SESSION_IMPL *session = conn.create_session();
    WT_CONFIG cfg{};

    SECTION("Limited length")
    {
        const char *s = "key=123";
        __wt_config_initn(session, &cfg, s, strlen(s) - 2); /* Exclude last two chars "23" */

        WT_CONFIG_ITEM key{};
        WT_CONFIG_ITEM val{};
        int ret = __wt_config_next(&cfg, &key, &val);
        REQUIRE(ret == 0);

        REQUIRE(key.len == 3);
        REQUIRE(WT_CONFIG_LIT_MATCH("key", key));

        REQUIRE(val.len == 1); /* Only "1" is included */
        REQUIRE(WT_CONFIG_LIT_MATCH("1", val));
        REQUIRE(val.type == WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);
        REQUIRE(val.val == 1);

        ret = __wt_config_next(&cfg, &key, &val);
        REQUIRE(ret == WT_NOTFOUND);
    }
}
