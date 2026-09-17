/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstring>

#include "wt_internal.h"
#include "wrappers/mock_session.h"

TEST_CASE("Config overlay", "[config][overlay]")
{
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session_mock->get_wt_session_impl();

    auto overlay_str = [&](const char *base, const char **overlay) {
        char *result = nullptr;
        REQUIRE(__wt_config_overlay(s, base, overlay, &result) == 0);
        REQUIRE(result != nullptr);
        return result;
    };

    SECTION("Substitutes matching keys and preserves neighbors")
    {
        const char *overlay[] = {"b=9,d=4", nullptr};
        char *replaced = overlay_str("a=1,b=2,c=3", overlay);
        REQUIRE(strcmp(replaced, "a=1,b=9,c=3") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Drops overlay keys that are not in the base")
    {
        const char *overlay[] = {"missing=1,b=9", nullptr};
        char *replaced = overlay_str("a=1,b=2", overlay);
        REQUIRE(strcmp(replaced, "a=1,b=9") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Replaces nested structs wholesale")
    {
        const char *overlay[] = {"c=(x=2,y=3)", nullptr};
        char *replaced = overlay_str("a=1,c=(x=1)", overlay);
        REQUIRE(strcmp(replaced, "a=1,c=(x=2,y=3)") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Preserves quotes on string values")
    {
        const char *overlay[] = {"key_format=\"S\"", nullptr};
        char *replaced = overlay_str("key_format=u,value_format=u", overlay);
        REQUIRE(strcmp(replaced, "key_format=\"S\",value_format=u") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Last overlay string wins")
    {
        const char *overlay[] = {"b=2", "b=3", nullptr};
        char *replaced = overlay_str("a=1,b=1", overlay);
        REQUIRE(strcmp(replaced, "a=1,b=3") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Empty overlay copies the base")
    {
        const char *overlay[] = {nullptr};
        char *replaced = overlay_str("a=1,b=2", overlay);
        REQUIRE(strcmp(replaced, "a=1,b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Matches collapse when the base has unique keys")
    {
        const char *base = "a=1,b=2,c=(x=1),key_format=u";
        const char *overlay[] = {
          "b=9,c=(x=2),key_format=\"S\",extra=drop", "id=7,version=(major=1,minor=1)", nullptr};
        const char *cfg[] = {base, overlay[0], overlay[1], nullptr};
        char *collapsed = nullptr;
        char *overlaid = nullptr;

        REQUIRE(__wt_config_collapse(s, cfg, &collapsed) == 0);
        REQUIRE(__wt_config_overlay(s, base, overlay, &overlaid) == 0);
        REQUIRE(strcmp(collapsed, overlaid) == 0);
        __wt_free(s, collapsed);
        __wt_free(s, overlaid);
    }
}
