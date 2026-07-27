/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../wrappers/mock_session.h"

/*
 * Unit tests for config-related functions.
 */

TEST_CASE("Parse integer", "[config][parse_int]")
{
    SECTION("No conversion")
    {
        const char *s = "abc";
        char *endptr = nullptr;
        int64_t val = __wti_config_parse_dec(s, strlen(s), &endptr);
        REQUIRE(val == 0);
        REQUIRE(endptr == s);
    }

    SECTION("Boundary conditions")
    {
        const char *max_s = "9223372036854775807"; /* INT64_MAX */
        char *endptr_max = nullptr;
        const int64_t max_val = __wti_config_parse_dec(max_s, strlen(max_s), &endptr_max);
        REQUIRE(max_val == INT64_MAX);
        REQUIRE(endptr_max == max_s + strlen(max_s));

        const char *min_s = "-9223372036854775808"; /* INT64_MIN */
        char *endptr_min = nullptr;
        const int64_t min_val = __wti_config_parse_dec(min_s, strlen(min_s), &endptr_min);
        REQUIRE(min_val == INT64_MIN);
        REQUIRE(endptr_min == min_s + strlen(min_s));
    }

    SECTION("Boundary conditions less than one")
    {
        const char *max_s = "9223372036854775806"; /* INT64_MAX - 1 */
        char *endptr_max = nullptr;
        const int64_t max_val = __wti_config_parse_dec(max_s, strlen(max_s), &endptr_max);
        REQUIRE(max_val == INT64_MAX - 1);
        REQUIRE(endptr_max == max_s + strlen(max_s));

        const char *min_s = "-9223372036854775807"; /* INT64_MIN + 1 */
        char *endptr_min = nullptr;
        const int64_t min_val = __wti_config_parse_dec(min_s, strlen(min_s), &endptr_min);
        REQUIRE(min_val == INT64_MIN + 1);
        REQUIRE(endptr_min == min_s + strlen(min_s));
    }

    SECTION("Out of range")
    {
        const char *overflow_s = "9223372036854775808"; /* INT64_MAX + 1 */
        char *endptr_overflow = nullptr;
        errno = 0;
        const int64_t overflow_val =
          __wti_config_parse_dec(overflow_s, strlen(overflow_s), &endptr_overflow);
        REQUIRE(errno == ERANGE);
        REQUIRE(overflow_val == INT64_MAX);
        REQUIRE(endptr_overflow == overflow_s + strlen(overflow_s));

        const char *underflow_s = "-9223372036854775809"; /* INT64_MIN - 1 */
        char *endptr_underflow = nullptr;
        errno = 0;
        const int64_t underflow_val =
          __wti_config_parse_dec(underflow_s, strlen(underflow_s), &endptr_underflow);
        REQUIRE(errno == ERANGE);
        REQUIRE(underflow_val == INT64_MIN);
        REQUIRE(endptr_underflow == underflow_s + strlen(underflow_s));

        const char *longer_s = "123456789012345678901"; /* Longer than INT64_MAX */
        char *endptr_longer = nullptr;
        errno = 0;
        const int64_t longer_val =
          __wti_config_parse_dec(longer_s, strlen(longer_s), &endptr_longer);
        REQUIRE(errno == ERANGE);
        REQUIRE(longer_val == INT64_MAX);
        REQUIRE(endptr_longer == longer_s + strlen(longer_s));
    }

    SECTION("Limited length")
    {
        const char *s = "123";
        char *endptr = nullptr;
        int64_t val = __wti_config_parse_dec(s, 2, &endptr);
        REQUIRE(val == 12);
        REQUIRE(endptr == s + 2);
    }

    SECTION("Stopping at non-digit")
    {
        const char *s = "123abc";
        char *endptr = nullptr;
        int64_t val = __wti_config_parse_dec(s, strlen(s), &endptr);
        REQUIRE(val == 123);
        REQUIRE(endptr == s + 3);

        const char *blank_s = "   789 ";
        char *endptr_blank = nullptr;
        int64_t val_blank = __wti_config_parse_dec(blank_s, strlen(blank_s), &endptr_blank);
        REQUIRE(val_blank == 789);
        REQUIRE(endptr_blank == blank_s + 6);

        const char *pos_s = "   +123 ";
        char *endptr_pos = nullptr;
        int64_t pos_val = __wti_config_parse_dec(pos_s, strlen(pos_s), &endptr_pos);
        REQUIRE(pos_val == 123);
        REQUIRE(endptr_pos == pos_s + 7);

        const char *neg_s = "   -456 ";
        char *endptr_neg = nullptr;
        int64_t neg_val = __wti_config_parse_dec(neg_s, strlen(neg_s), &endptr_neg);
        REQUIRE(neg_val == -456);
        REQUIRE(endptr_neg == neg_s + 7);
    }

    SECTION("Explicit positive and negative")
    {
        const char *pos_s = "+456";
        char *endptr_pos = nullptr;
        int64_t pos_val = __wti_config_parse_dec(pos_s, strlen(pos_s), &endptr_pos);
        REQUIRE(pos_val == 456);
        REQUIRE(endptr_pos == pos_s + strlen(pos_s));

        const char *neg_s = "-789";
        char *endptr_neg = nullptr;
        int64_t neg_val = __wti_config_parse_dec(neg_s, strlen(neg_s), &endptr_neg);
        REQUIRE(neg_val == -789);
        REQUIRE(endptr_neg == neg_s + strlen(neg_s));

        const char *posz_s = "+0";
        char *endptr_posz = nullptr;
        int64_t posz_val = __wti_config_parse_dec(posz_s, strlen(posz_s), &endptr_posz);
        REQUIRE(posz_val == 0);
        REQUIRE(endptr_posz == posz_s + strlen(posz_s));

        const char *negz_s = "-0";
        char *endptr_negz = nullptr;
        int64_t negz_val = __wti_config_parse_dec(negz_s, strlen(negz_s), &endptr_negz);
        REQUIRE(negz_val == 0);
        REQUIRE(endptr_negz == negz_s + strlen(negz_s));
    }
}

TEST_CASE("Config replace", "[config][replace]")
{
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *s = session_mock->get_wt_session_impl();

    auto make_item = [](const char *str, WT_CONFIG_ITEM::WT_CONFIG_ITEM_TYPE type) {
        WT_CONFIG_ITEM item;
        WT_CLEAR(item);
        item.str = str;
        item.len = strlen(str);
        item.type = type;
        return item;
    };

    SECTION("Substitutes a matching key and preserves neighbors")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM new_ckpt = make_item("(new=2)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(
                  s, "a=1,checkpoint=(old=1),b=2", "checkpoint", &new_ckpt, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=1,checkpoint=(new=2),b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Replaces the first key")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("9", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);

        REQUIRE(__wt_config_replace(s, "a=1,b=2", "a", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=9,b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Replaces the last key")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("9", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);

        REQUIRE(__wt_config_replace(s, "a=1,b=2", "b", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=1,b=9") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Replaces a sole key")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("9", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);

        REQUIRE(__wt_config_replace(s, "a=1", "a", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=9") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Empty replacement value")
    {
        char *replaced = nullptr;
        /* Non-STRING type: empty bytes emit as checkpoint= with no value. */
        WT_CONFIG_ITEM val = make_item("", WT_CONFIG_ITEM::WT_CONFIG_ITEM_ID);

        REQUIRE(
          __wt_config_replace(s, "a=1,checkpoint=(old),b=2", "checkpoint", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=1,checkpoint=,b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Quoted string values keep their quotes")
    {
        char *replaced = nullptr;
        /*
         * PRESERVE_QUOTES looks at the bytes before/after the item span. Embed the value in a
         * buffer so the surrounding quotes are real.
         */
        char buf[] = "x\"new\"y";
        WT_CONFIG_ITEM val;
        WT_CLEAR(val);
        val.str = buf + 2; /* "new" without quotes */
        val.len = 3;
        val.type = WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRING;

        REQUIRE(__wt_config_replace(s, "path=\"old\",b=2", "path", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "path=\"new\",b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Duplicate keys are all substituted")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("(new)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(
                  s, "checkpoint=(one),a=1,checkpoint=(two)", "checkpoint", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "checkpoint=(new),a=1,checkpoint=(new)") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Does not match a key that merely shares a prefix")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("(new)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        /* checkpoint_abc must stay untouched when replacing checkpoint. */
        REQUIRE(__wt_config_replace(s, "checkpoint_abc=(keep),checkpoint=(old),b=2", "checkpoint",
                  &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "checkpoint_abc=(keep),checkpoint=(new),b=2") == 0);
        __wt_free(s, replaced);

        /* The reverse: replacing the longer name must not touch checkpoint. */
        REQUIRE(__wt_config_replace(s, "checkpoint=(keep),checkpoint_abc=(old)", "checkpoint_abc",
                  &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "checkpoint=(keep),checkpoint_abc=(new)") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Missing key returns WT_NOTFOUND and does not invent it")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("(new)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(s, "a=1,b=2", "checkpoint", &val, &replaced) == WT_NOTFOUND);
        REQUIRE(replaced == nullptr);
    }

    SECTION("Empty base returns WT_NOTFOUND")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("(new)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(s, "", "checkpoint", &val, &replaced) == WT_NOTFOUND);
        REQUIRE(replaced == nullptr);
    }

    SECTION("Base with only unrelated keys returns WT_NOTFOUND")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("1", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);

        REQUIRE(__wt_config_replace(s, "alpha=1,beta=2", "gamma", &val, &replaced) == WT_NOTFOUND);
        REQUIRE(replaced == nullptr);
    }

    SECTION("Prefix-only key does not satisfy an exact match")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("(new)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        /* checkpoint_abc alone is not a hit for checkpoint. */
        REQUIRE(__wt_config_replace(
                  s, "checkpoint_abc=(keep),b=2", "checkpoint", &val, &replaced) == WT_NOTFOUND);
        REQUIRE(replaced == nullptr);
    }

    SECTION("Nested struct values with commas survive rewrite")
    {
        char *replaced = nullptr;
        const char *new_ckpt = "(WiredTigerCheckpoint=(time=1,size=2,addr=\"abc\"))";
        WT_CONFIG_ITEM val = make_item(new_ckpt, WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(s,
                  "a=1,checkpoint=(WiredTigerCheckpoint=(time=0,size=0,addr=\"old\")),b=2",
                  "checkpoint", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced,
                  "a=1,checkpoint=(WiredTigerCheckpoint=(time=1,size=2,addr=\"abc\")),b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Replacement may shrink or grow the value")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM short_val = make_item("1", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);
        WT_CONFIG_ITEM long_val =
          make_item("(11111111111111111111)", WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT);

        REQUIRE(__wt_config_replace(s, "checkpoint=(quite_a_long_old_value),b=2", "checkpoint",
                  &short_val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "checkpoint=1,b=2") == 0);
        __wt_free(s, replaced);

        REQUIRE(
          __wt_config_replace(s, "checkpoint=1,b=2", "checkpoint", &long_val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "checkpoint=(11111111111111111111),b=2") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Three duplicate keys are all substituted")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("9", WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM);

        REQUIRE(__wt_config_replace(s, "a=1,a=2,a=3", "a", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "a=9,a=9,a=9") == 0);
        __wt_free(s, replaced);
    }

    SECTION("Boolean values round-trip")
    {
        char *replaced = nullptr;
        WT_CONFIG_ITEM val = make_item("true", WT_CONFIG_ITEM::WT_CONFIG_ITEM_BOOL);

        REQUIRE(__wt_config_replace(s, "enabled=false,b=2", "enabled", &val, &replaced) == 0);
        REQUIRE(strcmp(replaced, "enabled=true,b=2") == 0);
        __wt_free(s, replaced);
    }
}
