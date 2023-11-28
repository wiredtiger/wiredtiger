/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <string>
#include <vector>
#include "vector_bool.h"

void
test_binary_conversion(std::vector<bool> const &vec, std::string const &binary_str)
{
    std::string converted_str = vector_bool_to_binary_string(vec);
    REQUIRE(converted_str == binary_str);
    std::vector<bool> vec2 = vector_bool_from_binary_string(converted_str);
    REQUIRE(vec2 == vec);
}

void
test_hex_conversion(std::vector<bool> const &vec, std::string const &hex_str)
{
    std::string converted_str = vector_bool_to_hex_string(vec);
    REQUIRE(converted_str == hex_str);
    std::vector<bool> converted_vec = vector_bool_from_hex_string(hex_str);
    REQUIRE(converted_vec == vec);
}

TEST_CASE("Vector bool: test conversions", "[vector]")
{
    test_binary_conversion({false}, "0");
    test_binary_conversion({true}, "1");
    test_binary_conversion({false, true, false, true, true, false, true}, "1011010");
    test_binary_conversion(
      {false, true, false, true, true, false, true, false, false, true, false}, "01001011010");

    test_hex_conversion({false, false, false, false}, "0");
    test_hex_conversion({true, false, false, false}, "1");
    test_hex_conversion({false, true, false, false}, "2");
    test_hex_conversion({false, false, true, false}, "4");
    test_hex_conversion({false, false, false, true}, "8");
    test_hex_conversion({true, false, false, true}, "9");
    test_hex_conversion({true, true, false, true}, "b");
    test_hex_conversion({true, true, true, true}, "f");

    test_hex_conversion({false, false, false, false, true, false, false, false}, "10");
    test_hex_conversion({true, false, false, false, false, false, false, false}, "01");
    test_hex_conversion({false, false, true, true, true, true, false, false}, "3c");
    test_hex_conversion({true, true, false, false, false, false, true, true}, "c3");

    test_hex_conversion(
      {true, true, true, true, false, true, true, false, true, false, false, false}, "16f");

    REQUIRE_THROWS_AS(vector_bool_from_binary_string("0120"), std::invalid_argument);
    REQUIRE_THROWS_AS(vector_bool_from_binary_string("0123456789abcef"), std::invalid_argument);
    REQUIRE_THROWS_AS(vector_bool_from_binary_string("qwerty"), std::invalid_argument);
    REQUIRE_THROWS_AS(vector_bool_from_hex_string("qwerty"), std::invalid_argument);
}

TEST_CASE("Vector bool: test operations", "[vector]")
{
    SECTION("Test 1")
    {
        std::vector<bool> v1(vector_bool_from_hex_string("16f"));
        std::vector<bool> v2(vector_bool_from_hex_string("abcd"));

        REQUIRE(get_true_count(v1) == 7);
        REQUIRE(get_true_count(v2) == 10);

        std::vector<bool> v_and(v1 & v2);
        std::vector<bool> v_xor(v1 ^ v2);

        REQUIRE(v_and == vector_bool_from_hex_string("014d"));
        REQUIRE(v_xor == vector_bool_from_hex_string("aaa2"));
    }

    SECTION("Test 2")
    {
        std::vector<bool> v1(vector_bool_from_hex_string("1"));
        std::vector<bool> v2(vector_bool_from_hex_string("ff"));

        REQUIRE(get_true_count(v1) == 1);
        REQUIRE(get_true_count(v2) == 8);

        std::vector<bool> v_and(v1 & v2);
        std::vector<bool> v_xor(v1 ^ v2);

        REQUIRE(v_and == vector_bool_from_hex_string("01"));
        REQUIRE(v_xor == vector_bool_from_hex_string("fe"));
    }
}
