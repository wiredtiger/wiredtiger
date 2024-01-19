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

TEST_CASE("Vector bool: test bit initialization", "[vector]")
{
    std::vector<bool> v1{false, false, false, true};
    REQUIRE(v1[0] == false);
    REQUIRE(v1[1] == false);
    REQUIRE(v1[2] == false);
    REQUIRE(v1[3] == true);

    std::vector<bool> v2{false, false, false, false, false, false, false, true};
    REQUIRE(v2[0] == false);
    REQUIRE(v2[1] == false);
    REQUIRE(v2[2] == false);
    REQUIRE(v2[3] == false);
    REQUIRE(v2[4] == false);
    REQUIRE(v2[5] == false);
    REQUIRE(v2[6] == false);
    REQUIRE(v2[7] == true);
}

TEST_CASE("Vector bool: test conversions", "[vector]")
{
    /*
     * Note: in the following tests, the std::vector<bool> is initialized least significant bit
     * first (so the first bool is stored in the 0th element of the vector) whereas the binary and
     * hex strings are written most significant bit/nibble first, as usual. This means that the
     * vector initialization and the binary/hex value may appear to be in the opposite order,
     * however they are actually in the same order as the following tests demonstrate.
     */

    test_binary_conversion({false}, "0");
    test_binary_conversion({true}, "1");
    test_binary_conversion({true, false}, "01");
    test_binary_conversion({false, true}, "10");
    test_binary_conversion({true, false, false}, "001");
    test_binary_conversion({false, false, true}, "100");
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

    test_hex_conversion({true, false, false, false, false, false, false, false}, "01");
    test_hex_conversion({false, true, false, false, false, false, false, false}, "02");
    test_hex_conversion({false, false, true, false, false, false, false, false}, "04");
    test_hex_conversion({false, false, false, true, false, false, false, false}, "08");
    test_hex_conversion({false, false, false, false, true, false, false, false}, "10");
    test_hex_conversion({false, false, false, false, false, true, false, false}, "20");
    test_hex_conversion({false, false, false, false, false, false, true, false}, "40");
    test_hex_conversion({false, false, false, false, false, false, false, true}, "80");

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

        REQUIRE(v_and == vector_bool_from_hex_string("14d"));
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

        REQUIRE(v_and == vector_bool_from_hex_string("1"));
        REQUIRE(v_xor == vector_bool_from_hex_string("fe"));
    }

    SECTION("Behavior Question")
    {
        std::vector<bool> v1(vector_bool_from_hex_string("1"));
        std::vector<bool> v2;
        std::vector<bool> v3;

        REQUIRE(v1.size() == 4);
        REQUIRE(v2.empty());
        REQUIRE(v3.empty());

        trim_most_significant_false_values(v1);
        REQUIRE(v1.size() == 1);

        REQUIRE(get_true_count(v1) == 1);
        REQUIRE(get_true_count(v2) == 0);
        REQUIRE(get_true_count(v3) == 0);

        std::vector<bool> v1_and_v2(v1 & v2);
        std::vector<bool> v2_and_v3(v2 & v3);

        REQUIRE(get_true_count(v2_and_v3) == 0);
        REQUIRE(v2_and_v3.empty());
        trim_most_significant_false_values(v2_and_v3);
        REQUIRE(v2_and_v3.empty());

        REQUIRE(get_true_count(v1_and_v2) == 0);
        REQUIRE(v1_and_v2.empty());
        trim_most_significant_false_values(v1_and_v2);
        REQUIRE(v1_and_v2.empty());
    }
}
