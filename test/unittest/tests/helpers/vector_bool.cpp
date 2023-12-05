/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "vector_bool.h"
#include <algorithm>
#include <charconv>
#include <iostream>

/*
 * This file contains helper functions to manipulate std::vector<bool> objects.
 *
 * These functions consider the first (with index 0) bool to be the least significant bit
 *
 * std::vector<bool> v1 {false, false, false, true} creates a vector using list initialization
 * with each bool in the vector initialized in order from the first (0th) element to the last.
 * In this example, the vector will contain four bool values such that
 *   v1[0] == false
 *   v1[1] == false
 *   v1[2] == false
 *   v1[3] == true
 * meaning that it is equivalent to 0b1000 or 0x8
 *
 * See https://en.cppreference.com/w/cpp/container/vector/vector and
 * https://en.cppreference.com/w/cpp/language/list_initialization
 */

/*
 * vector_bool_from_hex_string()
 *
 *
 */
std::vector<bool>
vector_bool_from_hex_string(std::string const &hex_str)
{
    std::vector<bool> result;

    // Reverse iterate through the hex string.
    // We start with the last character as that contains the least significant bits.
    for (auto iter = hex_str.crbegin(); iter != hex_str.crend(); ++iter) {
        int value_of_hex_char = std::stoi(std::string(1, *iter), nullptr, 16);
        result.push_back((value_of_hex_char & 1) != 0);
        result.push_back((value_of_hex_char & 2) != 0);
        result.push_back((value_of_hex_char & 4) != 0);
        result.push_back((value_of_hex_char & 8) != 0);
    }

    return result;
}

std::string
vector_bool_to_hex_string(std::vector<bool> const &vector_bool)
{
    std::string result;

    auto iter = vector_bool.cbegin();
    int bit_count = 0;
    int hex_digit_value = 0;
    while (iter != vector_bool.cend()) {
        int bit = *iter++;
        hex_digit_value |= (bit & 1) << bit_count;
        bit_count++;
        if (bit_count == 4) {
            char hex_digit = 0;
            std::to_chars(&hex_digit, &hex_digit + 1, hex_digit_value, 16);
            result.push_back(hex_digit);
            bit_count = 0;
            hex_digit_value = 0;
        }
    }

    std::reverse(result.begin(), result.end());
    return result;
}

std::vector<bool>
vector_bool_from_binary_string(std::string const &binary_str)
{
    std::vector<bool> result;

    for (auto iter = binary_str.crbegin(); iter != binary_str.crend(); iter++) {
        char ch = *iter;
        bool bit = false;
        switch (ch) {
        case '0':
            bit = false;
            break;
        case '1':
            bit = true;
            break;
        default:
            throw(std::invalid_argument("Binary value is not 0 or 1"));
        }
        result.push_back(bit);
    }

    return result;
}

std::string
vector_bool_to_binary_string(std::vector<bool> const &vector_bool)
{
    std::string result;

    for (auto iter = vector_bool.crbegin(); iter != vector_bool.crend(); iter++) {
        char ch = *iter ? '1' : '0';
        result.push_back(ch);
    }

    return result;
}

std::vector<bool>
operator&(std::vector<bool> const &a, std::vector<bool> const &b)
{
    const size_t smaller = std::min(a.size(), b.size());
    const size_t larger = std::max(a.size(), b.size());

    std::vector<bool> result;
    result.reserve(larger);

    for (size_t i = 0; i < smaller; i++)
        result.push_back(a[i] & b[i]);

    for (size_t i = smaller; i < larger; i++)
        result.push_back(false);

    return result;
}

std::vector<bool>
operator^(std::vector<bool> const &a, std::vector<bool> const &b)
{
    const size_t smaller = std::min(a.size(), b.size());
    const size_t larger = std::max(a.size(), b.size());

    std::vector<bool> result;
    result.reserve(larger);

    for (size_t i = 0; i < smaller; i++)
        result.push_back(a[i] != b[i]);

    std::vector<bool> const &source = a.size() > b.size() ? a : b;

    for (size_t i = smaller; i < larger; i++) {
        result.push_back(source[i]);
    }

    return result;
}

void
trim_most_significant_false_values(std::vector<bool> &vector_bool)
{
    while (!vector_bool.empty() && !vector_bool.back())
        vector_bool.pop_back();
}

unsigned
get_true_count(std::vector<bool> const &vector_bool)
{
    unsigned count = 0;

    for (auto i : vector_bool)
        if (i)
            count++;

    return count;
}
