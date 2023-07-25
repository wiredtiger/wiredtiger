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
#include <string>

std::vector<bool>
vector_bool_from_hex_string(std::string const &hex_str)
{
    std::vector<bool> result;

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
    while (iter != vector_bool.cend()) {
        int bit_count = 0;
        int hex_digit_value = 0;
        while ((bit_count < 4) && (iter != vector_bool.cend())) {
            int bit = *iter++;
            hex_digit_value |= (bit & 1) << bit_count;
            bit_count++;
        }
        char hex_digit = 0;
        std::to_chars(&hex_digit, &hex_digit + 1, hex_digit_value, 16);
        result.push_back(hex_digit);
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
    const unsigned smaller = std::min(a.size(), b.size());
    const unsigned larger = std::max(a.size(), b.size());

    std::vector<bool> result;

    for (unsigned i = 0; i < smaller; i++)
        result.push_back(a[i] & b[i]);

    for (unsigned i = smaller; i < larger; i++)
        result.push_back(false);

    return result;
}

std::vector<bool>
operator^(std::vector<bool> const &a, std::vector<bool> const &b)
{
    const unsigned smaller = std::min(a.size(), b.size());
    const unsigned larger = std::max(a.size(), b.size());

    std::vector<bool> result;
    result.reserve(larger);

    for (unsigned i = 0; i < smaller; i++)
        result.push_back(a[i] != b[i]);

    std::vector<bool> const &source = a.size() > b.size() ? a : b;

    for (unsigned i = smaller; i < larger; i++) {
        result.push_back(source[i]);
    }

    return result;
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
