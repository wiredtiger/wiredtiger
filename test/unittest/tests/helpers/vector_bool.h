/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include <string>
#include <vector>

/*
 * The following operations treat the std::vector<bool> as a binary number,
 * with the least significant bit stored in the 0th element.
 */

std::vector<bool> vector_bool_from_hex_string(std::string const &str);
std::vector<bool> vector_bool_from_binary_string(std::string const &str);

std::string vector_bool_to_binary_string(std::vector<bool> const &vector_bool);
std::string vector_bool_to_hex_string(std::vector<bool> const &vector_bool);

void trim_most_significant_false_values(std::vector<bool> &vector_bool);
unsigned get_true_count(std::vector<bool> const &vector_bool);

std::vector<bool> operator&(std::vector<bool> const &a, std::vector<bool> const &b);
std::vector<bool> operator^(std::vector<bool> const &a, std::vector<bool> const &b);
