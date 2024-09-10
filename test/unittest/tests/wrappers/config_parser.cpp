/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include "config_parser.h"
#include <string>

config_parser::config_parser(const std::map<std::string, std::string> &map)
    : _config_map(std::move(map)), _cfg{nullptr, nullptr, nullptr}
{
}

std::map<std::string, std::string> &
config_parser::get_config_map()
{
    return _config_map;
}

std::map<std::string, std::string> const &
config_parser::get_config_map() const
{
    return _config_map;
}

void
config_parser::construct_config_string()
{
    std::string config_string;
    for (const auto &config : _config_map)
        config_string += config.first + "=" + config.second + ",";
    _config_string = config_string;
    _cfg[0] = _config_string.data();
}

const char **
config_parser::get_config_array()
{
    construct_config_string();
    return const_cast<const char **>(_cfg);
}
