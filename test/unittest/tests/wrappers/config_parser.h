/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#pragma once

#include "wt_internal.h"
#include <map>
#include <string>

class config_parser {
public:
    explicit config_parser(const std::map<std::string, std::string>& map);
    ~config_parser() = default;

    std::map<std::string, std::string> &get_config_map();
    std::map<std::string, std::string> const& get_config_map() const;
    void construct_config_string();
    const char **get_config_array();

private:
    std::map<std::string, std::string> _config_map;
    std::string _config_string;
    char *_cfg[3];
};
