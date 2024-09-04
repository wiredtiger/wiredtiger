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
/*
 * WiredTiger requires a config string that is separated by "," to be passed into functions that
 * configure the system. The class aims to create a config parser where the creation of the config
 * string is controlled by a C++ map. The class should closely mimic the functionality of a map and
 * at the end, users would call get_config_array to construct the config string to be passed into
 * wiredtiger.
 *
 */
class config_parser {
public:
    explicit config_parser(const std::map<std::string, std::string> &map);
    ~config_parser() = default;

    // Fetch the configuration value. If it does not exist an exception will be returned.
    const std::string &get_config_value(std::string config) const;

    // Insert the configuration and value into map.
    void insert_config(std::string config, std::string value);

    // Erase the configuration and value into map. Return true if the erase was successful.
    bool erase_config(std::string config);

    // Get the configuration array used to be passed into wiredtiger.
    const char **get_config_array();

private:
    // Construct the WiredTiger configuration string.
    void construct_config_string();

    std::map<std::string, std::string> _config_map;
    std::string _config_string;
    char *_cfg[3];
};
