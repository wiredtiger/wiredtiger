/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include "wt_internal.h"
#include <map>
#include <string>

class config_parser {
public:
    config_parser(std::map<std::string, std::string> map);
    ~config_parser() = default;

    std::map<std::string, std::string> &get_config_map();
    void construct_config_string();
    const char **get_config_array();

private:
    std::map<std::string, std::string> _config_map;
    std::string _config_string;
    char *_cfg[3];
};
