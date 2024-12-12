/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace utils {

class LiveRestoreTestEnv {
public:
    const std::string _DB_DEST = "WT_LR_DEST";
    const std::string _DB_SOURCE = "WT_LR_SOURCE";
    const std::string _DB_HACKY_BACKUP = "WT_LR_HACKY_BACKUP";

    WT_LIVE_RESTORE_FS *_lr_fs;
    std::unique_ptr<connection_wrapper> _conn;
    WT_SESSION_IMPL *_session;

    LiveRestoreTestEnv() = delete;
    LiveRestoreTestEnv(bool verbose);

    ~LiveRestoreTestEnv();

    std::string dest_file_path(std::string file_name);
    std::string source_file_path(std::string file_name);
};

} // namespace utils.
