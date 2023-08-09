/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "wt_internal.h"

class chunkcache_wrapper {
public:
    explicit chunkcache_wrapper(int capacity, int chunk_size);
    ~chunkcache_wrapper();
    WT_CHUNKCACHE *
    chunkcache_get()
    {
        return &_chunkcache;
    };

private:
    WT_CHUNKCACHE _chunkcache;
};
