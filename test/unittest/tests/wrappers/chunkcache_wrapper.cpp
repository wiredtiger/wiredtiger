/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wiredtiger.h"
#include "chunkcache_wrapper.h"

chunkcache_wrapper::chunkcache_wrapper(int capacity, int chunk_size)
{

    _chunkcache.capacity = capacity;
    _chunkcache.chunk_size = chunk_size;

    _chunkcache.free_bitmap = new uint8_t[(_chunkcache.capacity / _chunkcache.chunk_size) / 8];

    _chunkcache.type = WT_CHUNKCACHE_FILE;
    _chunkcache.bytes_used = 0;
    _chunkcache.hashtable = nullptr;
    _chunkcache.hashtable_size = 0;
    _chunkcache.storage_path = nullptr;
    _chunkcache.fh = nullptr;
    _chunkcache.memory = nullptr;
    _chunkcache.evict_thread_tid = {0, 0, 0};
    _chunkcache.evict_trigger = 0;
    _chunkcache.pinned_objects = {nullptr, 0, {{0, {0,0,0,0,0}},0, 0, 0, 0, 0, nullptr, nullptr}};
    _chunkcache.flags = WT_CHUNKCACHE_CONFIGURED;
}

chunkcache_wrapper::~chunkcache_wrapper()
{
    
    // _chunkcache.type = WT_CHUNKCACHE_FILE;
    // _chunkcache.bytes_used = 0;
    // _chunkcache.capacity = 0;
    // _chunkcache.chunk_size = 0;
    // _chunkcache.hashtable = nullptr;
    // _chunkcache.hashtable_size = 0;
    // _chunkcache.storage_path = nullptr;
    // _chunkcache.fh = nullptr;
    // _chunkcache.free_bitmap = nullptr;
    // _chunkcache.memory = nullptr;
    // _chunkcache.evict_thread_tid = NULL;
    // _chunkcache.evict_trigger = 0;
    // _chunkcache.pinned_objects = NULL;
    // _chunkcache.flags = WT_CHUNKCACHE_EXITING;
}
