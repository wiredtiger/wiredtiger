/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "mock_connection.h"
#include "../utils.h"

mock_connection::mock_connection(WT_CONNECTION_IMPL *connection_impl)
    : _connection_impl(connection_impl)
{
}

mock_connection::~mock_connection()
{
    __wt_free(nullptr, _connection_impl->chunkcache.free_bitmap);
    __wt_free(nullptr, _connection_impl);
}

std::shared_ptr<mock_connection>
mock_connection::build_test_mock_connection()
{
    WT_CONNECTION_IMPL *connection_impl = nullptr;
    utils::throw_if_non_zero(__wt_calloc(nullptr, 1, sizeof(WT_CONNECTION_IMPL), &connection_impl));
    // Construct a Session object that will now own session.
    return std::shared_ptr<mock_connection>(new mock_connection(connection_impl));
}

int
mock_connection::setup_chunk_cache(
  WT_SESSION_IMPL *session, uint64_t capacity, size_t chunk_size, WT_CHUNKCACHE *&chunkcache)
{
    chunkcache = &_connection_impl->chunkcache;
    memset(chunkcache, 0, sizeof(WT_CHUNKCACHE));
    chunkcache->capacity = capacity;
    chunkcache->chunk_size = chunk_size;
    WT_RET(
      __wt_calloc(session, WT_CHUNKCACHE_BITMAP_SIZE(chunkcache->capacity, chunkcache->chunk_size),
        sizeof(uint8_t), &chunkcache->free_bitmap));

    return 0;
}
