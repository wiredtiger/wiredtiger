/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include "sized_lru_cache.h"

#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace palite {

/* One mutex per shard. maxSize is split equally, rounded up so a tiny budget still admits pages. */
template <class LRUShardType, typename Hash = std::hash<typename LRUShardType::key_type>>
class ConcurrentSizedLRUCache {
public:
    using key_type = typename LRUShardType::key_type;
    using mapped_type = typename LRUShardType::mapped_type;
    using K = key_type;
    using V = mapped_type;

    ConcurrentSizedLRUCache(std::size_t maxSize, std::size_t nShards) : _hash(Hash{})
    {
        assert(nShards > 0);
        const std::size_t shardSize = _shardSize(maxSize, nShards);
        _shards.reserve(nShards);
        for (std::size_t i = 0; i < nShards; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->cache.setMaxSize(shardSize);
            _shards.push_back(std::move(shard));
        }
    }

    ConcurrentSizedLRUCache(const ConcurrentSizedLRUCache &) = delete;
    ConcurrentSizedLRUCache &operator=(const ConcurrentSizedLRUCache &) = delete;
    ConcurrentSizedLRUCache(ConcurrentSizedLRUCache &&) = delete;
    ConcurrentSizedLRUCache &operator=(ConcurrentSizedLRUCache &&) = delete;

    std::size_t
    add(const K &key, V &&entry)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.add(key, std::move(entry));
    }

    bool
    erase(const K &key)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.erase(key) > 0;
    }

    std::optional<V>
    getErase(const K &key)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.getErase(key);
    }

    void
    clear()
    {
        for (auto &shard : _shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            shard->cache.clear();
        }
    }

    bool
    hasKey(const K &key) const
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.hasKey(key);
    }

    std::size_t
    size() const
    {
        std::size_t total = 0;
        for (const auto &shard : _shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            total += shard->cache.size();
        }
        return total;
    }

    std::size_t
    count() const
    {
        std::size_t total = 0;
        for (const auto &shard : _shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            total += shard->cache.count();
        }
        return total;
    }

    std::size_t
    setMaxSize(std::size_t maxSize)
    {
        const std::size_t shardSize = _shardSize(maxSize, _shards.size());
        std::size_t evicted = 0;
        for (auto &shard : _shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            evicted += shard->cache.setMaxSize(shardSize);
        }
        return evicted;
    }

private:
    struct Shard {
        mutable std::mutex mtx;
        LRUShardType cache;
    };

    static std::size_t
    _shardSize(std::size_t maxSize, std::size_t nShards)
    {
        return (maxSize + nShards - 1) / nShards;
    }

    Shard &
    shard_for(const K &key)
    {
        return *_shards[_hash(key) % _shards.size()];
    }

    const Shard &
    shard_for(const K &key) const
    {
        return *_shards[_hash(key) % _shards.size()];
    }

    std::vector<std::unique_ptr<Shard>> _shards;
    Hash _hash;
};

} /* namespace palite */
