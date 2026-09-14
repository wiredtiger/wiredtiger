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

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

/* One mutex per shard. Budget is split equally and rounded up. */
template <typename K, typename V, typename GetSize, typename Hash = std::hash<K>>
class ConcurrentSizedLRUCache {
public:
    ConcurrentSizedLRUCache(size_t max_size, size_t n_shards) : hash(Hash{})
    {
        if (n_shards == 0)
            n_shards = 1;
        const size_t shard_bytes = shard_size(max_size, n_shards);
        shards.reserve(n_shards);
        for (size_t i = 0; i < n_shards; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->cache.set_max_size(shard_bytes);
            shards.push_back(std::move(shard));
        }
    }

    ConcurrentSizedLRUCache(const ConcurrentSizedLRUCache &) = delete;
    ConcurrentSizedLRUCache &operator=(const ConcurrentSizedLRUCache &) = delete;
    ConcurrentSizedLRUCache(ConcurrentSizedLRUCache &&) = delete;
    ConcurrentSizedLRUCache &operator=(ConcurrentSizedLRUCache &&) = delete;

    size_t
    put(const K &key, V &&entry)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.put(key, std::move(entry));
    }

    bool
    erase(const K &key)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.erase(key);
    }

    std::optional<V>
    get_erase(const K &key)
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.get_erase(key);
    }

    void
    clear()
    {
        for (auto &shard : shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            shard->cache.clear();
        }
    }

    bool
    contains(const K &key) const
    {
        auto &shard = shard_for(key);
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.cache.contains(key);
    }

    size_t
    size() const
    {
        size_t total = 0;
        for (const auto &shard : shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            total += shard->cache.size();
        }
        return total;
    }

    size_t
    count() const
    {
        size_t total = 0;
        for (const auto &shard : shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            total += shard->cache.count();
        }
        return total;
    }

    size_t
    set_max_size(size_t max_size)
    {
        const size_t shard_bytes = shard_size(max_size, shards.size());
        size_t evicted = 0;
        for (auto &shard : shards) {
            std::lock_guard<std::mutex> lock(shard->mtx);
            evicted += shard->cache.set_max_size(shard_bytes);
        }
        return evicted;
    }

private:
    struct Shard {
        mutable std::mutex mtx;
        SizedLRUCache<K, V, GetSize, Hash> cache;
    };

    static size_t
    shard_size(size_t max_size, size_t n_shards)
    {
        return (max_size + n_shards - 1) / n_shards;
    }

    Shard &
    shard_for(const K &key)
    {
        return *shards[hash(key) % shards.size()];
    }

    const Shard &
    shard_for(const K &key) const
    {
        return *shards[hash(key) % shards.size()];
    }

    std::vector<std::unique_ptr<Shard>> shards;
    Hash hash;
};
