/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace palite {

/*
 * Byte-budget LRU: list for recency, map for O(1) lookup. Not thread-safe; pair with
 * ConcurrentSizedLRUCache for sharded access.
 */
template <typename K, typename V, typename GetSize, typename Hash = std::hash<K>>
class SizedLRUCache {
public:
    using ListEntry = std::pair<K, V>;
    using List = std::list<ListEntry>;
    using iterator = typename List::iterator;
    using Map = std::unordered_map<K, iterator, Hash>;
    using key_type = K;
    using mapped_type = V;

    static constexpr std::size_t kPageSize = 4096;

    SizedLRUCache() = default;
    explicit SizedLRUCache(std::size_t maxSize)
    {
        setMaxSize(maxSize);
    }

    SizedLRUCache(const SizedLRUCache &) = delete;
    SizedLRUCache &operator=(const SizedLRUCache &) = delete;
    SizedLRUCache(SizedLRUCache &&) = delete;
    SizedLRUCache &operator=(SizedLRUCache &&) = delete;

    /* Replace any existing key. Returns bytes evicted to stay within the budget. */
    std::size_t
    add(const K &key, V &&entry)
    {
        auto i = _map.find(key);
        if (i != _map.end()) {
            _curSize -= _getSize(i->second->second);
            _list.erase(i->second);
        }

        _curSize += _getSize(entry);
        _list.push_front(std::make_pair(key, std::move(entry)));
        _map[key] = _list.begin();

        const std::size_t evicted = _evictToMaxSize();
        assert(_curSize <= _maxSize);
        return evicted;
    }

    std::optional<V>
    getErase(const K &key)
    {
        auto it = _map.find(key);
        if (it == _map.end())
            return std::nullopt;

        V ret = std::move(it->second->second);
        _curSize -= _getSize(ret);
        _list.erase(it->second);
        _map.erase(it);
        return ret;
    }

    typename Map::size_type
    erase(const K &key)
    {
        auto it = _map.find(key);
        if (it == _map.end())
            return 0;

        _curSize -= _getSize(it->second->second);
        _list.erase(it->second);
        _map.erase(it);
        return 1;
    }

    void
    clear()
    {
        _map.clear();
        _list.clear();
        _curSize = 0;
    }

    bool
    hasKey(const K &key) const
    {
        return _map.find(key) != _map.end();
    }

    std::size_t
    size() const
    {
        return _curSize;
    }

    std::size_t
    count() const
    {
        return _list.size();
    }

    bool
    empty() const
    {
        return _curSize == 0;
    }

    std::size_t
    setMaxSize(std::size_t maxSize)
    {
        _maxSize = maxSize;
        _map.reserve(maxSize / kPageSize);
        return _evictToMaxSize();
    }

private:
    std::size_t
    _evictToMaxSize()
    {
        std::size_t evictedSize = 0;
        while (_curSize > _maxSize) {
            auto &pair = _list.back();
            const auto sz = _getSize(pair.second);
            evictedSize += sz;
            _curSize -= sz;
            _map.erase(pair.first);
            _list.pop_back();
        }
        return evictedSize;
    }

    std::size_t _maxSize{0};
    std::size_t _curSize{0};
    GetSize _getSize{};
    List _list;
    Map _map;
};

/* One mutex per shard. maxSize is split equally, rounded up so a tiny budget still admits entries. */
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
