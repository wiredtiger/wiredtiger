/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

extern "C" {
#include "wt_internal.h"
}

#include <list>
#include <memory>
#include <mutex>
#include <new>
#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct CacheKey {
    uint64_t tableid;
    uint64_t pageid;
    uint64_t lsn;
};

struct CacheKeyHash {
    size_t operator()(const CacheKey &key) const noexcept
    {
        size_t h = std::hash<uint64_t>{}(key.tableid);
        h = (h * 1315423911u) + std::hash<uint64_t>{}(key.pageid);
        h = (h * 1315423911u) + std::hash<uint64_t>{}(key.lsn);
        return h;
    }
};

struct CacheKeyEqual {
    bool operator()(const CacheKey &a, const CacheKey &b) const noexcept
    {
        return a.tableid == b.tableid && a.pageid == b.pageid && a.lsn == b.lsn;
    }
};

struct WtFreeDeleter {
    void operator()(uint8_t *p) const noexcept
    {
        if (p != nullptr)
            __wt_free_int(NULL, &p);
    }
};

struct CacheEntry {
    uint64_t backlink_lsn = 0;
    uint64_t base_lsn = 0;
    uint64_t backlink_checkpoint_id = 0;
    uint64_t base_checkpoint_id = 0;
    uint64_t delta_count = 0;
    WT_PAGE_LOG_ENCRYPTION encryption = {};
    size_t size = 0;
    std::unique_ptr<uint8_t, WtFreeDeleter> data;
};

using CacheValue = std::unique_ptr<CacheEntry>;

struct CacheEntrySize {
    size_t operator()(const CacheValue &entry) const noexcept
    {
        return entry ? entry->size : 0;
    }
};

template <typename K, typename V, typename GetSize, typename Hash, typename KeyEqual>
class SizedLRUCache {
public:
    using key_type = K;
    using mapped_type = V;
    using ListEntry = std::pair<K, V>;
    using List = std::list<ListEntry>;
    using iterator = typename List::iterator;
    using Map = std::unordered_map<K, iterator, Hash, KeyEqual>;

    SizedLRUCache() : _max_size(0), _cur_size(0), _get_size(GetSize{}) {}
    explicit SizedLRUCache(size_t max_size) : _max_size(0), _cur_size(0), _get_size(GetSize{})
    {
        setMaxSize(max_size);
    }

    SizedLRUCache(SizedLRUCache &&) = delete;
    SizedLRUCache &operator=(SizedLRUCache &&) = delete;
    SizedLRUCache(const SizedLRUCache &) = delete;
    SizedLRUCache &operator=(const SizedLRUCache &) = delete;

    size_t add(const K &key, V &&entry)
    {
        auto it = _map.find(key);
        if (it != _map.end()) {
            _cur_size -= _get_size(it->second->second);
            _list.erase(it->second);
        }

        _cur_size += _get_size(entry);
        _list.emplace_front(key, std::move(entry));
        _map[key] = _list.begin();

        size_t evicted_size = 0;
        while (_cur_size > _max_size && !_list.empty()) {
            auto &pair = _list.back();
            const size_t sz = _get_size(pair.second);
            evicted_size += sz;
            _cur_size -= sz;
            _map.erase(pair.first);
            _list.pop_back();
        }

        return evicted_size;
    }

    bool get_erase(const K &key, V *out)
    {
        auto it = _map.find(key);
        if (it == _map.end())
            return false;

        const size_t sz = _get_size(it->second->second);
        _cur_size -= sz;

        if (out != nullptr)
            *out = std::move(it->second->second);

        _list.erase(it->second);
        _map.erase(it);
        return true;
    }

    bool erase(const K &key)
    {
        auto it = _map.find(key);
        if (it == _map.end())
            return false;

        _cur_size -= _get_size(it->second->second);
        _list.erase(it->second);
        _map.erase(it);
        return true;
    }

    void clear()
    {
        _map.clear();
        _list.clear();
        _cur_size = 0;
    }

    size_t size() const
    {
        return _cur_size;
    }

    size_t count() const
    {
        return _list.size();
    }

    void setMaxSize(size_t max_size)
    {
        _max_size = max_size;
        _map.reserve(max_size / 4096);
    }

private:
    size_t _max_size;
    size_t _cur_size;
    GetSize _get_size;
    List _list;
    Map _map;
};

template <typename LRUShardType, typename Hash>
class ConcurrentSizedLRUCache {
public:
    using key_type = typename LRUShardType::key_type;
    using mapped_type = typename LRUShardType::mapped_type;
    using K = key_type;
    using V = mapped_type;

    explicit ConcurrentSizedLRUCache(size_t max_size, size_t shards) : _hash(Hash{})
    {
        const size_t n = shards == 0 ? 1 : shards;
        _shards.reserve(n);
        const size_t shard_size = max_size / n;
        for (size_t i = 0; i < n; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->cache.setMaxSize(shard_size);
            _shards.push_back(std::move(shard));
        }
    }

    ConcurrentSizedLRUCache(ConcurrentSizedLRUCache &&) = delete;
    ConcurrentSizedLRUCache &operator=(ConcurrentSizedLRUCache &&) = delete;
    ConcurrentSizedLRUCache(const ConcurrentSizedLRUCache &) = delete;
    ConcurrentSizedLRUCache &operator=(const ConcurrentSizedLRUCache &) = delete;

    size_t add(const K &key, V &&entry)
    {
        auto &shard = *_shards[_hash(key) % _shards.size()];
        std::lock_guard<std::mutex> lk(shard.mutex);
        return shard.cache.add(key, std::move(entry));
    }

    bool get_erase(const K &key, V *out)
    {
        auto &shard = *_shards[_hash(key) % _shards.size()];
        std::lock_guard<std::mutex> lk(shard.mutex);
        return shard.cache.get_erase(key, out);
    }

    bool erase(const K &key)
    {
        auto &shard = *_shards[_hash(key) % _shards.size()];
        std::lock_guard<std::mutex> lk(shard.mutex);
        return shard.cache.erase(key);
    }

    void clear()
    {
        for (auto &shard_ptr : _shards) {
            auto &shard = *shard_ptr;
            std::lock_guard<std::mutex> lk(shard.mutex);
            shard.cache.clear();
        }
    }

    size_t size() const
    {
        size_t total = 0;
        for (auto &shard_ptr : _shards) {
            auto &shard = *shard_ptr;
            std::lock_guard<std::mutex> lk(shard.mutex);
            total += shard.cache.size();
        }
        return total;
    }

    size_t count() const
    {
        size_t total = 0;
        for (auto &shard_ptr : _shards) {
            auto &shard = *shard_ptr;
            std::lock_guard<std::mutex> lk(shard.mutex);
            total += shard.cache.count();
        }
        return total;
    }

private:
    struct Shard {
        std::mutex mutex;
        LRUShardType cache;
    };

    std::vector<std::unique_ptr<Shard>> _shards;
    Hash _hash;
};

using CacheShard =
  SizedLRUCache<CacheKey, CacheValue, CacheEntrySize, CacheKeyHash, CacheKeyEqual>;
using Cache = ConcurrentSizedLRUCache<CacheShard, CacheKeyHash>;

} // namespace

struct __wt_disagg_victim_cache {
    explicit __wt_disagg_victim_cache(uint64_t max_size, uint32_t shards)
        : cache(max_size, shards == 0 ? 1 : shards)
    {
    }

    Cache cache;
};

extern "C" {

int
__wt_disagg_cache_configure(WT_CONNECTION_IMPL *conn, uint64_t max_size, uint32_t shards)
{
    WT_DISAGGREGATED_STORAGE *disagg;

    if (conn == NULL)
        return (EINVAL);

    disagg = &conn->disaggregated_storage;

    if (max_size == 0 || shards == 0) {
        if (disagg->victim_cache != NULL) {
            delete disagg->victim_cache;
            disagg->victim_cache = NULL;
        }
        disagg->victim_cache_size = 0;
        disagg->victim_cache_shards = 0;
        return (0);
    }

    if (disagg->victim_cache != NULL &&
      disagg->victim_cache_size == max_size && disagg->victim_cache_shards == shards)
        return (0);

    if (disagg->victim_cache != NULL) {
        delete disagg->victim_cache;
        disagg->victim_cache = NULL;
    }

    disagg->victim_cache = new (std::nothrow) WT_DISAGG_VICTIM_CACHE(max_size, shards);
    if (disagg->victim_cache == NULL)
        return (ENOMEM);

    disagg->victim_cache_size = max_size;
    disagg->victim_cache_shards = shards;
    return (0);
}

void
__wt_disagg_cache_destroy(WT_CONNECTION_IMPL *conn)
{
    WT_DISAGGREGATED_STORAGE *disagg;

    if (conn == NULL)
        return;

    disagg = &conn->disaggregated_storage;
    if (disagg->victim_cache != NULL) {
        delete disagg->victim_cache;
        disagg->victim_cache = NULL;
    }
    disagg->victim_cache_size = 0;
    disagg->victim_cache_shards = 0;
}

bool
__wt_disagg_cache_configured(WT_CONNECTION_IMPL *conn)
{
    return (conn != NULL && conn->disaggregated_storage.victim_cache != NULL);
}

int
__wt_disagg_cache_put(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg, uint64_t page_id,
  const WT_PAGE_LOG_PUT_ARGS *put_args, const WT_ITEM *buf)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    WT_DISAGG_VICTIM_CACHE *cache;
    CacheKey key;
    CacheValue entry;
    uint8_t *copy;

    if (session == NULL || block_disagg == NULL || put_args == NULL || buf == NULL)
        return (EINVAL);

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    cache = disagg->victim_cache;
    if (cache == NULL)
        return (0);

    entry = std::make_unique<CacheEntry>();
    entry->backlink_lsn = put_args->backlink_lsn;
    entry->base_lsn = put_args->base_lsn;
    entry->backlink_checkpoint_id = put_args->backlink_checkpoint_id;
    entry->base_checkpoint_id = put_args->base_checkpoint_id;
    entry->delta_count = put_args->delta_count;
    entry->encryption = put_args->encryption;
    entry->size = buf->size;

    copy = NULL;
    WT_RET(__wt_malloc(session, buf->size, &copy));
    memcpy(copy, buf->data, buf->size);
    entry->data.reset(copy);

    key.tableid = block_disagg->tableid;
    key.pageid = page_id;
    key.lsn = put_args->lsn;

    cache->cache.add(key, std::move(entry));
    return (0);
}

int
__wt_disagg_cache_get(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg, uint64_t page_id,
  uint64_t lsn, WT_PAGE_LOG_GET_ARGS *get_args, WT_ITEM *results_array, uint32_t *results_count,
  bool *foundp)
{
    WT_CONNECTION_IMPL *conn;
    WT_DISAGGREGATED_STORAGE *disagg;
    WT_DISAGG_VICTIM_CACHE *cache;
    CacheKey key;
    CacheValue entry;
    bool found;

    if (foundp != NULL)
        *foundp = false;

    if (session == NULL || block_disagg == NULL || get_args == NULL || results_array == NULL ||
      results_count == NULL)
        return (EINVAL);

    conn = S2C(session);
    disagg = &conn->disaggregated_storage;
    cache = disagg->victim_cache;
    if (cache == NULL)
        return (0);

    key.tableid = block_disagg->tableid;
    key.pageid = page_id;
    key.lsn = lsn;

    found = cache->cache.get_erase(key, &entry);
    if (!found)
        return (0);

    if (*results_count < 1)
        return (EINVAL);

    void *raw = entry->data.release();
    results_array[0].data = raw;
    results_array[0].mem = raw;
    results_array[0].size = entry->size;
    results_array[0].memsize = entry->size;
    results_array[0].flags = 0;
    *results_count = 1;

    get_args->backlink_lsn = entry->backlink_lsn;
    get_args->base_lsn = entry->base_lsn;
    get_args->backlink_checkpoint_id = entry->backlink_checkpoint_id;
    get_args->base_checkpoint_id = entry->base_checkpoint_id;
    get_args->delta_count = entry->delta_count;
    get_args->encryption = entry->encryption;

    if (foundp != NULL)
        *foundp = true;

    return (0);
}

} /* extern "C" */
