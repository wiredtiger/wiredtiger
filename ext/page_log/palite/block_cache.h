/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 */

#pragma once

#include "sized_lru_cache.h"
#include "wiredtiger.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <tuple>
#include <utility>
#include <vector>

namespace palite {

struct CacheItemContent {
    uint64_t lsn;
    uint64_t backlink_lsn;
    uint64_t base_lsn;
    uint64_t backlink_checkpoint_id;
    uint64_t base_checkpoint_id;
    uint64_t delta_count;
    std::vector<uint8_t> data;
};

using CacheItem = std::unique_ptr<CacheItemContent>;

struct GetCacheItemSize {
    std::size_t
    operator()(const CacheItem &item) const
    {
        return item->data.size();
    }
};

using CacheKey = std::tuple<uint64_t, uint64_t, uint64_t>; /* table id, page id, lsn */

struct CacheKeyHash {
    std::size_t
    operator()(const CacheKey &key) const noexcept
    {
        const auto &[table_id, page_id, lsn] = key;
        std::size_t h = std::hash<uint64_t>{}(table_id);
        auto mix = [&](uint64_t x) {
            h ^= std::hash<uint64_t>{}(x) + 0x9e3779b9 + (h << 6) + (h >> 2);
        };
        mix(page_id);
        mix(lsn);
        return h;
    }
};

class BlockCache {
public:
    BlockCache(std::size_t sizeBytes, std::size_t numShards)
        : _maxSize(sizeBytes), _cache(sizeBytes, numShards == 0 ? 1 : numShards)
    {
    }

    BlockCache(const BlockCache &) = delete;
    BlockCache &operator=(const BlockCache &) = delete;
    BlockCache(BlockCache &&) = delete;
    BlockCache &operator=(BlockCache &&) = delete;

    bool
    available() const
    {
        return _maxSize.load(std::memory_order_relaxed) > 0;
    }

    std::size_t
    size() const
    {
        return _cache.size();
    }

    std::size_t
    count() const
    {
        return _cache.count();
    }

    void
    erase(uint64_t tableId, uint64_t pageId, uint64_t lsn)
    {
        _cache.erase(CacheKey(tableId, pageId, lsn));
    }

    /*
     * Consume-on-get: a hit copies the page into results_array and removes it from the cache.
     * Returns false on miss.
     */
    bool
    tryGet(uint64_t tableId, uint64_t pageId, WT_PAGE_LOG_GET_ARGS &getArgs, WT_ITEM *resultsArray,
      uint32_t &resultsCount)
    {
        if (!available())
            return false;

        auto value = _cache.getErase(CacheKey(tableId, pageId, getArgs.lsn));
        if (!value)
            return false;

        auto &item = *value;
        const std::size_t size = item->data.size();
        void *data = nullptr;
        if (size > 0) {
            data = std::malloc(size);
            if (data == nullptr)
                throw std::bad_alloc();
            std::memcpy(data, item->data.data(), size);
        }
        resultsArray[0].data = resultsArray[0].mem = data;
        resultsArray[0].size = resultsArray[0].memsize = size;

        getArgs.backlink_lsn = item->backlink_lsn;
        getArgs.base_lsn = item->base_lsn;
        getArgs.backlink_checkpoint_id = item->backlink_checkpoint_id;
        getArgs.base_checkpoint_id = item->base_checkpoint_id;
        getArgs.delta_count = item->delta_count;
        resultsCount = 1;
        return true;
    }

    int
    put(uint64_t tableId, uint64_t pageId, uint64_t checkpointId, const WT_PAGE_LOG_PUT_ARGS &putArgs,
      const WT_ITEM &buf)
    {
        if (!available())
            return 0;
        if (putArgs.flags & WT_PAGE_LOG_DELTA)
            return 0;

        auto cacheItem = std::make_unique<CacheItemContent>(CacheItemContent{putArgs.lsn,
          putArgs.backlink_lsn, putArgs.base_lsn, checkpointId, checkpointId, putArgs.delta_count,
          {}});
        if (buf.size > 0 && buf.data != nullptr) {
            const auto *p = static_cast<const uint8_t *>(buf.data);
            cacheItem->data.assign(p, p + buf.size);
        }
        _cache.add(CacheKey(tableId, pageId, putArgs.lsn), std::move(cacheItem));
        return 0;
    }

    int
    has(uint64_t tableId, uint64_t pageId, const WT_PAGE_LOG_PUT_ARGS &putArgs)
    {
        if (!available())
            return -1;
        return _cache.hasKey(CacheKey(tableId, pageId, putArgs.lsn)) ? 0 : -1;
    }

    int
    del(uint64_t tableId, uint64_t pageId, const WT_PAGE_LOG_PUT_ARGS &putArgs)
    {
        if (!available())
            return -1;
        return _cache.erase(CacheKey(tableId, pageId, putArgs.lsn)) ? 0 : -1;
    }

private:
    using LRUShard = SizedLRUCache<CacheKey, CacheItem, GetCacheItemSize, CacheKeyHash>;
    using LRUCache = ConcurrentSizedLRUCache<LRUShard, CacheKeyHash>;

    std::atomic<std::size_t> _maxSize;
    LRUCache _cache;
};

} /* namespace palite */
