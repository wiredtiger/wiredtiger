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

#include "concurrent_sized_lru_cache.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

struct VictimCacheKey {
    uint64_t table_id;
    uint64_t page_id;
    uint64_t lsn;

    bool operator==(const VictimCacheKey &) const = default;
};

struct VictimCacheKeyHash {
    size_t
    operator()(const VictimCacheKey &key) const noexcept
    {
        size_t h = std::hash<uint64_t>{}(key.table_id);
        h ^= std::hash<uint64_t>{}(key.page_id) << 1;
        h ^= std::hash<uint64_t>{}(key.lsn) << 2;
        return h;
    }
};

struct VictimCacheEntry {
    uint64_t lsn;
    uint64_t backlink_lsn;
    uint64_t base_lsn;
    uint64_t backlink_checkpoint_id;
    uint64_t base_checkpoint_id;
    uint64_t delta_count;
    std::vector<uint8_t> data;
};

struct VictimCacheEntrySize {
    size_t
    operator()(const VictimCacheEntry &item) const
    {
        return item.data.size();
    }
};

class VictimCache {
public:
    VictimCache(size_t size_bytes, size_t n_shards)
        : max_size(size_bytes), cache(size_bytes, n_shards)
    {
    }

    VictimCache(const VictimCache &) = delete;
    VictimCache &operator=(const VictimCache &) = delete;
    VictimCache(VictimCache &&) = delete;
    VictimCache &operator=(VictimCache &&) = delete;

    bool
    available() const
    {
        return max_size > 0;
    }

    size_t
    size() const
    {
        return cache.size();
    }

    size_t
    count() const
    {
        return cache.count();
    }

    bool
    erase(uint64_t table_id, uint64_t page_id, uint64_t lsn)
    {
        return cache.erase(VictimCacheKey{table_id, page_id, lsn});
    }

    std::optional<VictimCacheEntry>
    get_erase(uint64_t table_id, uint64_t page_id, uint64_t lsn)
    {
        if (!available())
            return std::nullopt;
        return cache.get_erase(VictimCacheKey{table_id, page_id, lsn});
    }

    void
    put(uint64_t table_id, uint64_t page_id, VictimCacheEntry &&entry)
    {
        if (!available())
            return;
        const uint64_t lsn = entry.lsn;
        cache.put(VictimCacheKey{table_id, page_id, lsn}, std::move(entry));
    }

    bool
    contains(uint64_t table_id, uint64_t page_id, uint64_t lsn) const
    {
        if (!available())
            return false;
        return cache.contains(VictimCacheKey{table_id, page_id, lsn});
    }

private:
    const size_t max_size;
    ConcurrentSizedLRUCache<VictimCacheKey, VictimCacheEntry, VictimCacheEntrySize,
      VictimCacheKeyHash>
      cache;
};
