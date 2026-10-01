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

#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

/*
 * The cache is shared by every handle the page log opens, so the table is part of the key: two
 * tables use the same page ids.
 */
struct victim_cache_key {
    uint64_t table_id;
    uint64_t page_id;
    uint64_t lsn;

    bool operator==(const victim_cache_key &) const = default;
};

template <> struct std::hash<victim_cache_key> {
    size_t
    operator()(const victim_cache_key &key) const noexcept
    {
        size_t h = std::hash<uint64_t>{}(key.table_id);
        h ^= std::hash<uint64_t>{}(key.page_id) << 1;
        h ^= std::hash<uint64_t>{}(key.lsn) << 2;
        return h;
    }
};

struct victim_cache_entry {
    uint64_t lsn;
    uint64_t backlink_lsn;
    uint64_t base_lsn;
    uint64_t backlink_checkpoint_id;
    uint64_t base_checkpoint_id;
    uint64_t delta_count;
    std::vector<uint8_t> data;
};

/*
 * One cache for the whole page log, bounded by the bytes it holds and evicting least recently used
 * first, which is how a production page log bounds its own. Either bound may be zero, meaning
 * unbounded in that dimension; the cache is unavailable only when both are.
 *
 * A read consumes the entry it returns, so nothing is ever used twice and recency order is also
 * insertion order. The list is kept anyway: it costs nothing and holds if that ever changes.
 */
class victim_cache {
public:
    explicit victim_cache(uint32_t max_entries, uint64_t max_bytes = 0)
        : max_entries(max_entries), max_bytes(max_bytes), total_bytes(0)
    {
    }

    victim_cache(const victim_cache &) = delete;
    victim_cache &operator=(const victim_cache &) = delete;
    victim_cache(victim_cache &&) = delete;
    victim_cache &operator=(victim_cache &&) = delete;

    bool
    available() const
    {
        return max_entries > 0 || max_bytes > 0;
    }

    /* Bytes held, the figure the page log interface has no way to report to WiredTiger. */
    uint64_t
    bytes() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return total_bytes;
    }

    bool
    erase(uint64_t table_id, uint64_t page_id, uint64_t lsn)
    {
        if (!available())
            return false;
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(victim_cache_key{table_id, page_id, lsn});
        if (it == map.end())
            return false;
        drop(it);
        return true;
    }

    std::optional<victim_cache_entry>
    get_erase(uint64_t table_id, uint64_t page_id, uint64_t lsn)
    {
        if (!available())
            return std::nullopt;
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(victim_cache_key{table_id, page_id, lsn});
        if (it == map.end())
            return std::nullopt;
        victim_cache_entry entry = std::move(it->second.entry);
        total_bytes -= entry_bytes(entry);
        order.erase(it->second.position);
        map.erase(it);
        return entry;
    }

    void
    put(uint64_t table_id, uint64_t page_id, victim_cache_entry &&entry)
    {
        if (!available())
            return;
        const victim_cache_key key{table_id, page_id, entry.lsn};
        const uint64_t cost = entry_bytes(entry);
        std::lock_guard<std::mutex> lock(mtx);

        /* A page too large for the whole cache would evict everything and still not fit. */
        if (max_bytes > 0 && cost > max_bytes)
            return;

        auto it = map.find(key);
        if (it != map.end())
            drop(it);

        /* Make room under whichever bounds are configured, oldest first. */
        while (!order.empty() &&
          ((max_entries > 0 && map.size() + 1 > max_entries) ||
            (max_bytes > 0 && total_bytes + cost > max_bytes)))
            drop(map.find(order.back()));

        order.push_front(key);
        total_bytes += cost;
        map.emplace(key, slot{std::move(entry), order.begin()});
    }

    bool
    contains(uint64_t table_id, uint64_t page_id, uint64_t lsn) const
    {
        if (!available())
            return false;
        std::lock_guard<std::mutex> lock(mtx);
        return map.contains(victim_cache_key{table_id, page_id, lsn});
    }

private:
    /* Per-entry bookkeeping beyond the image: key, restored metadata, vector and list nodes. */
    static constexpr uint64_t ENTRY_OVERHEAD = 128;

    struct slot {
        victim_cache_entry entry;
        std::list<victim_cache_key>::iterator position;
    };

    static uint64_t
    entry_bytes(const victim_cache_entry &entry)
    {
        return entry.data.capacity() + ENTRY_OVERHEAD;
    }

    /* Remove an entry and everything that accounts for it. The caller holds the lock. */
    void
    drop(std::unordered_map<victim_cache_key, slot>::iterator it)
    {
        total_bytes -= entry_bytes(it->second.entry);
        order.erase(it->second.position);
        map.erase(it);
    }

    const uint32_t max_entries;
    const uint64_t max_bytes;
    uint64_t total_bytes;
    mutable std::mutex mtx;
    std::list<victim_cache_key> order; /* Front is most recently added. */
    std::unordered_map<victim_cache_key, slot> map;
};
