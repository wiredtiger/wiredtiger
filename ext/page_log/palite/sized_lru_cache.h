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

#include <cassert>
#include <cstddef>
#include <list>
#include <optional>
#include <unordered_map>
#include <utility>

/* Byte-budget LRU: list for recency, map for O(1) lookup. Not thread-safe. */
template <typename K, typename V> class sized_lru_cache {
public:
    static constexpr size_t PAGE_SIZE = 4096;

    sized_lru_cache() = default;
    explicit sized_lru_cache(size_t new_max_size)
    {
        set_max_size(new_max_size);
    }

    sized_lru_cache(const sized_lru_cache &) = delete;
    sized_lru_cache &operator=(const sized_lru_cache &) = delete;
    sized_lru_cache(sized_lru_cache &&) = delete;
    sized_lru_cache &operator=(sized_lru_cache &&) = delete;

    /* Replace any existing key. Returns bytes evicted to stay within the budget. */
    size_t
    put(const K &key, V &&entry)
    {
        auto i = map.find(key);
        if (i != map.end()) {
            cur_size -= i->second->entry.data.size();
            list.erase(i->second);
        }

        cur_size += entry.data.size();
        list.push_front(node{key, std::move(entry)});
        map[key] = list.begin();

        const size_t evicted = evict_to_max_size();
        assert(cur_size <= max_size);
        return evicted;
    }

    std::optional<V>
    get_erase(const K &key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return std::nullopt;

        V ret = std::move(it->second->entry);
        cur_size -= ret.data.size();
        list.erase(it->second);
        map.erase(it);
        return ret;
    }

    bool
    erase(const K &key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        cur_size -= it->second->entry.data.size();
        list.erase(it->second);
        map.erase(it);
        return true;
    }

    void
    clear()
    {
        map.clear();
        list.clear();
        cur_size = 0;
    }

    bool
    contains(const K &key) const
    {
        return map.find(key) != map.end();
    }

    size_t
    size() const
    {
        return cur_size;
    }

    size_t
    count() const
    {
        return list.size();
    }

    bool
    empty() const
    {
        return cur_size == 0;
    }

    size_t
    set_max_size(size_t new_max_size)
    {
        max_size = new_max_size;
        map.reserve(new_max_size / PAGE_SIZE);
        return evict_to_max_size();
    }

private:
    struct node {
        K key;
        V entry;
    };

    size_t
    evict_to_max_size()
    {
        size_t evicted_size = 0;
        while (cur_size > max_size) {
            auto &item = list.back();
            const auto sz = item.entry.data.size();
            evicted_size += sz;
            cur_size -= sz;
            map.erase(item.key);
            list.pop_back();
        }
        return evicted_size;
    }

    size_t max_size{0};
    size_t cur_size{0};
    std::list<node> list;
    std::unordered_map<K, typename std::list<node>::iterator> map;
};
