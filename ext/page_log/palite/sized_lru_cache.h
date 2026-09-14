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

namespace palite {

/* Byte-budget LRU: list for recency, map for O(1) lookup. Not thread-safe. */
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

} /* namespace palite */
