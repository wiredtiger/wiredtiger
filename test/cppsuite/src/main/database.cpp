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

#include "database.h"

#include "collection.h"
#include "src/common/constants.h"
#include "src/common/random_generator.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
std::string
Database::GenerateCollectionName(const uint64_t id)
{
    return (std::string("table:collection_" + std::to_string(id)));
}

void
Database::AddCollection(uint64_t key_count)
{
    std::lock_guard<std::mutex> lg(_mutex);
    if (_session.Get() == nullptr)
        _session = ConnectionManager::GetInstance().CreateSession();
    if (_collectionCreateConfig.empty())
        testutil_die(EINVAL, "database: no collection create config specified!");
    uint64_t nextId = _nextCollectionId++;
    std::string collectionName = GenerateCollectionName(nextId);
    /* FIX-ME-Test-Framework: This will get removed when we split the model up. */
    _collections.emplace(std::piecewise_construct, std::forward_as_tuple(nextId),
      std::forward_as_tuple(nextId, key_count, collectionName));
    testutil_check(
      _session->create(_session.Get(), collectionName.c_str(), _collectionCreateConfig.c_str()));
    _operationTracker->saveSchemaOperation(
      TrackingOperation::kCreateCollection, nextId, _timestampManager->GetNextTimestamp());
}

Collection &
Database::GetCollection(uint64_t id)
{
    std::lock_guard<std::mutex> lg(_mutex);
    const auto it = _collections.find(id);
    if (it == _collections.end())
        testutil_die(EINVAL, "tried to get collection that doesn't exist.");
    return (it->second);
}

Collection &
Database::GetRandomCollection()
{
    size_t collectionCount = GetCollectionCount();
    /* Any caller should expect at least one collection to exist. */
    testutil_assert(collectionCount != 0);
    return (GetCollection(
      RandomGenerator::GetInstance().GenerateInteger<uint64_t>(0, collectionCount - 1)));
}

uint64_t
Database::GetCollectionCount()
{
    std::lock_guard<std::mutex> lg(_mutex);
    return (_collections.size());
}

std::vector<std::string>
Database::GetCollectionNames()
{
    std::lock_guard<std::mutex> lg(_mutex);
    std::vector<std::string> collectionNames;

    for (auto const &it : _collections)
        collectionNames.push_back(it.second.name);

    return (collectionNames);
}

std::vector<uint64_t>
Database::GetCollectionIds()
{
    std::lock_guard<std::mutex> lg(_mutex);
    std::vector<uint64_t> collectionIds;

    for (auto const &it : _collections)
        collectionIds.push_back(it.first);

    return (collectionIds);
}

void
Database::SetTimestampManager(TimestampManager *tsm)
{
    testutil_assert(_timestampManager == nullptr);
    _timestampManager = tsm;
}

void
Database::SetOperationTracker(OperationTracker *op_tracker)
{
    testutil_assert(_operationTracker == nullptr);
    _operationTracker = op_tracker;
}

void
Database::SetCreateConfig(bool useCompression, bool useReverseCollator)
{
    _collectionCreateConfig = kDefaultFrameworkSchema;
    _collectionCreateConfig += useCompression ? std::string(SNAPPY_BLK) + "," : "";
    _collectionCreateConfig += useReverseCollator ? std::string(REVERSE_COL_CFG) + "," : "";
}
} // namespace test_harness
