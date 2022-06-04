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

#include "operation_tracker.h"

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
OperationTracker::OperationTracker(
  configuration *_config, const bool useCompression, TimestampManager &tsm)
    : Component(operationTracker, _config), _operationTableName(tableNameOpWorkloadTracker),
      _schemaTableConfig(SCHEMA_TRACKING_TABLE_CONFIG),
      _schemaTableName(tableNameSchemaWorkloadTracker), _useCompression(useCompression),
      _timestampManager(tsm)
{
    _operationTableConfig = "key_format=" + _config->get_string(trackingKeyFormat) +
      ",value_format=" + _config->get_string(trackingValueFormat) + ",log=(enabled=true)";
}

const std::string &
OperationTracker::getSchemaTableName() const
{
    return (_schemaTableName);
}

const std::string &
OperationTracker::getOperationTableName() const
{
    return (_operationTableName);
}

void
OperationTracker::Load()
{
    Component::Load();

    if (!_enabled)
        return;

    /* Initiate schema tracking. */
    _session = connection_manager::instance().create_session();
    testutil_check(
      _session->create(_session.get(), _schemaTableName.c_str(), _schemaTableConfig.c_str()));
    _schemaTrackingCursor = _session.open_scoped_cursor(_schemaTableName);
    Logger::LogMessage(LOG_TRACE, "Schema tracking initiated");

    /* Initiate operations tracking. */
    testutil_check(
      _session->create(_session.get(), _operationTableName.c_str(), _operationTableConfig.c_str()));
    Logger::LogMessage(LOG_TRACE, "Operations tracking created");

    /*
     * Open sweep cursor in a dedicated sweep session. This cursor will be used to clear out
     * obsolete data from the tracking table.
     */
    _sweepSession = connection_manager::instance().create_session();
    _sweepCursor = _sweepSession.open_scoped_cursor(_operationTableName);
    Logger::LogMessage(LOG_TRACE, "Tracking table sweep initialized");
}

void
OperationTracker::DoWork()
{
    WT_DECL_RET;
    wt_timestamp_t timestamp, oldestTimestamp;
    uint64_t collectionId, sweepCollectionId;
    int operationType;
    const char *key, *value;
    char *sweepKey;
    bool globallyVisibleUpdateFound;

    /*
     * This function prunes old data from the tracking table as the default validation logic doesn't
     * use it. User-defined validation may need this data, so don't allow it to be removed.
     */
    const std::string key_format(_sweepCursor->key_format);
    const std::string value_format(_sweepCursor->value_format);
    if (key_format != OPERATION_TRACKING_KEY_FORMAT ||
      value_format != OPERATION_TRACKING_VALUE_FORMAT)
        return;

    key = sweepKey = nullptr;
    globallyVisibleUpdateFound = false;

    /* Take a copy of the oldest so that we sweep with a consistent timestamp. */
    oldestTimestamp = _timestampManager.GetOldestTimestamp();

    /* We need to check if the component is still running to avoid unnecessary iterations. */
    while (_running && (ret = _sweepCursor->prev(_sweepCursor.get())) == 0) {
        testutil_check(_sweepCursor->get_key(_sweepCursor.get(), &collectionId, &key, &timestamp));
        testutil_check(_sweepCursor->get_value(_sweepCursor.get(), &operationType, &value));
        /*
         * If we're on a new key, reset the check. We want to track whether we have a globally
         * visible update for the current key.
         */
        if (sweepKey == nullptr || sweepCollectionId != collectionId ||
          strcmp(sweepKey, key) != 0) {
            globallyVisibleUpdateFound = false;
            if (sweepKey != nullptr)
                free(sweepKey);
            sweepKey = static_cast<char *>(dstrdup(key));
            sweepCollectionId = collectionId;
        }
        if (timestamp <= oldestTimestamp) {
            if (globallyVisibleUpdateFound) {
                if (Logger::traceLevel == LOG_TRACE)
                    Logger::LogMessage(LOG_TRACE,
                      std::string("workload tracking: Obsoleted update, key=") + sweepKey +
                        ", collectionId=" + std::to_string(collectionId) +
                        ", timestamp=" + std::to_string(timestamp) + ", oldest_timestamp=" +
                        std::to_string(oldestTimestamp) + ", value=" + value);
                /*
                 * Wrap the removal in a transaction as we need to specify we aren't using a
                 * timestamp on purpose.
                 */
                testutil_check(
                  _sweepSession->begin_transaction(_sweepSession.get(), "no_timestamp=true"));
                testutil_check(_sweepCursor->remove(_sweepCursor.get()));
                testutil_check(_sweepSession->commit_transaction(_sweepSession.get(), nullptr));
            } else if (static_cast<trackingOperation>(operationType) == trackingOperation::INSERT) {
                if (Logger::traceLevel == LOG_TRACE)
                    Logger::LogMessage(LOG_TRACE,
                      std::string("workload tracking: Found globally visible update, key=") +
                        sweepKey + ", collectionId=" + std::to_string(collectionId) +
                        ", timestamp=" + std::to_string(timestamp) + ", oldest_timestamp=" +
                        std::to_string(oldestTimestamp) + ", value=" + value);
                globallyVisibleUpdateFound = true;
            }
        }
    }

    free(sweepKey);

    /*
     * If we get here and the test is still running, it means we must have reached the end of the
     * table. We can also get here because the test is no longer running. In this case, the cursor
     * can either be at the end of the table or still on a valid entry since we interrupted the
     * work.
     */
    if (ret != 0 && ret != WT_NOTFOUND)
        testutil_die(LOG_ERROR,
          "Tracking table sweep failed: cursor->next() returned an unexpected error %d.", ret);

    /* If we have a position, give it up. */
    testutil_check(_sweepCursor->reset(_sweepCursor.get()));
}

void
OperationTracker::saveSchemaOperation(
  const trackingOperation &operation, const uint64_t &collectionId, wt_timestamp_t timestamp)
{
    std::string error_message;

    if (!_enabled)
        return;

    if (operation == trackingOperation::CREATE_COLLECTION ||
      operation == trackingOperation::DELETE_COLLECTION) {
        _schemaTrackingCursor->set_key(_schemaTrackingCursor.get(), collectionId, timestamp);
        _schemaTrackingCursor->set_value(_schemaTrackingCursor.get(), static_cast<int>(operation));
        testutil_check(_schemaTrackingCursor->insert(_schemaTrackingCursor.get()));
    } else {
        error_message =
          "saveSchemaOperation: invalid operation " + std::to_string(static_cast<int>(operation));
        testutil_die(EINVAL, error_message.c_str());
    }
}

int
OperationTracker::save_operation(const uint64_t transactionId, const trackingOperation &operation,
  const uint64_t &collectionId, const std::string &key, const std::string &value,
  wt_timestamp_t timestamp, scoped_cursor &cursor)
{
    WT_DECL_RET;

    if (!_enabled)
        return (0);

    testutil_assert(cursor.get() != nullptr);

    if (operation == trackingOperation::CREATE_COLLECTION ||
      operation == trackingOperation::DELETE_COLLECTION) {
        const std::string error =
          "save_operation: invalid operation " + std::to_string(static_cast<int>(operation));
        testutil_die(EINVAL, error.c_str());
    } else {
        setTrackingCursor(transactionId, operation, collectionId, key, value, timestamp, cursor);
        ret = cursor->insert(cursor.get());
    }
    return (ret);
}

/* Note that the transaction id is not used in the default implementation of the tracking table. */
void
OperationTracker::setTrackingCursor(const uint64_t transactionId,
  const trackingOperation &operation, const uint64_t &collectionId, const std::string &key,
  const std::string &value, wt_timestamp_t timestamp, scoped_cursor &cursor)
{
    cursor->set_key(cursor.get(), collectionId, key.c_str(), timestamp);
    cursor->set_value(cursor.get(), static_cast<int>(operation), value.c_str());
}

} // namespace test_harness
