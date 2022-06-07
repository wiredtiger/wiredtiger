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

#include "transaction.h"

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"

namespace test_harness {

Transaction::Transaction(
  Configuration *config, TimestampManager *timestampManager, WT_SESSION *session)
    : _timestampManager(timestampManager), _session(session)
{
    /* Use optional here as our populate threads don't define this configuration. */
    Configuration *transactionConfig = config->GetOptionalSubconfig(kOpsPerTransaction);
    if (transactionConfig != nullptr) {
        _minOpCount = transactionConfig->GetOptionalInt(kMin, 1);
        _maxOpCount = transactionConfig->GetOptionalInt(kMax, 1);
        delete transactionConfig;
    }
}

bool
Transaction::Running() const
{
    return (_running);
}

void
Transaction::IncrementOpCounter()
{
    _opCount++;
}

void
Transaction::Begin(const std::string &config)
{
    testutil_assert(!_running);
    testutil_check(
      _session->begin_transaction(_session, config.empty() ? nullptr : config.c_str()));
    /* This randomizes the number of operations to be executed in one transaction. */
    _targetOpCount =
      RandomGenerator::GetInstance().GenerateInteger<int64_t>(_minOpCount, _maxOpCount);
    _opCount = 0;
    _running = true;
    _rollbackRequired = false;
}

void
Transaction::TryBegin(const std::string &config)
{
    if (!_running)
        Begin(config);
}

/*
 * It's possible to receive rollback in commit, when this happens the API will rollback the
 * transaction internally.
 */
bool
Transaction::Commit(const std::string &config)
{
    WT_DECL_RET;
    testutil_assert(_running && !_rollbackRequired);

    ret = _session->commit_transaction(_session, config.empty() ? nullptr : config.c_str());
    /*
     * FIXME-WT-9198 Now we are accepting the error code EINVAL because of possible invalid
     * timestamps as we know it can happen due to the nature of the framework. The framework may set
     * the stable/oldest timestamps to a more recent date than the commit timestamp of the
     * transaction which makes the transaction invalid. We only need to check against the stable
     * timestamp as, by definition, the oldest timestamp is older than the stable one.
     */
    testutil_assert(ret == 0 || ret == EINVAL || ret == WT_ROLLBACK);

    if (ret != 0)
        Logger::LogMessage(LOG_WARN,
          "Failed to commit transaction in commit, received error code: " + std::to_string(ret));
    _opCount = 0;
    _running = false;
    return (ret == 0);
}

void
Transaction::Rollback(const std::string &config)
{
    testutil_assert(_running);
    testutil_check(
      _session->rollback_transaction(_session, config.empty() ? nullptr : config.c_str()));
    _rollbackRequired = false;
    _opCount = 0;
    _running = false;
}

void
Transaction::TryRollback(const std::string &config)
{
    if (CanRollback())
        Rollback(config);
}

/*
 * FIXME: WT-9198 We're concurrently doing a transaction that contains a bunch of operations while
 * moving the stable timestamp. Eat the occasional EINVAL from the transaction's first commit
 * timestamp being earlier than the stable timestamp.
 */
int
Transaction::SetCommitTimestamp(wt_timestamp_t timestamp)
{
    /* We don't want to set zero timestamps on transactions if we're not using timestamps. */
    if (!_timestampManager->IsEnabled())
        return 0;
    const std::string config = kCommitTimestamp + "=" + TimestampManager::DecimalToHex(timestamp);
    return _session->timestamp_transaction(_session, config.c_str());
}

void
Transaction::SetRollbackRequired(bool rollback)
{
    _rollbackRequired = rollback;
}

bool
Transaction::CanCommit()
{
    return (!_rollbackRequired && CanRollback());
}

bool
Transaction::CanRollback()
{
    return (_running && _opCount >= _targetOpCount);
}
} // namespace test_harness
