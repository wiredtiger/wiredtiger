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

#include "timestamp_manager.h"

#include <sstream>

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
const std::string
TimestampManager::DecimalToHex(uint64_t value)
{
    std::stringstream ss;
    ss << std::hex << value;
    std::string res(ss.str());
    return (res);
}

TimestampManager::TimestampManager(Configuration *config) : Component("timestamp_manager", config)
{
}

void
TimestampManager::Load()
{
    Component::Load();
    int64_t currentOldestLag = _config->GetInt(oldestLag);
    testutil_assert(currentOldestLag >= 0);
    /* Cast and then shift left to match the seconds position. */
    _oldestLag = currentOldestLag;
    _oldestLag <<= 32;

    int64_t currentStableLag = _config->GetInt(stableLag);
    testutil_assert(currentStableLag >= 0);
    /* Cast and then shift left to match the seconds position. */
    _stableLag = currentStableLag;
    _stableLag <<= 32;
}

void
TimestampManager::DoWork()
{
    std::string config, LogMessage;
    /* latestTimestampSecs represents the time component of the latest timestamp provided. */
    wt_timestamp_t latestTimestampSecs = GetTimeNowSecs();

    /*
     * Keep a time window between the latest and stable ts less than the max defined in the
     * configuration.
     */
    wt_timestamp_t newStableTimestamp = _stableTimestamp;
    testutil_assert(latestTimestampSecs >= _stableTimestamp);
    if ((latestTimestampSecs - _stableTimestamp) > _stableLag) {
        LogMessage = "Timestamp_manager: Stable timestamp expired.";
        newStableTimestamp = latestTimestampSecs - _stableLag;
        config += stableTimestamp + "=" + DecimalToHex(newStableTimestamp);
    }

    /*
     * Keep a time window between the stable and oldest ts less than the max defined in the
     * configuration.
     */
    wt_timestamp_t newOldestTimestamp = _oldestTimestamp;
    testutil_assert(_stableTimestamp >= _oldestTimestamp);
    if ((newStableTimestamp - _oldestTimestamp) > _oldestLag) {
        if (LogMessage.empty())
            LogMessage = "Timestamp_manager: Oldest timestamp expired.";
        else
            LogMessage += " Oldest timestamp expired.";
        newOldestTimestamp = newStableTimestamp - _oldestLag;
        if (!config.empty())
            config += ",";
        config += oldestTimestamp + "=" + DecimalToHex(newOldestTimestamp);
    }

    if (!LogMessage.empty())
        Logger::LogMessage(LOG_TRACE, LogMessage);

    /*
     * Save the new timestamps. Any timestamps that we're viewing from another thread should be set
     * AFTER we've saved the new timestamps to avoid races where we sweep data that is not yet
     * obsolete.
     */
    if (!config.empty()) {
        ConnectionManager::GetInstance().SetTimestamp(config);
        _oldestTimestamp = newOldestTimestamp;
        _stableTimestamp = newStableTimestamp;
    }
}

wt_timestamp_t
TimestampManager::GetNextTimestamp()
{
    uint64_t currentTime = GetTimeNowSecs();
    uint64_t increment = _incrementTimestamp.fetch_add(1);
    currentTime |= (increment & 0x00000000FFFFFFFF);
    return (currentTime);
}

wt_timestamp_t
TimestampManager::GetOldestTimestamp() const
{
    return (_oldestTimestamp);
}

wt_timestamp_t
TimestampManager::GetValidReadTimestamp() const
{
    /* Use GetOldestTimestamp here to convert from atomic to wt_timestamp_t. */
    wt_timestamp_t currentOldestTimestamp = GetOldestTimestamp();
    wt_timestamp_t currentStableTimestamp = _stableTimestamp;
    if (currentStableTimestamp > currentOldestTimestamp) {
        --currentStableTimestamp;
    }
    /*
     * Assert that our stable and oldest match if 0 or that the stable is greater than or equal to
     * the oldest. Ensuring that the oldest is never greater than the stable.
     */
    testutil_assert((currentStableTimestamp == 0 && currentOldestTimestamp == 0) ||
      currentStableTimestamp >= currentOldestTimestamp);
    /*
     * Its okay to return a timestamp less than a concurrently updated oldest timestamp as all
     * readers should be reading with timestamp rounding.
     */
    return RandomGenerator::GetInstance().GenerateInteger<wt_timestamp_t>(
      currentOldestTimestamp, currentStableTimestamp);
}

uint64_t
TimestampManager::GetTimeNowSecs() const
{
    auto now = std::chrono::system_clock::now().time_since_epoch();
    uint64_t currentTimeSecs =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count()) << 32;
    return (currentTimeSecs);
}
} // namespace test_harness
