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

#ifndef TIMESTAMP_MANAGER_H
#define TIMESTAMP_MANAGER_H

#include <atomic>

#include "component.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {
/*
 * The timestamp monitor class manages global timestamp state for all components in the test
 * harness. It also manages the global timestamps within WiredTiger.
 *
 * The format of a timestamp is as follows, the first 32 bits represent the epoch time in seconds.
 * The last 32 bits represent an increment for uniqueness.
 */
class TimestampManager : public Component {
    public:
    static const std::string DecimalToHex(uint64_t value);

    public:
    explicit TimestampManager(Configuration *config);
    virtual ~TimestampManager() = default;

    void Load() override final;
    void DoWork() override final;

    /* Get a unique timestamp. */
    wt_timestamp_t GetNextTimestamp();

    /* Get oldest timestamp. */
    wt_timestamp_t GetOldestTimestamp() const;

    /*
     * Generate a timestamp between the oldest timestamp and the stable timestamp.
     *
     * WiredTiger will abort commit transactions that attempt to commit behind an active read
     * timestamp in order to preserve repeatable reads. Currently the cppsuite doesn't handle that
     * well, so to avoid this issue we will read behind the stable timestamp.
     *
     * This timestamp isn't guaranteed to provide a repeatable read as the oldest could move
     * concurrently removing the previously seen data.
     */
    wt_timestamp_t GetValidReadTimestamp() const;

    private:
    /* Get the current time in seconds, bit shifted to the expected location. */
    uint64_t GetTimeNowSecs() const;

    private:
    std::atomic<wt_timestamp_t> _incrementTimestamp{0};
    /* The tracking table sweep needs to read the oldest timestamp. */
    std::atomic<wt_timestamp_t> _oldestTimestamp{0U};
    wt_timestamp_t _stableTimestamp = 0U;
    /*
     * _oldest_lag is the time window between the stable and oldest timestamps.
     * _stable_lag is the time window between the latest and stable timestamps.
     */
    uint64_t _oldestLag = 0U, _stableLag = 0U;
};
} // namespace test_harness

#endif
