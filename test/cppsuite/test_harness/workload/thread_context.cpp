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

#include "../core/configuration.h"
#include "../timestamp_manager.h"
#include "../util/api_const.h"
#include "../util/logger.h"
#include "workload_tracking.h"
#include "random_generator.h"
#include "thread_context.h"

namespace test_harness {

const std::string
type_string(thread_type type)
{
    switch (type) {
    case thread_type::CUSTOM:
        return ("custom");
    case thread_type::CHECKPOINT:
        return ("checkpoint");
    case thread_type::INSERT:
        return ("insert");
    case thread_type::READ:
        return ("read");
    case thread_type::REMOVE:
        return ("remove");
    case thread_type::UPDATE:
        return ("update");
    default:
        testutil_die(EINVAL, "unexpected thread_type: %d", static_cast<int>(type));
    }
}

/* thread_context class implementation */
thread_context::thread_context(uint64_t id, thread_type type, configuration *config,
  scoped_session &&created_session, timestamp_manager *timestamp_manager,
  workload_tracking *tracking, database &dbase)
    : /* These won't exist for certain threads which is why we use optional here. */
      collection_count(config->get_optional_int(COLLECTION_COUNT, 1)),
      key_count(config->get_optional_int(KEY_COUNT_PER_COLLECTION, 1)),
      key_size(config->get_optional_int(KEY_SIZE, 1)),
      value_size(config->get_optional_int(VALUE_SIZE, 1)),
      thread_count(config->get_int(THREAD_COUNT)), type(type), id(id), db(dbase),
      session(std::move(created_session)), tsm(timestamp_manager),
      transaction(transaction_context(config, timestamp_manager, session.get())),
      tracking(tracking), _sleeping_time_ms(config->get_throttle())

{
    if (tracking->enabled())
        op_track_cursor = session.open_scoped_cursor(tracking->get_operation_table_name());
    testutil_assert(key_size > 0 && value_size > 0);
}

void
thread_context::finish()
{
    _running = false;
}

std::string
thread_context::pad_string(const std::string &value, uint64_t size)
{
    uint64_t diff = size > value.size() ? size - value.size() : 0;
    std::string s(diff, '0');
    return (s.append(value));
}

bool
thread_context::update(
  scoped_cursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(tracking != nullptr);
    testutil_assert(cursor.get() != nullptr);

    wt_timestamp_t ts = tsm->get_next_ts();
    transaction.set_commit_timestamp(ts);

    cursor->set_key(cursor.get(), key.c_str());
    cursor->set_value(cursor.get(), value.c_str());
    ret = cursor->update(cursor.get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.set_needs_rollback(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to update a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.get())->txn->id;
    ret = tracking->save_operation(
      txn_id, tracking_operation::INSERT, collection_id, key, value, ts, op_track_cursor);

    if (ret == 0)
        transaction.add_op();
    else if (ret == WT_ROLLBACK)
        transaction.set_needs_rollback(true);
    else
        testutil_die(ret, "unhandled error while trying to save an update to the tracking table");
    return (ret == 0);
}

bool
thread_context::insert(
  scoped_cursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(tracking != nullptr);
    testutil_assert(cursor.get() != nullptr);

    wt_timestamp_t ts = tsm->get_next_ts();
    transaction.set_commit_timestamp(ts);

    cursor->set_key(cursor.get(), key.c_str());
    cursor->set_value(cursor.get(), value.c_str());
    ret = cursor->insert(cursor.get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.set_needs_rollback(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to insert a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.get())->txn->id;
    ret = tracking->save_operation(
      txn_id, tracking_operation::INSERT, collection_id, key, value, ts, op_track_cursor);

    if (ret == 0)
        transaction.add_op();
    else if (ret == WT_ROLLBACK)
        transaction.set_needs_rollback(true);
    else
        testutil_die(ret, "unhandled error while trying to save an insert to the tracking table");
    return (ret == 0);
}

bool
thread_context::remove(scoped_cursor &cursor, uint64_t collection_id, const std::string &key)
{
    WT_DECL_RET;
    testutil_assert(tracking != nullptr);
    testutil_assert(cursor.get() != nullptr);

    wt_timestamp_t ts = tsm->get_next_ts();
    transaction.set_commit_timestamp(ts);

    cursor->set_key(cursor.get(), key.c_str());
    ret = cursor->remove(cursor.get());
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.set_needs_rollback(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to remove a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.get())->txn->id;
    ret = tracking->save_operation(
      txn_id, tracking_operation::DELETE_KEY, collection_id, key, "", ts, op_track_cursor);

    if (ret == 0)
        transaction.add_op();
    else if (ret == WT_ROLLBACK)
        transaction.set_needs_rollback(true);
    else
        testutil_die(ret, "unhandled error while trying to save a remove to the tracking table");
    return (ret == 0);
}

void
thread_context::sleep()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(_sleeping_time_ms));
}

bool
thread_context::running() const
{
    return (_running);
}
} // namespace test_harness
