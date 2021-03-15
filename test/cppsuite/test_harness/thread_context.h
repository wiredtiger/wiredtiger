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

#ifndef THREAD_CONTEXT_H
#define THREAD_CONTEXT_H

#include "random_generator.h"

namespace test_harness {
/* Define the different thread operations. */
enum class thread_operation {
    INSERT,
    UPDATE,
    READ,
    REMOVE,
    CHECKPOINT,
    TIMESTAMP,
    MONITOR,
    COMPONENT
};

/* Container class for a thread and any data types it may need to interact with the database. */
class thread_context {
    public:
    thread_context(std::vector<std::string> collection_names, thread_operation type, int64_t max_op,
      int64_t min_op)
        : _collection_names(collection_names), _current_op_count(0U), _in_txn(false),
          _max_op(max_op), _max_op_count(0), _min_op(min_op), _running(false), _type(type)
    {
    }

    void
    finish()
    {
        _running = false;
    }

    const std::vector<std::string> &
    get_collection_names() const
    {
        return _collection_names;
    }

    thread_operation
    get_thread_operation() const
    {
        return _type;
    }

    bool
    is_running() const
    {
        return _running;
    }

    void
    set_running(bool running)
    {
        _running = running;
    }

    void
    begin_transaction(WT_SESSION *session, const std::string &config)
    {
        if (!_in_txn) {
            testutil_check(
              session->begin_transaction(session, config.empty() ? NULL : config.c_str()));
            _max_op_count = random_generator::instance().generate_number(_min_op, _max_op);
            _current_op_count = 0;
            _in_txn = true;
        }
    }

    /* Returns true if the current transaction has been committed. */
    bool
    commit_transaction(WT_SESSION *session, const std::string &config)
    {
        /* A transaction cannot be committed if not started. */
        testutil_assert(_in_txn);
        if (!_running || (++_current_op_count > _max_op_count)) {
            testutil_check(
              session->commit_transaction(session, config.empty() ? NULL : config.c_str()));
            _in_txn = false;
        }

        return !_in_txn;
    }

    private:
    const std::vector<std::string> _collection_names;
    uint64_t _current_op_count;
    bool _in_txn, _running;
    int64_t _min_op, _max_op, _max_op_count;
    const thread_operation _type;
};
} // namespace test_harness

#endif
