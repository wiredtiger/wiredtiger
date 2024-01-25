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

#ifndef MODEL_DRIVER_KV_WORKLOAD_SEQUENCE_H
#define MODEL_DRIVER_KV_WORKLOAD_SEQUENCE_H

#include <atomic>
#include <deque>
#include <memory>
#include "model/driver/kv_workload.h"
#include "model/core.h"
#include "model/data_value.h"

namespace model {

/*
 * kv_workload_sequence --
 *     A sequence of operations in a workload.
 */
class kv_workload_sequence {

public:
    /*
     * kv_workload_sequence::kv_workload_sequence --
     *     Create a new sequence of operations.
     */
    inline kv_workload_sequence() {}

    /*
     * kv_workload_sequence::transaction --
     *     Check if this sequence represents a transaction.
     */
    virtual bool
    transaction() const noexcept
    {
        return false;
    }

    /*
     * kv_workload_sequence::operator<< --
     *     Add an operation to the sequence.
     */
    inline kv_workload_sequence &
    operator<<(operation::any &&op)
    {
        _operations.push_back(std::move(op));
        return *this;
    }

    /*
     * kv_workload_sequence::overlaps_with --
     *     Check whether this sequence overlaps in any key ranges with the other sequence.
     */
    bool overlaps_with(const kv_workload_sequence &other) const;

    /*
     * kv_workload_sequence::must_start_before_starting --
     *     Declare that the other sequence cannot start until this sequence starts.
     */
    void must_start_before_starting(kv_workload_sequence &other);

    /*
     * kv_workload_sequence::must_finish_before_starting --
     *     Declare that the other sequence cannot start until this sequence finishes.
     */
    void must_finish_before_starting(kv_workload_sequence &other);

    /*
     * kv_workload_sequence::operations --
     *     Get the list of operations. Note that the lifetime of this reference is constrained to
     *     the lifetime of this object.
     */
    inline std::deque<operation::any> &
    operations() noexcept
    {
        return _operations;
    }

    /*
     * kv_workload_sequence::operations --
     *     Get the list of operations. Note that the lifetime of this reference is constrained to
     *     the lifetime of this object.
     */
    inline const std::deque<operation::any> &
    operations() const noexcept
    {
        return _operations;
    }

    /*
     * kv_workload_sequence::runnable_after_finish --
     *     Get the list of sequences that are unblocked after this sequence completes. Note that the
     *     lifetime of this reference is constrained to the lifetime of this object.
     */
    inline const std::deque<kv_workload_sequence *>
    runnable_after_finish() const noexcept
    {
        return _runnable_after_finish;
    }

    // XXX
    /* The number of unsatisfied dependencies before this sequence can run. */
    std::atomic<size_t> _unsatisfied_dependencies;

    bool _done;
    size_t _index;

    inline void
    prepare_to_run()
    {
        _done = false;
        _index = 0;
        _unsatisfied_dependencies = _dependencies_start.size() + _dependencies_finish.size();
    }

protected:
    /*
     * kv_workload_sequence::contains_key --
     *     Check whether the sequence contains an operation that touches any key in the range.
     */
    bool contains_key(table_id_t table_id, const data_value &start, const data_value &stop) const;

protected:
    std::deque<operation::any> _operations;

    /* Sequences that must start before this sequence can start, and the inverse. */
    std::deque<kv_workload_sequence *> _dependencies_start;
    std::deque<kv_workload_sequence *> _runnable_after_start;

    /* Sequences that must finish before this sequence can start, and the inverse. */
    std::deque<kv_workload_sequence *> _dependencies_finish;
    std::deque<kv_workload_sequence *> _runnable_after_finish;
};

/*
 * kv_workload_sequence_ptr --
 *     Pointer to a sequence.
 */
using kv_workload_sequence_ptr = std::shared_ptr<kv_workload_sequence>;

/*
 * kv_workload_transaction --
 *     A single workload transaction.
 */
class kv_workload_transaction : public kv_workload_sequence {

public:
    /*
     * kv_workload_transaction::kv_workload_transaction --
     *     Create a new sequence of operations.
     */
    inline kv_workload_transaction(txn_id_t id, bool prepared) : _id(id), _prepared(prepared) {}

    /*
     * kv_workload_sequence::transaction --
     *     Check if this sequence represents a transaction.
     */
    virtual bool
    transaction() const noexcept
    {
        return true;
    }

    /*
     * kv_workload_transaction::id --
     *     Get the transaction ID.
     */
    inline txn_id_t
    id() const noexcept
    {
        return _id;
    }

    /*
     * kv_workload_transaction::prepared --
     *     Check if this is a prepared transaction.
     */
    inline bool
    prepared() const noexcept
    {
        return _prepared;
    }

protected:
    txn_id_t _id;
    bool _prepared;
};

/*
 * kv_workload_sequence_ptr --
 *     Pointer to a transaction.
 */
using kv_workload_transaction_ptr = std::shared_ptr<kv_workload_transaction>;

} /* namespace model */
#endif
