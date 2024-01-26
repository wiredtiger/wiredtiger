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

#include <algorithm>
#include "model/driver/kv_workload_generator.h"
#include "model/util.h"

namespace model {

/*
 * kv_workload_generator_spec::kv_workload_generator_spec --
 *     Create the generator specifcation using default probability values.
 */
kv_workload_generator_spec::kv_workload_generator_spec()
{
    prepared_transaction = 0.25;
    allow_set_commit_timestamp = 0.25;
    nonprepared_transaction_rollback = 0.1;
    prepared_transaction_rollback_after_prepare = 0.1;
    prepared_transaction_rollback_before_prepare = 0.1;

    insert = 0.75;
    finish_transaction = 0.08;
    remove = 0.15;
    set_commit_timestamp = 0.05;
    truncate = 0.005;

    checkpoint = 0.02;
    restart = 0.001;
    set_stable_timestamp = 0.2;

    max_concurrent_transactions = 3;
}

/*
 * kv_workload_generator::kv_workload_generator --
 *     Create a new workload generator.
 */
kv_workload_generator::kv_workload_generator(kv_workload_generator_spec spec, uint64_t seed)
    : _workload_ptr(std::make_shared<kv_workload>()), _workload(*(_workload_ptr.get())),
      _last_table_id(0), _last_txn_id(0), _random(seed), _spec(spec)
{
}

/*
 * kv_workload_transaction_ptr --
 *     Generate a random transaction.
 */
kv_workload_transaction_ptr
kv_workload_generator::generate_transaction()
{
    /* Choose the transaction ID and whether this will be a prepared transaction. */
    txn_id_t txn_id = ++_last_txn_id;
    bool prepared = _random.next_float() < _spec.prepared_transaction;

    /* Start the new transaction. */
    kv_workload_transaction_ptr txn_ptr =
      std::make_shared<kv_workload_transaction>(txn_id, prepared);
    kv_workload_transaction &txn = *txn_ptr.get();
    txn << operation::begin_transaction(txn_id);

    bool use_set_commit_timestamp =
      !prepared && _random.next_float() < _spec.allow_set_commit_timestamp;
    if (use_set_commit_timestamp)
        txn << operation::set_commit_timestamp(txn_id, k_timestamp_none /* placeholder */);

    /* Add all operations. But do not actually fill in timestamps; we'll do that later. */
    bool done = false;
    while (!done) {
        float total = _spec.insert + _spec.finish_transaction + _spec.remove +
          _spec.set_commit_timestamp + _spec.truncate;
        probability_switch(_random.next_float() * total)
        {
            probability_case(_spec.insert)
            {
                table_context_ptr table = choose_table(txn_ptr);
                data_value key = generate_key(table);
                data_value value = generate_value(table);
                txn << operation::insert(table->id(), txn_id, key, value);
            }
            probability_case(_spec.finish_transaction)
            {
                if (prepared) {
                    if (_random.next_float() < _spec.prepared_transaction_rollback_before_prepare)
                        txn << operation::rollback_transaction(txn_id);
                    else {
                        txn << operation::prepare_transaction(
                          txn_id, k_timestamp_none /* placeholder */);
                        if (_random.next_float() <
                          _spec.prepared_transaction_rollback_after_prepare)
                            txn << operation::rollback_transaction(txn_id);
                        else
                            txn << operation::commit_transaction(txn_id);
                    }
                } else
                    txn << operation::commit_transaction(txn_id);
                done = true;
            }
            probability_case(_spec.remove)
            {
                table_context_ptr table = choose_table(txn_ptr);
                data_value key = generate_key(table);
                txn << operation::remove(table->id(), txn_id, key);
            }
            probability_case(_spec.set_commit_timestamp)
            {
                if (use_set_commit_timestamp)
                    txn << operation::set_commit_timestamp(
                      txn_id, k_timestamp_none /* placeholder */);
            }
            probability_case(_spec.truncate)
            {
                table_context_ptr table = choose_table(txn_ptr);
                data_value start = generate_key(table);
                data_value stop = generate_key(table);
                if (start > stop)
                    std::swap(start, stop);
                txn << operation::truncate(table->id(), txn_id, start, stop);
            }
        }
    }

    return txn_ptr;
}

/*
 * kv_workload_generator::fill_in_timestamps --
 *     Fill in the timestamps.
 */
void
kv_workload_generator::fill_in_timestamps(
  kv_workload_sequence &sequence, timestamp_t first, timestamp_t last)
{
    if (first + 10 >= last)
        throw model_exception("Need a bigger difference between first and last timestamp");

    /* Special operation sequences. */
    if (!sequence.transaction()) {
        for (operation::any &op : sequence.operations()) {
            if (std::holds_alternative<operation::set_stable_timestamp>(op)) {
                timestamp_t t = first + _random.next_uint64(last - first) - 1000 /*XXX*/;
                std::get<operation::set_stable_timestamp>(op).stable_timestamp = t;
                /* The "set stable timestamp" sequence has only one operation, so we're done. */
                break;
            }
        }
        return;
    }

    /* Transactions. */

    /* Count the number of explicit timestamp sets. Find prepare and commit operations. */
    size_t num_set_commit_timestamp = 0;
    operation::prepare_transaction *prepare = nullptr;
    operation::commit_transaction *commit = nullptr;
    for (operation::any &op : sequence.operations()) {
        if (std::holds_alternative<operation::set_commit_timestamp>(op))
            num_set_commit_timestamp++;
        if (std::holds_alternative<operation::prepare_transaction>(op))
            prepare = &std::get<operation::prepare_transaction>(op);
        if (std::holds_alternative<operation::commit_transaction>(op))
            commit = &std::get<operation::commit_transaction>(op);
    }

    /* Non-prepared transactions. */
    if (prepare == nullptr) {

        /*
         * Use floating point arithmetic in the unlikely case we'll need too many timestamps to
         * avoid degenerate cases.
         */
        size_t timestamps_needed = num_set_commit_timestamp + 1;
        double step = (last - first) / (double)timestamps_needed;

        /* Pick timestamps for the "set commit timestamp" operations. */
        double d = first;
        size_t count = 1;
        for (operation::any &op : sequence.operations())
            if (std::holds_alternative<operation::set_commit_timestamp>(op)) {
                double l = first + count * step;
                d = d + _random.next_double() * (l - d);
                count++;
                std::get<operation::set_commit_timestamp>(op).commit_timestamp = (timestamp_t)d;
            }

        /*
         * Counterintuitively, the commit timestamp applies to operations that come before the
         * first set commit timestamp operation, but it cannot be less than the first timestamp.
         */
        timestamp_t commit_timestamp = (timestamp_t)(d + _random.next_double() * (last - d));
        if (commit != nullptr)
            commit->commit_timestamp = commit_timestamp;
    }

    /* Prepared transactions. */
    else {
        timestamp_t prepare_timestamp = first + _random.next_uint64(last - first - 10);
        timestamp_t commit_timestamp =
          prepare_timestamp + _random.next_uint64(last - prepare_timestamp - 5);
        timestamp_t durable_timestamp =
          commit_timestamp + _random.next_uint64(last - commit_timestamp);

        prepare->prepare_timestamp = prepare_timestamp;
        if (commit != nullptr) {
            commit->commit_timestamp = commit_timestamp;
            commit->durable_timestamp = durable_timestamp;
        }
    }
}

/*
 * kv_workload_generator::generate --
 *     Generate the workload.
 */
void
kv_workload_generator::generate()
{
    /* Create tables. */
    /* TODO: Should not be hard-coded. */
    size_t num_tables = 2 + (size_t)_random.next_uint64(10);
    for (size_t i = 0; i < num_tables; i++)
        create_table();

    /* Generate a serialized workload. We'll fill in timestamps and shuffle it later. */
    /* TODO: Should not be hard-coded. */
    size_t length = 1000 + (size_t)_random.next_uint64(10);
    for (size_t i = 0; i < length; i++)
        probability_switch(_random.next_float())
        {
            probability_case(_spec.checkpoint)
            {
                kv_workload_sequence_ptr p = std::make_shared<kv_workload_sequence>();
                *p << operation::checkpoint();
                _sequences.push_back(p);
            }
            probability_case(_spec.set_stable_timestamp)
            {
                kv_workload_sequence_ptr p = std::make_shared<kv_workload_sequence>();
                *p << operation::set_stable_timestamp(k_timestamp_none); /* Placeholder. */
                _sequences.push_back(p);
            }
            probability_case(_spec.restart)
            {
                kv_workload_sequence_ptr p = std::make_shared<kv_workload_sequence>();
                *p << operation::restart();
                _sequences.push_back(p);
            }
            probability_default
            {
                _sequences.push_back(generate_transaction());
            }
        }

    /* Remember the positions in the list; we'll need it to enforce partial ordering later. */
    for (size_t i = 0; i < _sequences.size(); i++)
        _sequences[i]->_index = i;

    /* Position special sequences that are not transactions. */
    size_t last_special = 0;
    for (size_t i = 0; i < _sequences.size(); i++) {
        if (_sequences[i]->transaction())
            continue;

        /* Position after all sequences since the last special sequence. */
        for (size_t j = last_special; j < i; j++)
            _sequences[j]->must_finish_before_starting(*_sequences[i].get());

        last_special = i;
    }

    /*
     * Find dependencies between the workload subsequences: If two sequences operate on the same
     * keys, they must be run sequentially. If there is any overlap, the transaction in the
     * second sequence will abort.
     */
    for (size_t i = 0; i < _sequences.size(); i++)
        for (size_t j = i + 1; j < _sequences.size(); j++)
            if (_sequences[i]->overlaps_with(*_sequences[j].get()))
                _sequences[i]->must_finish_before_starting(*_sequences[j].get());

    /* Fill in the timestamps based on the partial order. */
    std::deque<kv_workload_sequence *> runnable;
    std::deque<kv_workload_sequence *> next;

    size_t next_barrier = 0;
    for (kv_workload_sequence_ptr &seq : _sequences)
        if (!seq->transaction()) {
            next_barrier = seq->_index;
            break;
        }

    for (kv_workload_sequence_ptr &seq : _sequences) {
        seq->prepare_to_run();
        if (seq->_unsatisfied_dependencies == 0) {
            if (seq->_index <= next_barrier)
                runnable.push_back(seq.get());
            else
                next.push_back(seq.get());
        }
    }

    timestamp_t step = 1000;
    timestamp_t first = step + 1;
    timestamp_t last = first + step;
    while (!runnable.empty() || !next.empty()) {

        /*
         * Assign timestamps in a way that satisfies the partial order until the next timestamp
         * barrier.
         */
        while (!runnable.empty()) {
            for (kv_workload_sequence *seq : runnable)
                fill_in_timestamps(*seq, first, last);

            std::deque<kv_workload_sequence *> next_runnable;
            for (kv_workload_sequence *seq : runnable)
                for (kv_workload_sequence *n : seq->runnable_after_finish())
                    if (n->_unsatisfied_dependencies.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                        if (n->_index <= next_barrier)
                            next_runnable.push_back(n);
                        else
                            next.push_back(n);
                    }

            runnable = next_runnable;
            first = last + 1;
            last = first + step - 1;
        }

        /*
         * By now, we have assigned timestamps to everything up until (and including) the operation
         * sequence at the "next barrier" index. Find the next barrier and get ready for the next
         * operation.
         */
        bool found = false;
        for (size_t i = next_barrier + 1; i < _sequences.size(); i++)
            if (!_sequences[i]->transaction()) {
                found = true;
                next_barrier = i;
                break;
            }
        if (!found)
            next_barrier = _sequences.size();

        std::deque<kv_workload_sequence *> next_next;
        for (kv_workload_sequence *n : next)
            if (n->_index <= next_barrier)
                runnable.push_back(n);
            else
                next_next.push_back(n);
        next = next_next;
    }

    /* Create an execution schedule, mixing operations from different transactions. */

    runnable.clear();
    for (kv_workload_sequence_ptr &seq : _sequences) {
        seq->prepare_to_run();
        if (seq->_unsatisfied_dependencies == 0)
            runnable.push_back(seq.get());
    }

    timestamp_t stable = k_timestamp_none;
    while (!runnable.empty()) {

        /* Take the next operation from one of the runnable sequences. */
        size_t next_sequence_index =
          _random.next_index(std::min(runnable.size(), _spec.max_concurrent_transactions));
        kv_workload_sequence *next_sequence = runnable[next_sequence_index];

        /* Assert that there is at least one operation left. */
        const auto &next_sequence_operations = next_sequence->operations();
        if (next_sequence->_next_operation_index >= next_sequence_operations.size())
            throw model_exception("Internal error: No more operations left in a sequence");

        /* Get the next operation. */
        const operation::any &op = next_sequence_operations[next_sequence->_next_operation_index++];
        _workload << op;

        /* Verify timestamps. */
        if (std::holds_alternative<operation::set_stable_timestamp>(op)) {
            timestamp_t t = std::get<operation::set_stable_timestamp>(op).stable_timestamp;
            if (t < stable)
                std::cerr << "Warning: Stable timestamp went backwards: " << stable << " -> " << t
                          << std::endl;
            stable = t;
        }
        if (std::holds_alternative<operation::prepare_transaction>(op)) {
            timestamp_t t = std::get<operation::prepare_transaction>(op).prepare_timestamp;
            if (t < stable)
                std::cerr << "Warning: Prepare timestamp is before the stable timestamp: " << t
                          << " < " << stable << " (sequence " << next_sequence->_index << ")"
                          << std::endl;
        }
        if (std::holds_alternative<operation::set_commit_timestamp>(op)) {
            timestamp_t t = std::get<operation::set_commit_timestamp>(op).commit_timestamp;
            if (t < stable)
                std::cerr << "Warning: Commit timestamp is before the stable timestamp: " << t
                          << " < " << stable << std::endl;
        }
        if (std::holds_alternative<operation::commit_transaction>(op)) {
            timestamp_t t = std::get<operation::commit_transaction>(op).commit_timestamp;
            if (t < stable)
                std::cerr << "Warning: Commit timestamp is before the stable timestamp: " << t
                          << " < " << stable << std::endl;
            t = std::get<operation::commit_transaction>(op).durable_timestamp;
            if (t < stable && t != k_timestamp_none)
                std::cerr << "Warning: Durable timestamp is before the stable timestamp: " << t
                          << " < " << stable << std::endl;
        }

        /* If the operation resulted in a database restart, skip the rest of started operations. */
        if (std::holds_alternative<operation::restart>(op)) {
            std::deque<kv_workload_sequence *> next_runnable;
            for (kv_workload_sequence *r : runnable) {
                if (r->_next_operation_index > 0) {
                    for (kv_workload_sequence *n : r->runnable_after_finish())
                        if (n->_unsatisfied_dependencies.fetch_sub(1, std::memory_order_seq_cst) ==
                          1) {
                            if (n->transaction())
                                next_runnable.push_back(n);
                            else
                                /*
                                 * We need to do this to keep non-transaction sequences at roughly
                                 * the expected positions.
                                 */
                                next_runnable.push_front(n);
                        }
                } else {
                    next_runnable.push_back(r);
                }
            }
            runnable = next_runnable;
            continue;
        }

        /* If this is the last operation, complete the sequence execution. */
        if (next_sequence->_next_operation_index >= next_sequence_operations.size()) {
            next_sequence->_done = true;
            runnable.erase(runnable.begin() + next_sequence_index);
            for (kv_workload_sequence *n : next_sequence->runnable_after_finish())
                if (n->_unsatisfied_dependencies.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                    if (n->transaction())
                        runnable.push_back(n);
                    else
                        /*
                         * We need to do this to keep non-transaction sequences at roughly the
                         * expected positions.
                         */
                        runnable.push_front(n);
                }
        }
    }
}

/*
 * kv_workload_generator::choose_table --
 *     Choose a table for an operation, creating one if necessary.
 */
kv_workload_generator::table_context_ptr
kv_workload_generator::choose_table(kv_workload_transaction_ptr txn)
{
    /* TODO: In the future, transaction context will specify its own table distribution. */
    (void)txn;

    if (_tables.empty())
        throw model_exception("No tables.");

    return _tables_list[_random.next_index(_tables_list.size())];
}

/*
 * kv_workload_generator::create_table --
 *     Create a table.
 */
void
kv_workload_generator::create_table()
{
    table_id_t id = ++_last_table_id;
    std::string name = "table" + std::to_string(id);
    std::string key_format = "Q";
    std::string value_format = "Q";

    table_context_ptr table = std::make_shared<table_context>(id, name, key_format, value_format);
    _tables_list.push_back(table);
    _tables[id] = table;

    _workload << operation::create_table(table->id(), table->name().c_str(),
      table->key_format().c_str(), table->value_format().c_str());
}

/*
 * kv_workload_generator::random_data_value --
 *     Generate a random data value, which can be used either as a key or a value.
 */
data_value
kv_workload_generator::random_data_value(const std::string &format)
{
    if (format.length() != 1)
        throw model_exception("The model does not currently support structs or types with sizes");

    switch (format[0]) {
    case 'Q':
        /* TODO: Should not be hard coded. */
        return data_value(_random.next_uint64(1000000));
    default:
        throw model_exception("Unsupported type.");
    };
}

} /* namespace model */
