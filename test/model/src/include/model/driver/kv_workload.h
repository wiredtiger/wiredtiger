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

#include <deque>
#include <iostream>
#include <variant>
#include "model/core.h"
#include "model/data_value.h"
#include "model/kv_database.h"

namespace model {

using table_id_t = int;

/*
 * op --
 *     The namespace for all workload operations.
 */
namespace operation {

/*
 * with_txn_id --
 *     Annotates transactional operations.
 */
struct with_txn_id {
    txn_id_t txn_id;

    /*
     * with_txn_id::with_txn_id --
     *     Create the operation.
     */
    inline with_txn_id(txn_id_t txn_id) : txn_id(txn_id) {}

    /*
     * with_txn_id::transactional --
     *     Return whether the operation is transactional.
     */
    inline bool
    transactional() const
    {
        return true;
    }

    /*
     * with_txn_id::transaction_id --
     *     Get the transaction ID.
     */
    inline txn_id_t
    transaction_id() const
    {
        return txn_id;
    }
};

/*
 * without_txn_id --
 *     Annotates non-transactional operations.
 */
struct without_txn_id {

    /*
     * without_txn_id::transactional --
     *     Return whether the operation is transactional.
     */
    inline bool
    transactional() const
    {
        return false;
    }

    /*
     * without_txn_id::transaction_id --
     *     Placeholder for getting a transaction ID.
     */
    inline txn_id_t
    transaction_id() const
    {
        throw model_exception("Not a transactional operation");
    }
};

/*
 * begin_transaction --
 *     A representation of this workload operation.
 */
struct begin_transaction : public with_txn_id {

    /*
     * begin_transaction::begin_transaction --
     *     Create the operation.
     */
    inline begin_transaction(txn_id_t txn_id) : with_txn_id(txn_id) {}

    /*
     * begin_transaction::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const begin_transaction &other) const noexcept
    {
        return txn_id == other.txn_id;
    }

    /*
     * begin_transaction::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const begin_transaction &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const begin_transaction &op)
{
    out << "begin_transaction(" << op.txn_id << ")";
    return out;
}

/*
 * checkpoint --
 *     A representation of this workload operation.
 */
struct checkpoint : public without_txn_id {
    std::string name;

    /*
     * checkpoint::checkpoint --
     *     Create the operation.
     */
    inline checkpoint(const char *name = nullptr) : name(name == nullptr ? "" : name) {}

    /*
     * checkpoint::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const checkpoint &other) const noexcept
    {
        return name == other.name;
    }

    /*
     * begin_transaction::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const checkpoint &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const checkpoint &op)
{
    out << "checkpoint(" << op.name << ")";
    return out;
}

/*
 * commit_transaction --
 *     A representation of this workload operation.
 */
struct commit_transaction : public with_txn_id {
    timestamp_t commit_timestamp;
    timestamp_t durable_timestamp;

    /*
     * commit_transaction::commit_transaction --
     *     Create the operation.
     */
    inline commit_transaction(txn_id_t txn_id, timestamp_t commit_timestamp = k_timestamp_none,
      timestamp_t durable_timestamp = k_timestamp_none)
        : with_txn_id(txn_id), commit_timestamp(commit_timestamp),
          durable_timestamp(durable_timestamp)
    {
    }

    /*
     * commit_transaction::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const commit_transaction &other) const noexcept
    {
        return txn_id == other.txn_id && commit_timestamp == other.commit_timestamp &&
          durable_timestamp == other.durable_timestamp;
    }

    /*
     * commit_transaction::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const commit_transaction &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const commit_transaction &op)
{
    out << "commit_transaction(" << op.txn_id << ", " << op.commit_timestamp << ", "
        << op.durable_timestamp << ")";
    return out;
}

/*
 * crash --
 *     A representation of this workload operation.
 */
struct crash : public without_txn_id {

    /*
     * crash::crash --
     *     Create the operation.
     */
    inline crash() {}

    /*
     * crash::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const crash &other) const noexcept
    {
        return true;
    }

    /*
     * crash::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const crash &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const crash &op)
{
    out << "crash()";
    return out;
}

/*
 * create_table --
 *     A representation of this workload operation.
 */
struct create_table : public without_txn_id {
    table_id_t table_id; /* This will be the table's public ID. */
    std::string name;
    std::string key_format;
    std::string value_format;

    /*
     * create_table::create_table --
     *     Create the operation.
     */
    inline create_table(
      table_id_t table_id, const char *name, const char *key_format, const char *value_format)
        : table_id(table_id), name(name), key_format(key_format), value_format(value_format)
    {
    }

    /*
     * create_table::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const create_table &other) const noexcept
    {
        return table_id == other.table_id && name == other.name && key_format == other.key_format &&
          value_format == other.value_format;
    }

    /*
     * create_table::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const create_table &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const create_table &op)
{
    out << "create_table(" << op.table_id << ", " << op.name << ", " << op.key_format << ", "
        << op.value_format << ")";
    return out;
}

/*
 * insert --
 *     A representation of this workload operation.
 */
struct insert : public with_txn_id {
    table_id_t table_id;
    data_value key;
    data_value value;

    /*
     * insert::insert --
     *     Create the operation.
     */
    inline insert(
      table_id_t table_id, txn_id_t txn_id, const data_value &key, const data_value &value)
        : with_txn_id(txn_id), table_id(table_id), key(key), value(value)
    {
    }

    /*
     * insert::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const insert &other) const noexcept
    {
        return table_id == other.table_id && txn_id == other.txn_id && key == other.key &&
          value == other.value;
    }

    /*
     * insert::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const insert &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const insert &op)
{
    out << "insert(" << op.table_id << ", " << op.txn_id << ", " << op.key << ", " << op.value
        << ")";
    return out;
}

/*
 * prepare_transaction --
 *     A representation of this workload operation.
 */
struct prepare_transaction : public with_txn_id {
    timestamp_t prepare_timestamp;

    /*
     * prepare_transaction::prepare_transaction --
     *     Create the operation.
     */
    inline prepare_transaction(txn_id_t txn_id, timestamp_t prepare_timestamp)
        : with_txn_id(txn_id), prepare_timestamp(prepare_timestamp)
    {
    }

    /*
     * prepare_transaction::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const prepare_transaction &other) const noexcept
    {
        return txn_id == other.txn_id && prepare_timestamp == other.prepare_timestamp;
    }

    /*
     * prepare_transaction::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const prepare_transaction &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const prepare_transaction &op)
{
    out << "prepare_transaction(" << op.txn_id << ", " << op.prepare_timestamp << ")";
    return out;
}

/*
 * remove --
 *     A representation of this workload operation.
 */
struct remove : public with_txn_id {
    table_id_t table_id;
    data_value key;

    /*
     * remove::remove --
     *     Create the operation.
     */
    inline remove(table_id_t table_id, txn_id_t txn_id, const data_value &key)
        : with_txn_id(txn_id), table_id(table_id), key(key)
    {
    }

    /*
     * remove::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const remove &other) const noexcept
    {
        return table_id == other.table_id && txn_id == other.txn_id && key == other.key;
    }

    /*
     * remove::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const remove &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const remove &op)
{
    out << "remove(" << op.table_id << ", " << op.txn_id << ", " << op.key << ")";
    return out;
}

/*
 * restart --
 *     A representation of this workload operation.
 */
struct restart : public without_txn_id {

    /*
     * restart::restart --
     *     Create the operation.
     */
    inline restart() {}

    /*
     * restart::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const restart &other) const noexcept
    {
        return true;
    }

    /*
     * restart::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const restart &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const restart &op)
{
    out << "restart()";
    return out;
}

/*
 * rollback_to_stable --
 *     A representation of this workload operation.
 */
struct rollback_to_stable : public without_txn_id {

    /*
     * rollback_to_stable::rollback_to_stable --
     *     Create the operation.
     */
    inline rollback_to_stable() {}

    /*
     * rollback_to_stable::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const rollback_to_stable &other) const noexcept
    {
        return true;
    }

    /*
     * rollback_to_stable::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const rollback_to_stable &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const rollback_to_stable &op)
{
    out << "rollback_to_stable()";
    return out;
}

/*
 * rollback_transaction --
 *     A representation of this workload operation.
 */
struct rollback_transaction : public with_txn_id {

    /*
     * rollback_transaction::rollback_transaction --
     *     Create the operation.
     */
    inline rollback_transaction(txn_id_t txn_id) : with_txn_id(txn_id) {}

    /*
     * rollback_transaction::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const rollback_transaction &other) const noexcept
    {
        return txn_id == other.txn_id;
    }

    /*
     * rollback_transaction::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const rollback_transaction &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const rollback_transaction &op)
{
    out << "rollback_transaction(" << op.txn_id << ")";
    return out;
}

/*
 * set_commit_timestamp --
 *     A representation of this workload operation.
 */
struct set_commit_timestamp : public with_txn_id {
    timestamp_t commit_timestamp;

    /*
     * set_commit_timestamp::set_commit_timestamp --
     *     Create the operation.
     */
    inline set_commit_timestamp(txn_id_t txn_id, timestamp_t commit_timestamp)
        : with_txn_id(txn_id), commit_timestamp(commit_timestamp)
    {
    }

    /*
     * set_commit_timestamp::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const set_commit_timestamp &other) const noexcept
    {
        return txn_id == other.txn_id && commit_timestamp == other.commit_timestamp;
    }

    /*
     * set_commit_timestamp::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const set_commit_timestamp &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const set_commit_timestamp &op)
{
    out << "set_commit_timestamp(" << op.txn_id << ", " << op.commit_timestamp << ")";
    return out;
}

/*
 * set_stable_timestamp --
 *     A representation of this workload operation.
 */
struct set_stable_timestamp : public without_txn_id {
    timestamp_t stable_timestamp;

    /*
     * set_stable_timestamp::set_stable_timestamp --
     *     Create the operation.
     */
    inline set_stable_timestamp(timestamp_t stable_timestamp) : stable_timestamp(stable_timestamp)
    {
    }

    /*
     * set_stable_timestamp::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const set_stable_timestamp &other) const noexcept
    {
        return stable_timestamp == other.stable_timestamp;
    }

    /*
     * set_stable_timestamp::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const set_stable_timestamp &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const set_stable_timestamp &op)
{
    out << "set_stable_timestamp(" << op.stable_timestamp << ")";
    return out;
}

/*
 * truncate --
 *     A representation of this workload operation.
 */
struct truncate : public with_txn_id {
    table_id_t table_id;
    data_value start;
    data_value stop;

    /*
     * truncate::truncate --
     *     Create the operation.
     */
    inline truncate(
      table_id_t table_id, txn_id_t txn_id, const data_value &start, const data_value &stop)
        : with_txn_id(txn_id), table_id(table_id), start(start), stop(stop)
    {
    }

    /*
     * truncate::operator== --
     *     Compare for equality.
     */
    inline bool
    operator==(const truncate &other) const noexcept
    {
        return table_id == other.table_id && txn_id == other.txn_id && start == other.start &&
          stop == other.stop;
    }

    /*
     * truncate::operator!= --
     *     Compare for inequality.
     */
    inline bool
    operator!=(const truncate &other) const noexcept
    {
        return !(*this == other);
    }
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const truncate &op)
{
    out << "truncate(" << op.table_id << ", " << op.txn_id << ", " << op.start << ", " << op.stop
        << ")";
    return out;
}

/*
 * any --
 *     Any workload operation.
 */
using any = std::variant<begin_transaction, checkpoint, commit_transaction, crash, create_table,
  insert, prepare_transaction, remove, restart, rollback_to_stable, rollback_transaction,
  set_commit_timestamp, set_stable_timestamp, truncate>;

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const any &op)
{
    std::visit([&out](auto &&x) { out << x; }, op);
    return out;
}

/*
 * parse --
 *     Parse an operation from a string. Throw an exception on error.
 */
any parse(const char *str);

/*
 * parse --
 *     Parse an operation from a string. Throw an exception on error.
 */
inline any
parse(const std::string &str)
{
    return parse(str.c_str());
}

/*
 * transactional --
 *     Check if the workload operation is a transactional operation, including begin and commit.
 */
inline bool
transactional(const any &op)
{
    bool r = false;
    std::visit([&r](auto &&x) { r = x.transactional(); }, op);
    return r;
}

/*
 * transaction_id --
 *     Extract the transaction ID.
 */
inline txn_id_t
transaction_id(const any &op)
{
    txn_id_t r = k_txn_none;
    std::visit([&r](auto &&x) { r = x.transaction_id(); }, op);
    return r;
}

} /* namespace operation */

/*
 * k_no_seq_no --
 *     No sequence.
 */
constexpr size_t k_no_seq_no = std::numeric_limits<size_t>::max();

/*
 * kv_workload_operation --
 *     A workload operation in a key-value database.
 */
struct kv_workload_operation {

    operation::any operation; /* The operation. */
    size_t seq_no;            /* The source sequence number, if known. */

    /*
     * kv_workload_operation::kv_workload_operation --
     *     Create a new workload operation.
     */
    inline kv_workload_operation(const operation::any &operation, size_t seq_no = k_no_seq_no)
        : operation(operation), seq_no(seq_no){};

    /*
     * kv_workload_operation::kv_workload_operation --
     *     Create a new workload operation.
     */
    inline kv_workload_operation(operation::any &&operation, size_t seq_no = k_no_seq_no)
        : operation(operation), seq_no(seq_no){};
};

/*
 * kv_workload --
 *     A workload representation for a key-value database.
 */
class kv_workload {

    friend std::ostream &operator<<(std::ostream &out, const kv_workload &workload);

public:
    /*
     * kv_workload::kv_workload --
     *     Create a new workload.
     */
    inline kv_workload() {}

    /*
     * kv_workload::operator<< --
     *     Add an operation to the workload.
     */
    inline kv_workload &
    operator<<(const operation::any &op)
    {
        _operations.push_back(kv_workload_operation(op));
        return *this;
    }

    /*
     * kv_workload::operator<< --
     *     Add an operation to the workload.
     */
    inline kv_workload &
    operator<<(operation::any &&op)
    {
        _operations.push_back(kv_workload_operation(std::move(op)));
        return *this;
    }

    /*
     * kv_workload::operator<< --
     *     Add an operation to the workload.
     */
    inline kv_workload &
    operator<<(const kv_workload_operation &op)
    {
        _operations.push_back(op);
        return *this;
    }

    /*
     * kv_workload::operator<< --
     *     Add an operation to the workload.
     */
    inline kv_workload &
    operator<<(kv_workload_operation &&op)
    {
        _operations.push_back(std::move(op));
        return *this;
    }

    /*
     * kv_workload_sequence::size --
     *     Get the length of the sequence.
     */
    inline size_t
    size() const noexcept
    {
        return _operations.size();
    }

    /*
     * kv_workload_sequence::operator[] --
     *     Get an operation in the sequence.
     */
    inline kv_workload_operation &
    operator[](size_t index)
    {
        return _operations[index];
    }

    /*
     * kv_workload_sequence::operator[] --
     *     Get an operation in the sequence.
     */
    inline const kv_workload_operation &
    operator[](size_t index) const
    {
        return _operations[index];
    }

    /*
     * kv_workload::run --
     *     Run the workload in the model. Return the return codes of the workload operations.
     */
    std::vector<int> run(kv_database &database) const;

    /*
     * kv_workload::run_in_wiredtiger --
     *     Run the workload in WiredTiger. Return the return codes of the workload operations.
     */
    std::vector<int> run_in_wiredtiger(const char *home, const char *connection_config = nullptr,
      const char *table_config = nullptr) const;

private:
    std::deque<kv_workload_operation> _operations;
};

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const kv_workload &workload)
{
    for (const kv_workload_operation &op : workload._operations)
        out << op.operation << std::endl;
    return out;
}

/*
 * operator<< --
 *     Human-readable output.
 */
inline std::ostream &
operator<<(std::ostream &out, const std::shared_ptr<kv_workload> &workload)
{
    out << *workload.get();
    return out;
}

} /* namespace model */
