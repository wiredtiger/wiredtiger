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

#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>

using json = nlohmann::json;

extern "C" {
#include "wt_internal.h"
}

#include "model/driver/debug_log_parser.h"
#include "model/util.h"

namespace model {

/*
 * from_json --
 *     Parse the given log entry.
 */
void
from_json(const json &j, debug_log_parser::row_put &out)
{
    j.at("fileid").get_to(out.fileid);
    j.at("key").get_to(out.key);
    j.at("value").get_to(out.value);
}

/*
 * from_json --
 *     Parse the given log entry.
 */
void
from_json(const json &j, debug_log_parser::txn_timestamp &out)
{
    j.at("commit_ts").get_to(out.commit_ts);
    j.at("durable_ts").get_to(out.durable_ts);
    j.at("prepare_ts").get_to(out.prepare_ts);
}

/*
 * debug_log_parser::metadata_apply --
 *     Apply the given operation to the model.
 */
void
debug_log_parser::metadata_apply(const row_put &op)
{
    std::string key =
      std::get<std::string>(data_value::unpack(op.key.c_str(), op.key.length(), "S"));
    std::string value =
      std::get<std::string>(data_value::unpack(op.value.c_str(), op.value.length(), "S"));

    /* Parse the configuration string. */
    std::shared_ptr<config_map> m =
      std::make_shared<config_map>(config_map::from_string(value.c_str()));

    /* Remember the metadata. */
    _metadata[key] = m;

    /* Special handling for column groups. */
    if (key.substr(0, 9) == "colgroup:") {
        std::string name = key.substr(9);
        if (name.find(':') != std::string::npos)
            throw model_exception("The model does not currently support column groups");

        std::string source = m->get_string("source");
        _file_to_colgroup[source] = name;

        /* Establish mapping from the file ID to the table name, if possible. */
        auto i = _file_to_fileid.find(source);
        if (i != _file_to_fileid.end())
            _fileid_to_table[i->second] = name;
    }

    /* Special handling for files. */
    if (key.substr(0, 5) == "file:") {
        uint64_t id = m->get_uint64("id");
        _fileid_to_file[id] = key;
        _file_to_fileid[key] = id;

        /* Establish mapping from the file ID to the table name, if possible. */
        auto i = _file_to_colgroup.find(key);
        if (i != _file_to_colgroup.end())
            _fileid_to_table[id] = i->second;
    }

    /* Special handling for LSM. */
    if (key.substr(0, 4) == "lsm:")
        throw model_exception("The model does not currently support LSM");

    /* Special handling for tables. */
    if (key.substr(0, 6) == "table:") {
        std::string name = key.substr(6);
        if (!_database.contains_table(name))
            _database.create_table(name);
    }
}

/*
 * debug_log_parser::apply --
 *     Apply the given operation to the model.
 */
void
debug_log_parser::apply(kv_transaction_ptr txn, const row_put &op)
{
    /* Handle metadata operations. */
    if (op.fileid == 0) {
        metadata_apply(op);
        return;
    }

    /* Find the table name from the file ID. */
    uint64_t fileid = op.fileid & (WT_LOGOP_IGNORE - 1);
    auto table_itr = _fileid_to_table.find(fileid);
    if (table_itr == _fileid_to_table.end())
        throw model_exception("Unknown file ID: " + std::to_string(fileid));
    std::string table = table_itr->second;

    /* Find the table's metadata. */
    auto metadata_itr = _metadata.find("table:" + table);
    if (metadata_itr == _metadata.end())
        throw model_exception("No metadata for table: " + table);
    std::shared_ptr<config_map> table_metadata = metadata_itr->second;

    /* Parse the key and the value. */
    data_value key = data_value::unpack(
      op.key.c_str(), op.key.length(), table_metadata->get_string("key_format").c_str());
    data_value value = data_value::unpack(
      op.value.c_str(), op.value.length(), table_metadata->get_string("value_format").c_str());

    /* Perform the operation. */
    _database.table(table)->insert(txn, key, value);
}

/*
 * debug_log_parser::apply --
 *     Apply the given operation to the model.
 */
void
debug_log_parser::apply(kv_transaction_ptr txn, const txn_timestamp &op)
{
    /* Handle the prepare operation. */
    if (op.commit_ts == k_timestamp_none && op.prepare_ts != k_timestamp_none) {
        txn->prepare(op.prepare_ts);
        return;
    }

    /* Handle the commit of a prepared transaction. */
    if (op.commit_ts != k_timestamp_none && op.prepare_ts != k_timestamp_none) {
        if (txn->state() != kv_transaction_state::prepared)
            throw model_exception("The transaction must be in a prepared state before commit");
        txn->commit(op.commit_ts, op.durable_ts);
        return;
    }

    /* Otherwise it is just an operation to set the commit timestamp. */
    txn->set_commit_timestamp(op.commit_ts);
}

/*
 * debug_log_parser::parse_json --
 *     Parse the debug log JSON file into the model.
 */
void
debug_log_parser::parse_json(kv_database &database, const char *path)
{
    debug_log_parser parser(database);

    /* Load the JSON from the provided file. */
    std::ifstream f(path);
    json data = json::parse(f);

    /* The debug log JSON file is structured as an array of log entries. */
    if (!data.is_array())
        throw model_exception("The top-level element in the JSON file is not an array");

    /* Now parse each individual entry. */
    for (auto &log_entry : data) {
        if (!log_entry.is_object())
            throw model_exception("The second-level element in the JSON file is not an object");

        std::string log_entry_type = log_entry.at("type").get<std::string>();

        /* The commit entry contains full description of a transaction, including all operations. */
        if (log_entry_type == "commit") {
            kv_transaction_ptr txn = database.begin_transaction();

            /* Replay all operations. */
            for (auto &op_entry : log_entry.at("ops")) {
                std::string op_type = op_entry.at("optype").get<std::string>();

                /* Row-store operations. */
                if (op_type == "row_modify")
                    throw model_exception("Unsupported operation.");
                if (op_type == "row_put") {
                    parser.apply(txn, op_entry.get<row_put>());
                    continue;
                }
                if (op_type == "row_remove")
                    throw model_exception("Unsupported operation.");
                if (op_type == "row_truncate")
                    throw model_exception("Unsupported operation.");

                /* Transaction operations. */
                if (op_type == "txn_timestamp") {
                    parser.apply(txn, op_entry.get<txn_timestamp>());
                    continue;
                }

                /* Operations that we can skip... for now. */
                if (op_type == "prev_lsn" || op_type == "checkpoint_start")
                    continue;

                /* Column-store operations (unsupported). */
                if (op_type.substr(0, 4) == "col_")
                    throw model_exception("The parser does not currently support column stores.");

                throw model_exception("Unsupported operation \"" + op_type + "\"");
            }

            if (txn->state() != kv_transaction_state::committed)
                txn->commit();
            continue;
        }

        /* Ignore these fields. */
        if (log_entry_type == "checkpoint" || log_entry_type == "file_sync" ||
          log_entry_type == "message" || log_entry_type == "system")
            continue;

        throw model_exception("Unsupported log entry type \"" + log_entry_type + "\"");
    }
}

} /* namespace model */
