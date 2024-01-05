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

#include <iomanip>
#include <iostream>
#include <sstream>

extern "C" {
#include "wt_internal.h"
}

#include "model/driver/kv_workload.h"
#include "model/driver/kv_workload_context.h"
#include "model/driver/kv_workload_context_wt.h"
#include "model/kv_database.h"
#include "model/kv_table.h"
#include "model/kv_transaction.h"
#include "model/util.h"

namespace model {

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::begin_transaction &op)
{
    context.add_transaction(op.txn_id, context.database().begin_transaction());
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::checkpoint &op)
{
    context.database().create_checkpoint(op.name == "" ? nullptr : op.name.c_str());
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::commit_transaction &op)
{
    context.remove_transaction(op.txn_id)->commit(op.commit_timestamp, op.durable_timestamp);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::create_table &op)
{
    kv_table_ptr table = context.database().create_table(op.name);
    table->set_key_value_format(op.key_format, op.value_format);
    context.add_table(op.table_id, table);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::insert &op)
{
    int ret = context.table(op.table_id)->insert(context.transaction(op.txn_id), op.key, op.value);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::prepare_transaction &op)
{
    context.transaction(op.txn_id)->prepare(op.prepare_timestamp);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::remove &op)
{
    int ret = context.table(op.table_id)->remove(context.transaction(op.txn_id), op.key);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::restart &op)
{
    (void)op;
    context.restart();
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::rollback_to_stable &op)
{
    (void)op;
    context.database().rollback_to_stable();
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::rollback_transaction &op)
{
    context.remove_transaction(op.txn_id)->rollback();
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::set_commit_timestamp &op)
{
    context.transaction(op.txn_id)->set_commit_timestamp(op.commit_timestamp);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::set_stable_timestamp &op)
{
    context.database().set_stable_timestamp(op.stable_timestamp);
}

/*
 * model_execute --
 *     Execute the given workload operation in the model.
 */
static void
model_execute(kv_workload_context &context, const operation::truncate &op)
{
    int ret =
      context.table(op.table_id)->truncate(context.transaction(op.txn_id), op.start, op.stop);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * kv_workload::run --
 *     Run the workload.
 */
void
kv_workload::run(kv_database &database) const
{
    kv_workload_context context{database};
    for (const operation::any &op : _operations)
        std::visit([&context](auto &&x) { model_execute(context, x); }, op);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::begin_transaction &op)
{
    kv_workload_context_wt::session_context_ptr session = context.allocate_txn_session(op.txn_id);
    int ret = session->session()->begin_transaction(session->session(), nullptr);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::checkpoint &op)
{
    WT_CONNECTION *conn = context.connection();

    WT_SESSION *session;
    int ret = conn->open_session(conn, nullptr, nullptr, &session);
    if (ret != 0)
        throw wiredtiger_exception("Failed to open a session", ret);
    wiredtiger_session_guard session_guard(session);

    std::ostringstream config;
    if (op.name != "")
        config << "name=" << op.name;

    std::string config_str = config.str();
    ret = session->checkpoint(session, config_str.c_str());
    if (ret != 0)
        throw wiredtiger_exception("Failed to create a checkpoint", ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::commit_transaction &op)
{
    kv_workload_context_wt::session_context_ptr session = context.remove_txn_session(op.txn_id);

    std::ostringstream config;
    if (op.commit_timestamp != k_timestamp_none)
        config << ",commit_timestamp=" << std::hex << op.commit_timestamp;
    if (op.durable_timestamp != k_timestamp_none)
        config << ",durable_timestamp=" << std::hex << op.durable_timestamp;

    std::string config_str = config.str();
    int ret = session->session()->commit_transaction(session->session(), config_str.c_str());

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::create_table &op)
{
    WT_CONNECTION *conn = context.connection();

    WT_SESSION *session;
    int ret = conn->open_session(conn, nullptr, nullptr, &session);
    if (ret != 0)
        throw wiredtiger_exception("Failed to open a session", ret);
    wiredtiger_session_guard session_guard(session);

    std::ostringstream config;
    config << "log=(enabled=false)";
    config << ",key_format=" << op.key_format << ",value_format=" << op.value_format;

    std::string config_str = config.str();
    std::string uri = std::string("table:") + op.name;
    ret = session->create(session, uri.c_str(), config_str.c_str());
    if (ret != 0)
        throw wiredtiger_exception("Failed to create a table", ret);

    context.add_table_uri(op.table_id, uri);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::insert &op)
{
    kv_workload_context_wt::session_context_ptr session = context.txn_session(op.txn_id);
    WT_CURSOR *cursor = session->cursor(op.table_id);
    int ret = wt_cursor_insert(cursor, op.key, op.value);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::prepare_transaction &op)
{
    kv_workload_context_wt::session_context_ptr session = context.txn_session(op.txn_id);

    std::ostringstream config;
    if (op.prepare_timestamp != k_timestamp_none)
        config << ",prepare_timestamp=" << std::hex << op.prepare_timestamp;

    std::string config_str = config.str();
    int ret = session->session()->prepare_transaction(session->session(), config_str.c_str());

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::remove &op)
{
    kv_workload_context_wt::session_context_ptr session = context.txn_session(op.txn_id);
    WT_CURSOR *cursor = session->cursor(op.table_id);
    int ret = wt_cursor_remove(cursor, op.key);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::restart &op)
{
    (void)op;

    context.wiredtiger_close();
    context.wiredtiger_open();
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::rollback_to_stable &op)
{
    (void)op;

    WT_CONNECTION *conn = context.connection();
    int ret = conn->rollback_to_stable(conn, nullptr);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::rollback_transaction &op)
{
    kv_workload_context_wt::session_context_ptr session = context.remove_txn_session(op.txn_id);
    int ret = session->session()->rollback_transaction(session->session(), nullptr);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::set_commit_timestamp &op)
{
    kv_workload_context_wt::session_context_ptr session = context.txn_session(op.txn_id);

    std::ostringstream config;
    if (op.commit_timestamp != k_timestamp_none)
        config << ",commit_timestamp=" << std::hex << op.commit_timestamp;

    std::string config_str = config.str();
    int ret = session->session()->timestamp_transaction(session->session(), config_str.c_str());

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::set_stable_timestamp &op)
{
    WT_CONNECTION *conn = context.connection();

    std::ostringstream config;
    config << "stable_timestamp=" << std::hex << op.stable_timestamp;

    std::string config_str = config.str();
    int ret = conn->set_timestamp(conn, config_str.c_str());

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception("Failed to set the stable timestamp", ret);
}

/*
 * wt_execute --
 *     Execute the given workload operation in WiredTiger.
 */
static void
wt_execute(kv_workload_context_wt &context, const operation::truncate &op)
{
    kv_workload_context_wt::session_context_ptr session = context.txn_session(op.txn_id);
    WT_CURSOR *cursor1 = session->cursor(op.table_id);
    WT_CURSOR *cursor2 = session->cursor(op.table_id, 1);
    int ret = wt_cursor_truncate(
      session->session(), context.table_uri(op.table_id), cursor1, cursor2, op.start, op.stop);

    /*
     * In the future, we would like to be able to test operations that can fail, at which point we
     * would record and compare return codes. But we're not there yet, so just fail on error.
     */
    if (ret != 0)
        throw wiredtiger_exception(ret);
}

/*
 * kv_workload::run --
 *     Run the workload in WiredTiger.
 */
void
kv_workload::run_in_wiredtiger(const char *home, const char *connection_config) const
{
    kv_workload_context_wt context{home, connection_config};
    context.wiredtiger_open();

    for (const operation::any &op : _operations)
        std::visit([&context](auto &&x) { wt_execute(context, x); }, op);

    context.wiredtiger_close();
}

} /* namespace model */
