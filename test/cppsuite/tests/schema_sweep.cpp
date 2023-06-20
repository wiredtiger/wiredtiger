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

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/main/test.h"

namespace test_harness {

// The intent of the test is to stress the code paths related to schema operations and dhandles
// management. This test keeps performing schema operations while reconfiguring the sweep server.
class schema_sweep : public test {
public:
    schema_sweep(const test_args &args) : test(args)
    {
        init_operation_tracker();
    }

    // Reconfigures the connection with different sweep server parameters.
    void
    custom_operation(thread_worker *tw) override final
    {
        WT_CONNECTION *conn = connection_manager::instance().get_connection();
        bool aggressive_sweep = false;
        const std::string aggressive_sweep_cfg =
          "file_manager=(close_handle_minimum=0,close_idle_time=1,close_scan_interval=1)";
        const std::string default_sweep_cfg =
          "file_manager=(close_handle_minimum=250,close_idle_time=30,close_scan_interval=10)";

        while (tw->running()) {
            tw->sleep();
            testutil_check(conn->reconfigure(
              conn, aggressive_sweep ? default_sweep_cfg.c_str() : aggressive_sweep_cfg.c_str()));
            aggressive_sweep = !aggressive_sweep;
        }
    }

    // Keeps creating collections.
    void
    insert_operation(thread_worker *tw) override final
    {
        auto max_coll_count = 1000;
        while (tw->running()) {
            if (tw->db.get_collection_count() < max_coll_count) {
                auto session = connection_manager::instance().create_session();
                tw->db.add_collection(session);
            }
            tw->sleep();
        }
    }

    // Keeps deleting collections.
    void
    remove_operation(thread_worker *tw) override final
    {
        while (tw->running()) {
            if (tw->db.get_collection_count() != 0) {
                std::string cfg = "force=";
                cfg += random_generator::instance().generate_bool() ? "true" : "false";
                // We don't need to check whether the collection has been actually removed or not.
                tw->db.remove_random_collection(cfg);
            }
            tw->sleep();
        }
    }

    // Selects a random collection and performs an update on it. Note that the collection can be
    // deleted while the update is happening.
    void
    update_operation(thread_worker *tw) override final
    {
        while (tw->running()) {
            tw->sleep();
            if (tw->db.get_collection_count() == 0)
                continue;
            {
                /*
                 * Use a function that holds a lock while retrieving the name. If we retrieve the
                 * collection then get the name from it, another thread may free the data allocated
                 * to the collection in parallel.
                 */
                const std::string collection_name(tw->db.get_random_collection_name());
                auto session = connection_manager::instance().create_session();

                WT_CURSOR *cursor;
                int ret = session->open_cursor(
                  session.get(), collection_name.c_str(), nullptr, nullptr, &cursor);
                // The collection may have been / is being deleted.
                testutil_assert(ret == 0 || ret == WT_NOTFOUND || ret == EBUSY);
                if (ret != 0)
                    continue;

                // We have a cursor opened on the collection, the ref should prevent it from being
                // deleted and we can perform an update safely.
                testutil_check(session->begin_transaction(session.get(), nullptr));

                auto key = random_generator::instance().generate_pseudo_random_string(tw->key_size);
                auto value =
                  random_generator::instance().generate_pseudo_random_string(tw->value_size);
                cursor->set_key(cursor, key.c_str());
                cursor->set_value(cursor, value.c_str());
                testutil_check(cursor->update(cursor));

                testutil_check(session->commit_transaction(session.get(), nullptr));
            }
        }
    }
};

} // namespace test_harness
