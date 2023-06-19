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

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class schema_sweep : public test {
public:
    schema_sweep(const test_args &args) : test(args)
    {
        init_operation_tracker();
    }

    /* Reconfigures the connection with different sweep server parameters 50% of the time. */
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
            if (random_generator::instance().generate_bool()) {
                testutil_check(conn->reconfigure(conn,
                  aggressive_sweep ? default_sweep_cfg.c_str() : aggressive_sweep_cfg.c_str()));
                aggressive_sweep = !aggressive_sweep;
            }
        }
    }

    /* Keeps creating collections. */
    void
    insert_operation(thread_worker *tw) override final
    {
        auto collection_name_len = 10;
        auto max_coll_count = 1000;

        while (tw->running()) {
            if (tw->db.get_collection_count() < max_coll_count)
                tw->db.add_collection();
            tw->sleep();
        }
    }

    /* Keeps deleting collections. */
    void
    remove_operation(thread_worker *tw) override final
    {
        auto collection_name_len = 10;

        while (tw->running()) {
            if (tw->db.get_collection_count() != 0) {
                auto &collection = tw->db.get_random_collection();
                bool force = random_generator::instance().generate_bool();

                std::string cfg = "force=";
                cfg += force ? "true" : "false";

                int ret;
                while (ret = tw->session->drop(
                               tw->session.get(), collection.name.c_str(), cfg.c_str()) == EBUSY)
                    ;
                testutil_assert(ret == 0 || ret == WT_NOTFOUND);
            }
            tw->sleep();
        }
    }

    void
    validate(const std::string &, const std::string &, database &) override final
    {
        logger::log_msg(LOG_WARN, "validate: nothing done");
    }
};

} // namespace test_harness
