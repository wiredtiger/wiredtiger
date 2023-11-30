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

#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/main/test.h"

using namespace test_harness;

class sequential_read : public test {
public:
    sequential_read(const test_args &args) : test(args)
    {
        _block_read = _config->get_bool("block_read");
        init_operation_tracker();
    }

    void
    read_operation(thread_worker *tc) override final
    {
        logger::log_msg(
          LOG_INFO, type_string(tc->type) + " thread {" + std::to_string(tc->id) + "} commencing.");

        /* In this test we use single thread. */
        testutil_assert(tc->thread_count == 1);

        collection &coll = tc->db.get_collection(tc->id);
        scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name, "block=true");

        tc->txn.begin();
        WT_CURSOR *c = cursor.get();
        if (_block_read) {
            while (true) {
                WT_ITEM *keys;
                WT_ITEM *values;
                size_t nret;
                auto ret = c->next_raw_n(c, 100, &keys, &values, &nret);
                if (ret != 0)
                    return;
            }
        } else {
            while (true) {
                WT_ITEM key;
                WT_ITEM value;
                auto ret = c->next(c);
                if (ret != 0)
                    return;
                c->get_key(c, &key);
                c->get_value(c, &value);
            }
        }

        return;
    }

private:
    bool _block_read = false;
};
