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

#ifndef RUNTIME_MONITOR_H
#define RUNTIME_MONITOR_H

#include <string>
#include <vector>

extern "C" {
#include "wiredtiger.h"
}

#include "core/configuration.h"
#include "workload/database_model.h"
#include "workload/database_operation.h"
#include "util/scoped_types.h"

namespace test_harness {
/* Static statistic get function. */
static void
get_stat(scoped_cursor &cursor, int stat_field, int64_t *valuep)
{
    const char *desc, *pvalue;
    cursor->set_key(cursor.get(), stat_field);
    testutil_check(cursor->search(cursor.get()));
    testutil_check(cursor->get_value(cursor.get(), &desc, &pvalue, valuep));
    testutil_check(cursor->reset(cursor.get()));
}

static std::string
collection_name_to_file_name(const std::string &collection_name)
{
    /* Strip out the URI prefix. */
    const size_t colon_pos = collection_name.find(':');
    testutil_assert(colon_pos != std::string::npos);
    const auto stripped_name = collection_name.substr(colon_pos + 1);

    /* Now add the directory and file extension. */
    return (std::string(DEFAULT_DIR) + "/" + stripped_name + ".wt");
}

/*
 * The WiredTiger configuration API doesn't accept string statistic names when retrieving statistic
 * values. This function provides the required mapping to statistic id. We should consider
 * generating it programmatically in `stat.py` to avoid having to manually add a condition every
 * time we want to observe a new postrun statistic.
 */
inline int
get_stat_field(const std::string &name)
{
    if (name == "cache_hs_insert")
        return (WT_STAT_CONN_CACHE_HS_INSERT);
    else if (name == "cc_pages_removed")
        return (WT_STAT_CONN_CC_PAGES_REMOVED);
    testutil_die(EINVAL, "get_stat_field: Stat \"%s\" is unrecognized", name.c_str());
}

class runtime_statistic {
    public:
    explicit runtime_statistic(configuration *config);

    /* Check that the given statistic is within bounds. */
    virtual void check(scoped_cursor &cursor) = 0;

    /* Suppress warning about destructor being non-virtual. */
    virtual ~runtime_statistic() {}

    bool enabled() const;

    protected:
    bool _enabled = false;
};

class cache_limit_statistic : public runtime_statistic {
    public:
    explicit cache_limit_statistic(configuration *config);

    void check(scoped_cursor &cursor) override final;

    private:
    int64_t limit;
};

class db_size_statistic : public runtime_statistic {
    public:
    db_size_statistic(configuration *config, database &database);
    virtual ~db_size_statistic() = default;

    /* Don't need the stat cursor for this. */
    void check(scoped_cursor &) override final;

    private:
    std::vector<std::string> get_file_names();

    private:
    database &_database;
    int64_t _limit;
};

class postrun_statistic_check {
    public:
    explicit postrun_statistic_check(configuration *config);
    virtual ~postrun_statistic_check() = default;

    void check(scoped_cursor &cursor) const;

    private:
    struct postrun_statistic {
        postrun_statistic(std::string &&name, const int64_t min_limit, const int64_t max_limit)
            : name(std::move(name)), field(get_stat_field(this->name)), min_limit(min_limit),
              max_limit(max_limit)
        {
        }
        const std::string name;
        const int field;
        const int64_t min_limit, max_limit;
    };

    private:
    bool check_stat(scoped_cursor &cursor, const postrun_statistic &stat) const;

    private:
    std::vector<postrun_statistic> _stats;
};

/*
 * The runtime monitor class is designed to track various statistics or other runtime signals
 * relevant to the given workload.
 */
class runtime_monitor : public component {
    public:
    runtime_monitor(configuration *config, database &database);
    ~runtime_monitor();

    /* Delete the copy constructor and the assignment operator. */
    runtime_monitor(const runtime_monitor &) = delete;
    runtime_monitor &operator=(const runtime_monitor &) = delete;

    void load() override final;
    void do_work() override final;
    void finish() override final;

    private:
    scoped_session _session;
    scoped_cursor _cursor;
    std::vector<runtime_statistic *> _stats;
    postrun_statistic_check _postrun_stats;
    database &_database;
};
} // namespace test_harness

#endif
