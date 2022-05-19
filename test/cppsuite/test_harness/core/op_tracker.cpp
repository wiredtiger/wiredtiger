#include "test_harness/test.h"
#include "op_tracker.h"

namespace test_harness {
op_tracker::op_tracker(const std::string id, const std::string &test_name)
    : _id(id), _test_name(test_name), _it_count(0), _total_time_taken(0)
{
}

void
op_tracker::append_stats()
{
    std::ofstream perf_file;

    perf_file.open(_test_name + ".json", std::ios_base::app);

    uint64_t avg = (uint64_t)_total_time_taken / _it_count;
    auto stat_info = "{\"name\":\"" + _id + "\",\"value\":" + std::to_string(avg) + "},";

    // /* Remove last extra comma. */
    // if (stat_info.back() == ',')
    //     stat_info.pop_back();

    perf_file << stat_info;
    perf_file.close();
}

template <typename T>
auto
op_tracker::track(T lambda)
{
    auto _start_time = std::chrono::steady_clock::now();
    int ret = lambda();
    auto _end_time = std::chrono::steady_clock::now();
    _total_time_taken += (_end_time - _start_time).count();
    _it_count += 1;

    return ret;
}

op_tracker::~op_tracker()
{
    if (_it_count != 0)
        append_stats();
}
}; // namespace test_harness
