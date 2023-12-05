#include <iostream>
#include <wiredtiger.h>
#include <cassert>
#include <cstdlib>
#include <random>
#include "cxxopts.hpp"
#include <sys/sdt.h>
#include <chrono>

using namespace std;

const int key_size = 16;
const int value_size = 72;
const int num_entries = 10000000;
const int default_reads = num_entries / 5;
const char *db_location = "./WT_TEST";

// pin the current running thread to certain cpu core.
// core_id starts from 1, return 0 on success.
inline int
pin_to_cpu_core(int core_id)
{
    if (core_id < 1)
        return -1;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id - 1, &cpuset);
    int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    return s;
}

string
form_key(int key_num)
{
    string key_string = to_string(key_num);
    return string(key_size - key_string.length(), '0') + key_string;
}

template <typename Clock = std::chrono::high_resolution_clock> class stopwatch {
    const typename Clock::time_point start_point;

    public:
    stopwatch() : start_point(Clock::now()) {}

    template <typename Rep = typename Clock::duration::rep,
      typename Units = typename Clock::duration>
    Rep
    elapsed_time() const
    {
        std::atomic_thread_fence(std::memory_order_relaxed);
        auto counted_time = std::chrono::duration_cast<Units>(Clock::now() - start_point).count();
        std::atomic_thread_fence(std::memory_order_relaxed);
        return static_cast<Rep>(counted_time);
    }
};

using precise_stopwatch = stopwatch<>;
using system_stopwatch = stopwatch<std::chrono::system_clock>;
using monotonic_stopwatch = stopwatch<std::chrono::steady_clock>;

int
main(int argc, char **argv)
{
    bool is_write, is_mmap, is_random, pause;
    int cache_size, compressed_size, num_reads;
    float read_ratio;
    string sql;

    cxxopts::Options commandline_options("leveldb read test", "Testing leveldb read performance.");
    commandline_options.add_options()(
      "w,write", "write", cxxopts::value<bool>(is_write)->default_value("false"))(
      "m,mmap", "mmap", cxxopts::value<bool>(is_mmap)->default_value("false"))(
      "r,random", "random", cxxopts::value<bool>(is_random)->default_value("false"))(
      "read_ratio", "read ratio", cxxopts::value<float>(read_ratio)->default_value("1"))(
      "p,pause", "pause", cxxopts::value<bool>(pause)->default_value("false"))(
      "cache_size", "cache size", cxxopts::value<int>(cache_size)->default_value("1000000"))(
      "c,compression", "compression", cxxopts::value<int>(compressed_size)->default_value("0"))(
      "n,num_reads", "num_reads", cxxopts::value<int>(num_reads)->default_value("0"));
    auto result = commandline_options.parse(argc, argv);
    if (num_reads == 0)
        num_reads = default_reads;

    WT_CONNECTION *connection;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *k, *v;
    int ret;

    if (is_write) {
        string command = string("rm -rf ") + db_location + "; mkdir " + db_location;
        (void)!system(command.c_str());  // ignore result
    }
    (void)!system("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; sudo fstrim -av");  // ignore result

    string open_config = "create,cache_size=" + to_string(cache_size) + "KB";
    if (compressed_size != 0) {
        open_config += ",extensions=[./ext/compressors/snappy/libwiredtiger_snappy.so]";
    }

    // Enable stats in json format, including file level stats for "file:kanade.wt"
    open_config += ",statistics=[all],statistics_log=(wait=1,json=true,on_close=true,sources=[\"file:kanade.wt\"])";

    // Have 4 to 8 eviction worker threads
    open_config += ",eviction=(threads_min=4,threads_max=8)";

    ret = wiredtiger_open(db_location, nullptr, open_config.c_str(), &connection);
    assert(ret == 0);
    ret = connection->open_session(connection, nullptr, nullptr, &session);
    assert(ret == 0);

    // pin_to_cpu_core(1);
    precise_stopwatch timer;
    if (is_write) {
        string create_config = "key_format=S,value_format=S";
        if (compressed_size != 0) {
            create_config += ",block_compressor=snappy";
        }
        ret = session->create(session, "table:kanade", create_config.c_str());
        assert(ret == 0);
        ret = session->open_cursor(session, "table:kanade", nullptr, nullptr, &cursor);
        assert(ret == 0);

        mt19937 generator(210);
        uniform_int_distribution<char> distribution(48, 126);

        for (int i = 0; i < num_entries; ++i) {
            string key_string = std::move(form_key(i));
            cursor->set_key(cursor, key_string.c_str());
            // printf("%d\n", i);
            string value;
            for (int j = 0; j < compressed_size; ++j) {
                value += distribution(generator);
            }
            for (int j = 0; j < value_size - compressed_size; ++j) {
                value += "0";
            }
            cursor->set_value(cursor, value.c_str());
            ret = cursor->insert(cursor);
            assert(ret == 0);
            // ret = cursor->reset(cursor);
            // assert(ret == 0);
        }

        ret = session->close(session, nullptr);
        assert(ret == 0);
        ret = connection->close(connection, nullptr);
        assert(ret == 0);

        DTRACE_PROBE2(leveldb, search1_start_probe, 0, 0);
        DTRACE_PROBE2(leveldb, search1_end_probe, 0, 0);
        DTRACE_PROBE2(leveldb, bcache_start_probe, 0, 0);
        DTRACE_PROBE2(leveldb, bcache_end_probe, 0, 0);
        // DTRACE_PROBE2(leveldb, pcache_access1, 0, 0);
        DTRACE_PROBE2(leveldb, pcache_access2, 0, 0);
    } else {
        remove("wt_trace_disk.txt");
        remove("wt_trace_eviction.txt");
        remove("wt_trace_app.txt");

        mt19937 generator(210);
        uniform_int_distribution<int> distribution(0, (int)((num_entries - 1)));

        ret = session->open_cursor(session, "table:kanade", nullptr, nullptr, &cursor);
        assert(ret == 0);

        int num_queries = is_random ? num_reads : num_entries;
        for (int i = 0; i < num_queries; ++i) {
            int key = is_random ? distribution(generator) : i;
            // if (rand() % 5 && key > num_entries / 2) key -= num_entries / 2;
            // if (rand() % 5 && key < num_entries / 2) key += num_entries / 2;
            string key_string = std::move(form_key((int)floor(key * read_ratio)));
            cout << "search " << key_string << endl;
            cursor->set_key(cursor, key_string.c_str());
            ret = cursor->search(cursor);
            assert(ret == 0);
            if (i == 0) {
                cursor->get_key(cursor, &k);
                cursor->get_value(cursor, &v);
                printf("key:%s, value:%s\n", k, v);
            }
            // ret = cursor->reset(cursor);
            // assert(ret == 0);
        }
    }
    auto diff = timer.elapsed_time<>();
    printf("Total Time: %.2f s\n", diff / (float)1000000000);

    return 0;
}
