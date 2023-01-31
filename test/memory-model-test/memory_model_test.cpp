/*
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 * See the README.md file for more information about this test and how to build and run it.
 *
 * This test was inspired by the work at https://preshing.com/20120515/memory-reordering-caught-in-the-act/
 */

#include <iostream>
#include <semaphore>
#include <thread>
#include <chrono>
#include <random>
#include <utility>
#include <unistd.h>

#if defined(x86_64) || defined(__x86_64__)
#define BARRIER_INSTRUCTION "mfence"
const bool is_arm64 = false;
#elif defined(__aarch64__)
#define BARRIER_INSTRUCTION "dmb ish"
const bool is_arm64 = true;
#endif


template<typename code>
void thread_function(std::string const& thread_name,
                     std::binary_semaphore& start_semaphore,
                     std::binary_semaphore& end_semaphore,
                     int rng_seed,
                     int loop_count,
                     code code_param) {
    std::mt19937 rng(rng_seed);
    for(int iterations = 0; iterations < loop_count; iterations++) {
        start_semaphore.acquire();
        while(rng() % 8 != 0) {};         // Short random delay
        code_param();
        end_semaphore.release();
    }
}


template<typename thread_1_code_t, typename thread_2_code_t, typename out_of_order_check_code_t>
class test_config {
public:
    test_config(std::string test_name,
                std::string test_description,
                thread_1_code_t thread_1_code,
                thread_2_code_t thread_2_code,
                out_of_order_check_code_t out_of_order_check_code,
                bool out_of_order_allowed)
      : _test_name(std::move(test_name)),
        _test_description(std::move(test_description)),
        _thread_1_code(thread_1_code),
        _thread_2_code(thread_2_code),
        _out_of_order_check_code(out_of_order_check_code),
        _out_of_order_allowed(out_of_order_allowed)
    {}

    std::string _test_name;
    std::string _test_description;
    thread_1_code_t _thread_1_code;
    thread_2_code_t _thread_2_code;
    out_of_order_check_code_t _out_of_order_check_code;
    bool _out_of_order_allowed;
};


template<typename thread_1_code, typename thread_2_code, typename out_of_order_check_code>
void perform_test(test_config<thread_1_code, thread_2_code, out_of_order_check_code> config,
                  int& x,
                  int& y,
                  int& r1,
                  int& r2,
                  std::binary_semaphore& start_semaphore1,
                  std::binary_semaphore& start_semaphore2,
                  std::binary_semaphore& end_semaphore1,
                  std::binary_semaphore& end_semaphore2,
                  int loop_count,
                  bool progress)
{
    std::cout << "Test name:        " << config._test_name << std::endl;
    std::cout << "Test description: " << config._test_description << std::endl;

    std::thread thread_1([&](){ thread_function("thread_one", start_semaphore1, end_semaphore1, 1, loop_count,config._thread_1_code); });
    std::thread thread_2([&](){ thread_function("thread_two", start_semaphore2, end_semaphore2, 2, loop_count,config._thread_2_code); });

    int iterations = 0;
    int out_of_order_count = 0;

    for (iterations = 0; iterations < loop_count; iterations++) {
        x = 0;
        y = 0;
        r1 = 0;
        r2 = 0;

        // Release the start semaphores to allow the worker threads to start an iteration of their work.
        start_semaphore1.release();
        start_semaphore2.release();
        // The threads do an iteration of their work at this point.
        // Wait on the end semaphores to know when they are finished.
        end_semaphore1.acquire();
        end_semaphore2.acquire();
        bool out_of_order = config._out_of_order_check_code();
        if (out_of_order) {
            out_of_order_count ++;
            if (progress)
                std::cout << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;
        }

        if (progress && iterations % 1000 == 0 && iterations != 0) {
            std::cout << '.' << std::flush;
            if (iterations % 50000 == 0) {
                std::cout << std::endl;
            }
        }
    }

    if (progress)
        // Ensure we have a newline after the last '.' is printed
        std::cout << std::endl;

    std::cout << "Total of " << out_of_order_count << " out of orders detected out of " << iterations <<
              " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;
    if (!config._out_of_order_allowed && out_of_order_count > 0)
        std::cout << "******** ERROR out of order operations were not allowed, but did occur. ********" << std::endl;
    std::cout << std::endl;

    thread_1.join();
    thread_2.join();
}


int main(int argc, char *argv[]) {
    std::cout << "WiredTiger Memory Model Test" << std::endl;
    std::cout << "============================" << std::endl;

    int loop_count = 1000000;

    int opt = 0;
    while ((opt = getopt(argc, argv, "n:")) != -1) {
        switch (opt) {
            case 'n':
                loop_count = atoi(optarg);
                break;
            default:
                break;
        }
    }

    if (is_arm64)
        std::cout << "Running on ARM64";
    else
        std::cout << "Running on x86";
    std::cout << " with loop count " << loop_count << std::endl << std::endl;

    std::binary_semaphore start_semaphore1{0};
    std::binary_semaphore start_semaphore2{0};
    std::binary_semaphore end_semaphore1{0};
    std::binary_semaphore end_semaphore2{0};

    // We declare the shared variables above the lambdas so the lambdas have access to them.
    int x = 0;
    int y = 0;
    int r1 = 0;
    int r2 = 0;

    //////////////////////////////////////////////////
    // Code that has a read and a write in each thread.
    //////////////////////////////////////////////////
    auto thread_1_code_write_then_read = [&]() { x = 1; r1 = y; };
    auto thread_2_code_write_then_read = [&]() { y = 1; r2 = x; };

    auto thread_1_code_write_then_barrier_then_read = [&]() { x = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r1 = y; };
    auto thread_2_code_write_then_barrier_then_read = [&]() { y = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r2 = x; };

    auto thread_1_atomic_increment_and_read = [&]() { __atomic_add_fetch(&x, 1, __ATOMIC_SEQ_CST); r1 = y; };
    auto thread_2_atomic_increment_and_read = [&]() { __atomic_add_fetch(&y, 1, __ATOMIC_SEQ_CST); r2 = x; };

    auto out_of_order_check_code_for_write_then_read = [&]() { return r1 == 0 && r2 == 0; };

    /////////////////////////////////////////////////////////////////////////////
    // Code that has two reads in one thread, and two writes in the other thread.
    /////////////////////////////////////////////////////////////////////////////
    auto thread_1_code_write_then_write = [&]() { x = 2; y = 3; };
    auto thread_2_code_read_then_read = [&]() { r1 = y; r2 = x; };
    auto thread_1_code_two_atomic_increments = [&]() { __atomic_exchange_n(&x, 2, __ATOMIC_SEQ_CST); __atomic_exchange_n(&y, 3, __ATOMIC_SEQ_CST); };
    auto thread_1_code_write_then_barrier_then_write = [&]() { x = 2; asm volatile(BARRIER_INSTRUCTION ::: "memory"); y = 3; };
    auto thread_2_code_read_then_barrier_then_read = [&]() { r1 = y; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r2 = x; };

    auto out_of_order_check_code_for_write_then_write = [&]() { return r1 == 3 && r2 == 0; };

    /////////////////////////////////////////////////////
    // Tests that have a read and a write in each thread.
    /////////////////////////////////////////////////////

    auto test_writes_then_reads =
            test_config("Test writes then reads",
                        "Each thread writes then reads. Out of orders ARE POSSIBLE.",
                        thread_1_code_write_then_read,
                        thread_2_code_write_then_read,
                        out_of_order_check_code_for_write_then_read,
                        true);

    auto test_writes_then_reads_one_barrier =
            test_config("Test writes then reads with one barrier",
                        "Each thread writes then reads, with one barrier between the write and read on thread 2. "
                        "Out of orders ARE POSSIBLE.",
                        thread_1_code_write_then_read,
                        thread_2_code_write_then_barrier_then_read,
                        out_of_order_check_code_for_write_then_read,
                        true);

    auto test_writes_then_reads_two_barriers =
            test_config("Test writes then reads with two barriers",
                        "Each thread writes then reads, with a barrier between the write and read on each thread. "
                        "Out of orders are NOT POSSIBLE.",
                        thread_1_code_write_then_barrier_then_read,
                        thread_2_code_write_then_barrier_then_read,
                        out_of_order_check_code_for_write_then_read,
                        false);

    auto test_writes_then_reads_one_atomic =
            test_config("Test writes then reads with one atomic",
                        "Each thread writes then reads, with one atomic increment used for one write. "
                        "Out of orders ARE POSSIBLE.",
                       thread_1_atomic_increment_and_read,
                       thread_2_code_write_then_read,
                        out_of_order_check_code_for_write_then_read,
                        true);

    auto test_writes_then_reads_two_atomics =
            test_config("Test writes then reads with two atomics",
                        "Each thread writes then reads, with atomic increments used for both writes. "
                        "Out of orders are NOT POSSIBLE.",
                        thread_1_atomic_increment_and_read,
                        thread_2_atomic_increment_and_read,
                        out_of_order_check_code_for_write_then_read,
                        false);

    auto test_writes_then_reads_one_barrier_one_atomic =
            test_config("Test writes then reads with one barrier and one atomic",
                        "Each thread writes then reads, with atomic increments used for for one write, "
                        "and a barrier used between the write and read in the other thread. "
                        "Out of orders are NOT POSSIBLE.",
                        thread_1_atomic_increment_and_read,
                        thread_2_atomic_increment_and_read,
                        out_of_order_check_code_for_write_then_read,
                        false);

    ///////////////////////////////////////////////////////////////////////////////
    // Tests that have two reads in one thread, and two writes in the other thread.
    ///////////////////////////////////////////////////////////////////////////////

    auto test_writes_and_reads =
            test_config("Test writes and reads",
                        "One thread has two writes, the other has two reads. "
                        "Out of orders ARE POSSIBLE on ARM64.",
                        thread_1_code_write_then_write,
                        thread_2_code_read_then_read,
                        out_of_order_check_code_for_write_then_write,
                        is_arm64);

    auto test_writes_and_reads_barrier_between_writes =
            test_config("Test writes and reads, with barrier between writes",
                        "One thread has two writes with a barrier between them, the other has two reads. "
                        "Out of orders ARE POSSIBLE on ARM64.",
                        thread_1_code_write_then_barrier_then_write,
                        thread_2_code_read_then_read,
                        out_of_order_check_code_for_write_then_write,
                        is_arm64);

    auto test_writes_and_reads_barrier_between_reads =
            test_config("Test writes and reads, with barrier between reads",
                        "One thread has two writes, the other has two reads with a barrier between them. "
                        "Out of orders are NOT POSSIBLE.",
                        thread_1_code_write_then_read,
                        thread_2_code_read_then_barrier_then_read,
                        out_of_order_check_code_for_write_then_write,
                        false);

    auto test_writes_and_reads_barrier_between_writes_and_between_reads =
            test_config("Test writes and reads, with barrier between writes and between reads",
                        "One thread has two writes with a barrier between them, "
                        "the other has two reads with a barrier between them. "
                        "Out of orders are NOT POSSIBLE.",
                        thread_1_code_write_then_barrier_then_write,
                        thread_2_code_read_then_barrier_then_read,
                        out_of_order_check_code_for_write_then_write,
                        false);

    auto test_writes_and_reads_atomics =
            test_config("Test writes and reads, with atomics",
                        "One thread has two writes using atomic increments, the other has two reads. "
                        "Out of orders are ARE POSSIBLE on ARM64.",
                        thread_1_code_two_atomic_increments,
                        thread_2_code_read_then_read,
                        out_of_order_check_code_for_write_then_write,
                        is_arm64);

    const bool progress = false;

    perform_test(test_writes_then_reads,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_then_reads_one_barrier,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_then_reads_two_barriers,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_then_reads_one_atomic,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_then_reads_two_atomics,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_and_reads,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_and_reads_barrier_between_writes,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_and_reads_barrier_between_reads,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_and_reads_barrier_between_writes_and_between_reads,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);

    perform_test(test_writes_and_reads_atomics,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count,
                 progress);
}


