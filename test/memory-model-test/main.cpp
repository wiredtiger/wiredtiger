#include <iostream>
#include <semaphore>
#include <thread>
#include <chrono>
#include <random>

#if defined(x86_64) || defined(__x86_64__)
#define BARRIER_INSTRUCTION "mfence"
#elif defined(__aarch64__)
#define BARRIER_INSTRUCTION "dmb ish"
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


template<typename thread_1_code, typename thread_2_code, typename out_of_order_check_code>
int perform_test(thread_1_code thread_1_code_param,
                 thread_2_code thread_2_code_param,
                 out_of_order_check_code out_of_order_check_code_param,
                 int& x,
                 int& y,
                 int& r1,
                 int& r2,
                 std::binary_semaphore& start_semaphore1,
                 std::binary_semaphore& start_semaphore2,
                 std::binary_semaphore& end_semaphore1,
                 std::binary_semaphore& end_semaphore2,
                 int loop_count) {

    std::thread thread_1([&](){ thread_function("thread_one", start_semaphore1, end_semaphore1, 1, loop_count,thread_1_code_param); });
    std::thread thread_2([&](){ thread_function("thread_two", start_semaphore2, end_semaphore2, 2, loop_count,thread_2_code_param); });

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
        bool out_of_order = out_of_order_check_code_param();
        if (out_of_order) {
            out_of_order_count ++;
            std::cout << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;
        }

        if (iterations % 1000 == 0 && iterations != 0) {
            std::cout << '.' << std::flush;
            if (iterations % 50000 == 0) {
                std::cout << std::endl;
            }
        }
    }

    std::cout << std::endl;
    std::cout << "Total of " << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;

    thread_1.join();
    thread_2.join();

    return 0;
}


int main() {
    std::cout << "Jeremy's Memory Model Test" << std::endl;

    const int loop_count = 1000000;

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
//    auto thread_1_code = [&]() { x = 1; r1 = y; };
//    auto thread_2_code = [&]() { y = 1; r2 = x; };

//    auto thread_1_code = [&]() { x = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r1 = y; };
//    auto thread_2_code = [&]() { y = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r2 = x; };

//    auto thread_1_code = [&]() { __atomic_add_fetch(&x, 1, __ATOMIC_SEQ_CST); r1 = y; };
//    auto thread_2_code = [&]() { __atomic_add_fetch(&y, 1, __ATOMIC_SEQ_CST); r2 = x; };

    //auto out_of_order_check_code = [&]() { return r1 == 0 && r2 == 0; };

    /////////////////////////////////////////////////////////////////////////////
    // Code that has two reads in one thread, and two writes in the other thread.
    /////////////////////////////////////////////////////////////////////////////
    auto thread_1_code = [&]() { x = 2; y = 3; };
    auto thread_2_code = [&]() { r1 = y; r2 = x; };
    //auto thread_1_code = [&]() { __atomic_exchange_n(&x, 2, __ATOMIC_SEQ_CST); __atomic_exchange_n(&y, 3, __ATOMIC_SEQ_CST); };
    //auto thread_1_code = [&]() { x = 2; asm volatile(BARRIER_INSTRUCTION ::: "memory"); y = 3; };
    //auto thread_2_code = [&]() { r1 = y; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r2 = x; };

    auto out_of_order_check_code = [&]() { return r1 == 3 && r2 == 0; };

    perform_test(thread_1_code,
                 thread_2_code,
                 out_of_order_check_code,
                 x, y, r1, r2,
                 start_semaphore1, start_semaphore2, end_semaphore1, end_semaphore2,
                 loop_count);
}