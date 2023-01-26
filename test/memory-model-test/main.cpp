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

    auto thread_1_code = [&]() { x = 1; r1 = y; };
    auto thread_2_code = [&]() { y = 1; r2 = x; };

//    auto thread_1_code = [&]() { x = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r1 = y; };
//    auto thread_2_code = [&]() { y = 1; asm volatile(BARRIER_INSTRUCTION ::: "memory"); r2 = x; };

//    auto thread_1_code = [&]() { __atomic_add_fetch(&x, 1, __ATOMIC_SEQ_CST); r1 = y; };
//    auto thread_2_code = [&]() { __atomic_add_fetch(&y, 1, __ATOMIC_SEQ_CST); r2 = x; };

    std::thread thread_1([&](){ thread_function("thread_one", start_semaphore1, end_semaphore1, 1, loop_count,thread_1_code); });
    std::thread thread_2([&](){ thread_function("thread_two", start_semaphore2, end_semaphore2, 2, loop_count,thread_2_code); });

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
        if (r1 == 0 && r2 == 0) {
            out_of_order_count ++;
            std::cout << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;
        }

        if (iterations % 100 == 0 && iterations != 0) {
            std::cout << '.' << std::flush;
            if (iterations % 5000 == 0) {
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
