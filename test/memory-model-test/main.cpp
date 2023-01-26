#include <iostream>
#include <semaphore>
#include <thread>
#include <chrono>
#include <random>


template<typename code>
void thread_function(std::string const& thread_name,
                     std::binary_semaphore& start_semaphore,
                     std::binary_semaphore& end_semaphore,
                     int rng_seed,
                     code code_param) {
    std::mt19937 rng(rng_seed);
    std::cout << "starting thread_function for thread: " << thread_name << std::endl;
    for(;;) {
        start_semaphore.acquire();
        while(rng() % 8 != 0) {};         // Short random delay
        code_param();
        end_semaphore.release();
    }
    std::cout << "ending thread_function for for thread: " << thread_name << std::endl;
}


int main() {
    std::cout << "Jeremy's Memory Model Test" << std::endl;

    int x = 0;
    int y = 0;
    int r1 = 0;
    int r2 = 0;

    std::binary_semaphore start_semaphore1{0};
    std::binary_semaphore start_semaphore2{0};
    std::binary_semaphore end_semaphore1{0};
    std::binary_semaphore end_semaphore2{0};

    auto thread_1_code = [&]() { x = 1; r1 = y; };
    auto thread_2_code = [&]() { y = 1; r2 = x; };

//    auto thread_1_code = [&]() { x = 1; asm volatile("dmb ish" ::: "memory"); r1 = y; };
//    auto thread_2_code = [&]() { y = 1; asm volatile("dmb ish" ::: "memory"); r2 = x; };

//    auto thread_1_code = [&]() { __atomic_add_fetch(&x, 1, __ATOMIC_SEQ_CST); r1 = y; };
//    auto thread_2_code = [&]() { __atomic_add_fetch(&y, 1, __ATOMIC_SEQ_CST); r2 = x; };

    std::thread thread_1([&](){ thread_function("thread_one", start_semaphore1, end_semaphore1, 1, thread_1_code); });
    std::thread thread_2([&](){ thread_function("thread_two", start_semaphore2, end_semaphore2, 2, thread_2_code); });

    int iterations = 0;
    int out_of_order_count = 0;

    for (iterations = 1; iterations < 1000000; iterations++) {
        x = 0;
        y = 0;
        r1 = 0;
        r2 = 0;

        using namespace std::literals;
        start_semaphore1.release();
        start_semaphore2.release();

        end_semaphore1.acquire();
        end_semaphore2.acquire();
        if (r1 == 0 && r2 == 0) {
            out_of_order_count ++;
            std::cout << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;
        }

        if (iterations % 100 == 0) {
            std::cout << '.' << std::flush;
            if (iterations % 5000 == 0) {
                std::cout << std::endl;
            }
        }
    }

    std::cout << std::endl;
    std::cout << "Total of " << out_of_order_count << " out of orders detected out of " << iterations << " iterations (" << 100.0f * double(out_of_order_count) / double(iterations) << "%)" << std::endl;

    start_semaphore1.release();
    start_semaphore2.release();
    end_semaphore1.release();
    end_semaphore2.release();
    thread_1.join();
    thread_2.join();

    std::cout << "at the end, x = " << x << std::endl;
    std::cout << "at the end, y = " << y << std::endl;

    return 0;
}
