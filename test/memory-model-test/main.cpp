#include <iostream>
#include <semaphore>
#include <thread>
#include <chrono>
#include <random>


//template<typename code>
//class thread_info {
//public:
//    thread_info(std::string const& thread_name, std::counting_semaphore<1>& semaphore, code code_param)
//      : _thread_name(thread_name), _semaphore(semaphore), _code(code_param)
//    {};
//    [[nodiscard]] std::string const& get_thread_name() const { return _thread_name; };
//    [[nodiscard]] code const& get_code() const { return _code; };
//private:
//    std::string const& _thread_name;
//    std::counting_semaphore<1>& _semaphore;
//    code _code;
//};


//template<typename code>
//void thread_function(thread_info<code> thread_info_param) {
//    using namespace std::literals;
//    std::cout << "starting thread_function for thread: " << thread_info_param.get_thread_name() << std::endl;
//    thread_info_param.get_code()();
//    std::this_thread::sleep_for(100ms);
//    std::cout << "ending thread_function for thread: " << thread_info_param.get_thread_name() << std::endl;
//}

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

    //auto thread_1_code = [&]() { x = 1; r1 = y; };
    //auto thread_2_code = [&]() { y = 1; r2 = x; };

    auto thread_1_code = [&]() { x = 1; r1 = y; };
    auto thread_2_code = [&]() { y = 1; r2 = x; };

//    auto thread_1_code = [&]() { x = 1; asm volatile("dmb ish" ::: "memory"); r1 = y; };
//    auto thread_2_code = [&]() { y = 1; asm volatile("dmb ish" ::: "memory"); r2 = x; };

    std::thread thread_1([&](){ thread_function("thread_one", start_semaphore1, end_semaphore1, 1, thread_1_code); });
    std::thread thread_2([&](){ thread_function("thread_two", start_semaphore2, end_semaphore2, 2, thread_2_code); });

    int out_of_order_count = 0;

    for (int i = 1; i < 1000000; i++) {
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
            std::cout << out_of_order_count << " out of orders detected out of " << i << " iterations" << std::endl;
        }

        if (i % 100 == 0) {
            std::cout << '.' << std::flush;
            if (i % 5000 == 0) {
                std::cout << std::endl;
            }
        }
    }

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
