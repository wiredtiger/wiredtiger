cmake_minimum_required(VERSION 3.16)
project(memory_model_test)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

add_executable(memory_model_test memory_model_test.cpp basic_semaphore.h)
target_link_libraries(memory_model_test Threads::Threads)
target_compile_options(memory_model_test PRIVATE -O2 -Wall -Werror)
