
find_package(CURL REQUIRED) 
cmake_minimum_required(VERSION 3.12)

# Create a library called "Hello" which includes the source file "hello.cxx".
# The extension is already found. Any number of sources could be listed here.
# add_executable(hello hello.cxx)

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
target_link_libraries(hello Azure::azure-storage-blobs)