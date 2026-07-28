set(WT_POSIX ON CACHE BOOL "")

# Header file here is required for portable futex implementation.
include_directories(AFTER SYSTEM "${CMAKE_SOURCE_DIR}/oss/apple")
