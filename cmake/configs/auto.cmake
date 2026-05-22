include(cmake/helpers.cmake)

### Auto configure options and checks that we can infer from our toolchain environment.

config_lib(
    HAVE_LIBMEMKIND
    "memkind library exists."
    LIB "memkind"
    HEADER "memkind.h"
)

config_lib(
    HAVE_LIBPTHREAD
    "Pthread library exists."
    LIB "pthread"
)

config_lib(
    HAVE_LIBRT
    "rt library exists."
    LIB "rt"
)

config_lib(
    HAVE_LIBDL
    "dl library exists."
    LIB "dl"
)

config_lib(
    HAVE_LIBCXX
    "stdc++ library exists."
    LIB "stdc++"
)

config_lib(
    HAVE_LIBACCEL_CONFIG
    "accel-config library exists."
    LIB "accel-config"
)

config_lib(
    HAVE_LIBLZ4
    "lz4 library exists."
    LIB "lz4"
    HEADER "lz4.h"
)

config_lib(
    HAVE_LIBSNAPPY
    "snappy library exists."
    LIB "snappy"
    HEADER "snappy.h"
)

config_lib(
    HAVE_LIBZ
    "zlib library exists."
    LIB "z"
    HEADER "zlib.h"
)

config_lib(
    HAVE_LIBZSTD
    "zstd library exists."
    LIB "zstd"
    HEADER "zstd.h"
)

config_lib(
    HAVE_LIBQPL
    "qpl library exists."
    LIB "qpl"
    HEADER "qpl/qpl.h"
)

config_lib(
    HAVE_LIBSODIUM
    "sodium library exists."
    LIB "sodium"
    HEADER "sodium.h"
)

if(CMAKE_C_BYTE_ORDER STREQUAL "BIG_ENDIAN")
    set(WORDS_BIGENDIAN TRUE)
endif()
