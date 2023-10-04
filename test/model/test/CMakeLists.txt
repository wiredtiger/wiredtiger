project(model_csuite)

include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

set(c_tests)

define_c_test(
    TARGET test_model_basic
    SOURCES model_basic/main.cpp
    LIBS wiredtiger_model
    DIR_NAME model_basic
    DEPENDS "ENABLE_MODEL" "WT_POSIX"
    CXX
)
