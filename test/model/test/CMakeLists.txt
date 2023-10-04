project(model_csuite)

include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

create_test_executable(test_model_basic
    SOURCES model_basic/main.cpp
    LIBS wiredtiger_model
    CXX
)
