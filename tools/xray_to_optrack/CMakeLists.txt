project(xray_to_optrack CXX)

if("${CMAKE_VERSION}" VERSION_LESS "3.13.4")
    message(STATUS "Skipping xray_to_optrack. LLVM CMake package requires CMake 3.13.4 minimum.")
    return()
endif()

# We require our Clang version >= 8 to use the xray feature.
if(NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "^(Apple)?[Cc]lang")
    message(STATUS "Skipping xray_to_optrack. Requires Clang toolchain.")
    return()
endif()
if(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS "8")
    message(STATUS "Skipping xray_to_optrack. Requires Clang >= 8, found Clang ${CMAKE_CXX_COMPILER_VERSION}")
    return()
endif()

# Find the LLVM package, preferencing the newest/highest package version.
SET(CMAKE_FIND_PACKAGE_SORT_ORDER NATURAL)
SET(CMAKE_FIND_PACKAGE_SORT_DIRECTION DEC)
find_package(LLVM CONFIG)

if(NOT LLVM_FOUND)
    message(STATUS "Skipping xray_to_optrack. LLVM CMake package not found.")
    return()
endif()
# We require at least LLVM 8 for linking against xray.
if(${LLVM_PACKAGE_VERSION} VERSION_LESS "8")
    message(STATUS "Skipping xray_to_optrack. Requires LLVM >= 8, found LLVM ${LLVM_PACKAGE_VERSION}")
    return()
endif()

llvm_map_components_to_libnames(llvm_libs support core symbolize xray)

add_executable(xray_to_optrack ${CMAKE_CURRENT_SOURCE_DIR}/xray_to_optrack.cxx)
target_include_directories(xray_to_optrack PRIVATE ${LLVM_INCLUDE_DIRS})
target_compile_definitions(xray_to_optrack PRIVATE ${LLVM_DEFINITIONS})
target_link_libraries(xray_to_optrack PRIVATE ${llvm_libs})
