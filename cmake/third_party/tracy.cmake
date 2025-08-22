if(NOT TRACY_ENABLE)
    # We don't need to construct a tracy library target.
    return()
endif()

if(TARGET wt::tracy)
    # Avoid redefining the imported library.
    return()
endif()

# Set Tracy source directory
set(TRACY_SOURCE_DIR "${CMAKE_SOURCE_DIR}/../tracy/public" CACHE PATH "Path to Tracy source code")

if(NOT EXISTS "${TRACY_SOURCE_DIR}/TracyClient.cpp")
    message(FATAL_ERROR "Tracy source not found at ${TRACY_SOURCE_DIR}. Please set TRACY_SOURCE_DIR to point to the Tracy public directory.")
endif()

# Create Tracy library target
add_library(wt_tracy STATIC
    ${TRACY_SOURCE_DIR}/TracyClient.cpp
)

# Set Tracy compile definitions
target_compile_definitions(wt_tracy PUBLIC
    TRACY_ENABLE=1
    $<$<CONFIG:Debug>:TRACY_ON_DEMAND>
)

# Set Tracy include directories
target_include_directories(wt_tracy PUBLIC
    ${TRACY_SOURCE_DIR}
)

# Tracy requires threading support
find_package(Threads REQUIRED)
target_link_libraries(wt_tracy PRIVATE Threads::Threads)

# Platform-specific libraries for Tracy
if(UNIX AND NOT APPLE)
    target_link_libraries(wt_tracy PRIVATE dl)
endif()

# Set compile options for Tracy
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(wt_tracy PRIVATE
        -Wall
        -Wextra
        -Wno-unused-parameter
        -Wno-unused-variable
        -Wno-missing-field-initializers
        -Wno-aggregate-return
    )
endif()

# Create alias target for consistency with other libraries
add_library(wt::tracy ALIAS wt_tracy)

# Make Tracy available to the rest of the build system
set_target_properties(wt_tracy PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)
