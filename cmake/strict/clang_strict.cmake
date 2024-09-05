include(cmake/strict/strict_flags_helpers.cmake)

# Get common CLANG flags.
set(clang_flags)
get_clang_base_flags(clang_flags C)

# Specific C flags:
list(APPEND clang_flags "-Weverything")

# In code coverage builds inline functions may not be inlined, which can result in additional
# unused copies of those functions, so the unused-function warning much be turned off.
if(CODE_COVERAGE_MEASUREMENT)
    list(APPEND clang_flags "-Wno-unused-function")
endif ()

if(CODE_STATIC_CHECK)
    #set(CMAKE_C_COMPILE_OBJECT "<CMAKE_CXX_COMPILER> -cc1 <DEFINES> <INCLUDES> <FLAGS> -o <OBJECT> -c <SOURCE>")
    #list(INSERT flags 0 "-cc1")
    #list(APPEND clang_flags -fmacro-backtrace-limit=0 -ferror-limit=1000 -Wno-unused-function -fno-inline -fstandalone-debug -O0)
    list(REMOVE_ITEM clang_flags -Werror)
    list(REMOVE_ITEM clang_flags -Weverything)
    list(APPEND clang_flags -Wno-unused-value -Wno-parentheses-equality -save-temps=obj)
    #set(clang_flags "")
endif ()

# Set our common compiler flags that can be used by the rest of our build.
set(COMPILER_DIAGNOSTIC_C_FLAGS ${clang_flags})
