# Additional clean files
cmake_minimum_required(VERSION 3.16)

if("${CONFIG}" STREQUAL "" OR "${CONFIG}" STREQUAL "Debug")
  file(REMOVE_RECURSE
  "bench/workgen/CMakeFiles/workgen.dir/workgenPYTHON_wrap.cxx"
  "bench/workgen/workgen.py"
  "lang/python/CMakeFiles/wiredtiger_python.dir/wiredtigerPYTHON_wrap.c"
  "lang/python/wiredtiger.py"
  )
endif()
