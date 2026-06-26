# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/test/huge
# Build directory: /data/work/git/wiredtiger2/build2/test/huge
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_huge_small]=] "/data/work/git/wiredtiger2/build2/test/huge/test_huge" "-s")
set_tests_properties([=[test_huge_small]=] PROPERTIES  LABELS "check" WORKING_DIRECTORY "/data/work/git/wiredtiger2/build2/test/huge/test_huge_small_test_dir" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/ctest_helpers.cmake;319;add_test;/data/work/git/wiredtiger2/test/huge/CMakeLists.txt;6;define_test_variants;/data/work/git/wiredtiger2/test/huge/CMakeLists.txt;0;")
