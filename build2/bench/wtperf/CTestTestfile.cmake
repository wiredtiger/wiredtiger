# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/bench/wtperf
# Build directory: /data/work/git/wiredtiger2/build2/bench/wtperf
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[wtperf_small_btree]=] "/data/work/git/wiredtiger2/build2/bench/wtperf/wtperf" "-O" "/data/work/git/wiredtiger2/bench/wtperf/runners/small-btree.wtperf" "-o" "run_time=20")
set_tests_properties([=[wtperf_small_btree]=] PROPERTIES  LABELS "check;wtperf" WORKING_DIRECTORY "/data/work/git/wiredtiger2/build2/bench/wtperf/wtperf_small_btree_test_dir" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/ctest_helpers.cmake;319;add_test;/data/work/git/wiredtiger2/bench/wtperf/CMakeLists.txt;29;define_test_variants;/data/work/git/wiredtiger2/bench/wtperf/CMakeLists.txt;0;")
