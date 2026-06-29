# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/test/packing
# Build directory: /data/work/git/wiredtiger2/build2/test/packing
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_packing]=] "/data/work/git/wiredtiger2/build2/test/packing/packing-test")
set_tests_properties([=[test_packing]=] PROPERTIES  LABELS "check" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;20;add_test;/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;0;")
add_test([=[test_intpack]=] "/data/work/git/wiredtiger2/build2/test/packing/intpack-test3")
set_tests_properties([=[test_intpack]=] PROPERTIES  _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;21;add_test;/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;0;")
add_test([=[test_int4bpack]=] "/data/work/git/wiredtiger2/build2/test/packing/int4bpack-test")
set_tests_properties([=[test_int4bpack]=] PROPERTIES  _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;22;add_test;/data/work/git/wiredtiger2/test/packing/CMakeLists.txt;0;")
