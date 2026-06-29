# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/test/cursor_order
# Build directory: /data/work/git/wiredtiger2/build2/test/cursor_order
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_cursor_order_row]=] "/data/work/git/wiredtiger2/build2/test/cursor_order/test_cursor_order" "-tr")
set_tests_properties([=[test_cursor_order_row]=] PROPERTIES  LABELS "check;test_cursor_order" WORKING_DIRECTORY "/data/work/git/wiredtiger2/build2/test/cursor_order/test_cursor_order_row_test_dir" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/ctest_helpers.cmake;319;add_test;/data/work/git/wiredtiger2/test/cursor_order/CMakeLists.txt;8;define_test_variants;/data/work/git/wiredtiger2/test/cursor_order/CMakeLists.txt;0;")
add_test([=[test_cursor_order_var]=] "/data/work/git/wiredtiger2/build2/test/cursor_order/test_cursor_order" "-tv")
set_tests_properties([=[test_cursor_order_var]=] PROPERTIES  LABELS "check;test_cursor_order" WORKING_DIRECTORY "/data/work/git/wiredtiger2/build2/test/cursor_order/test_cursor_order_var_test_dir" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/ctest_helpers.cmake;319;add_test;/data/work/git/wiredtiger2/test/cursor_order/CMakeLists.txt;8;define_test_variants;/data/work/git/wiredtiger2/test/cursor_order/CMakeLists.txt;0;")
