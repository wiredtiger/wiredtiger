# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/test/cppsuite
# Build directory: /data/work/git/wiredtiger2/build2/test/cppsuite
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[cppsuite]=] "/data/work/git/wiredtiger2/build2/test/cppsuite/run")
set_tests_properties([=[cppsuite]=] PROPERTIES  LABELS "check;cppsuite" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/cppsuite/CMakeLists.txt;79;add_test;/data/work/git/wiredtiger2/test/cppsuite/CMakeLists.txt;0;")
add_test([=[csuite_style_example]=] "/data/work/git/wiredtiger2/build2/test/cppsuite/csuite_style_example_test")
set_tests_properties([=[csuite_style_example]=] PROPERTIES  LABELS "check;cppsuite" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/cppsuite/CMakeLists.txt;81;add_test;/data/work/git/wiredtiger2/test/cppsuite/CMakeLists.txt;0;")
