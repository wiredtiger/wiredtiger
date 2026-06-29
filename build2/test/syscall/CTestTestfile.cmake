# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/test/syscall
# Build directory: /data/work/git/wiredtiger2/build2/test/syscall
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_syscall]=] "python3" "/data/work/git/wiredtiger2/build2/test/syscall/syscall.py")
set_tests_properties([=[test_syscall]=] PROPERTIES  SKIP_RETURN_CODE "3" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/test/syscall/CMakeLists.txt;11;add_test;/data/work/git/wiredtiger2/test/syscall/CMakeLists.txt;0;")
