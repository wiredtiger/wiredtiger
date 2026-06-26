# CMake generated Testfile for 
# Source directory: /data/work/git/wiredtiger2/lang/python
# Build directory: /data/work/git/wiredtiger2/build2/lang/python
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[test_ex_access]=] "/opt/mongodbtoolchain/revisions/8695910c32ef0ee5eecaae4c9ca515b4b6436a40/stow/cmake-v5.jbN/bin/cmake" "-E" "env" "PYTHONPATH=/data/work/git/wiredtiger2/build2/lang/python" "/opt/mongodbtoolchain/v5/bin/python3.10" "-S" "/data/work/git/wiredtiger2/examples/python/ex_access.py")
set_tests_properties([=[test_ex_access]=] PROPERTIES  LABELS "check" _BACKTRACE_TRIPLES "/data/work/git/wiredtiger2/lang/python/CMakeLists.txt;153;add_test;/data/work/git/wiredtiger2/lang/python/CMakeLists.txt;0;")
