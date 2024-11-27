#!/bin/bash

rm out.log

# Run the setup.
./build/test/cppsuite/test_union_fs -f > out.log 2>&1
exit=0
# Loop-de-loop.
while [ $exit -eq 0 ]
do
    echo executing
    # This will continuously grow the log. Switch this back to > once less bugs occur.
    ./build/test/cppsuite/test_union_fs >> out.log 2>&1
    exit=$?
    echo looping $exit
done

echo "==================="
echo "REPROd something! Logs in out.log"
echo "==================="
