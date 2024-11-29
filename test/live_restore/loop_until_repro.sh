#!/bin/bash

rm out.log

# Run the setup.
echo "Starting a fresh live restore run!"
./test/cppsuite/test_live_restore -f > out.log 2>&1
exit=0
it_count=1
# Loop-de-loop.
while [ $exit -eq 0 ]
do
    echo "Executing subsequent run:" $it_count
    # This will continuously grow the log. Switch this back to > once less bugs occur.
    ./test/cppsuite/test_live_restore >> out.log 2>&1
    exit=$?
    # If a count is supplied and we've reached it, exit.
    if [ ! -z "$1" ] && [ $it_count -eq $1 ]; then
        break
    fi

    ((it_count++))
done

if [ $exit -ne 0 ]; then
    echo "==================="
    echo "REPRO'd something! Exit code is: " $exit " Logs in out.log"
    echo "==================="
else
    echo "Script finished, no errors encountered!"
fi