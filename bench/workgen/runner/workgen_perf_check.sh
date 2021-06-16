#! /bin/bash

ERROR=0
OUTPUT=output.log
TEST=many-dhandle-stress.py
echo python3 $TEST 2> $OUTPUT
python3 $TEST 2> $OUTPUT

# Check exceptions
if grep -io "exception" $OUTPUT; then
	echo ERROR
	ERROR=1
fi

# Check if one DROP operation took too long
THRESHOLD=45
VALUE=$(grep -i "cycling idle.*drop" $OUTPUT | awk '{print $9}' | sort -n | tail -1)
if [[ $VALUE -ge $THRESHOLD ]]; then
	echo "ERROR: One drop operation took too long: ${VALUE}s (max: ${THRESHOLD}s)"
	ERROR=1
fi

# Maximum number of READ/INSERT/UPDATE warnings
THRESHOLD=1
VALUE=$(grep -ic "max latency exceeded" $OUTPUT)
if [[ $VALUE -ge $THRESHOLD ]]; then
	echo "ERROR: Too many long operations: $VALUE (max: $THRESHOLD)"
	ERROR=1
fi

if [[ $ERROR -ne 0 ]]; then
	echo FAILED
	exit 1;
fi

echo SUCCESS

