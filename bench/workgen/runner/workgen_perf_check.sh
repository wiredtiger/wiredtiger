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

# Maximum number of CREATE/DROP warnings
THRESHOLDS=(1 1)
OPERATIONS=("create" "drop")
for ((i = 0; i < ${#OPERATIONS[@]}; ++i)); do
	VALUE=$(grep -ic "cycling idle.*${OPERATIONS[$i]}" $OUTPUT)
	if [[ $VALUE -ge ${THRESHOLDS[$i]} ]]; then
		echo "ERROR: Too many long ${OPERATIONS[$i]} operations: $VALUE (max: ${THRESHOLDS[$i]})"
		ERROR=1
	fi
done

# Check if one CREATE/DROP operation took too long
# Trehsholds in s
THRESHOLDS=(1 1)
for ((i = 0; i < ${#OPERATIONS[@]}; ++i)); do
	VALUE=$(grep -i "cycling idle.*${OPERATIONS[$i]}" $OUTPUT | awk '{print $9}' | sort -n | tail -1)
	if [[ $VALUE -ge ${THRESHOLDS[$i]} ]]; then
		echo "ERROR: One ${OPERATIONS[$i]} operation took too long: ${VALUE}s (max: ${THRESHOLDS[$i]}s)"
		ERROR=1
	fi
done

# Maximum number of READ/INSERT/UPDATE warnings
THRESHOLDS=(1 1 1)
OPERATIONS=("read" "insert" "update")
for ((i = 0; i < ${#OPERATIONS[@]}; ++i)); do
	VALUE=$(grep -ic "max latency exceeded.*${OPERATIONS[$i]}" $OUTPUT)
	if [[ $VALUE -ge ${THRESHOLDS[$i]} ]]; then
		echo "ERROR: Too many long ${OPERATIONS[$i]} operations: $VALUE (max: ${THRESHOLDS[$i]})"
		ERROR=1
	fi
done

# Check if one READ/INSERT/UPDATE operation took too long
# Trehsholds in us
THRESHOLDS=(1 1 1)
for ((i = 0; i < ${#OPERATIONS[@]}; ++i)); do
	VALUE=$(grep -i "max latency exceeded.*${OPERATIONS[$i]}" $OUTPUT | awk '{print $12}' | sort -n | tail -1)
	if [[ $VALUE -ge ${THRESHOLDS[$i]} ]]; then
		echo "ERROR: One ${OPERATIONS[$i]} operation took too long: ${VALUE}us (max: ${THRESHOLDS[$i]}us)"
		ERROR=1
	fi
done

if [[ $ERROR -ne 0 ]]; then
	echo FAILED
	exit 1;
fi

echo SUCCESS
