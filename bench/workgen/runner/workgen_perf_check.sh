#! /bin/bash
#
# Checks warnings and exceptions raised by a given workgen test as well as lantencies of specific operations.
# This script is used in evergreen to assess performance using workgen.
#

usage () {
    cat << EOF
Usage: $0 test_name threshold_1 threshold_2 threshold_3
Arguments:
    test_name    # Test to run
    threshold_1  # Maximum allowed warnings from DROP operations.
    threshold_2  # Maximum time in seconds a DROP operation can take.
    threshold_3  # Maximum allowed warnings from CREATE/INSERT/UPDATE operations.
EOF
}

if [ $1 == "-h" ]; then
    usage
    exit
fi

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    usage
    exit 1
fi

ERROR=0
OUTPUT=output.log
echo "python3 $1 2> $OUTPUT"
python3 $1 2> $OUTPUT

# Check exceptions
if grep -io "exception" $OUTPUT; then
    echo ERROR
    ERROR=1
fi

# Maximum number of DROP warnings
THRESHOLD=$2
VALUE=$(grep -ic "cycling idle.*drop" $OUTPUT)
if [[ $VALUE -ge $THRESHOLD ]]; then
    echo "ERROR: Too many long DROP operations: $VALUE (max allowed: $THRESHOLD)"
    ERROR=1
fi

# Check if a DROP operation took too long
THRESHOLD=$3
VALUE=$(grep -i "cycling idle.*drop" $OUTPUT | awk '{print $9}' | sort -n | tail -1)
if [[ $VALUE -ge $THRESHOLD ]]; then
    echo "ERROR: A drop operation took too long: ${VALUE}s (max allowed: ${THRESHOLD}s)"
    ERROR=1
fi

# Maximum number of READ/INSERT/UPDATE warnings
THRESHOLD=$4
VALUE=$(grep -ic "max latency exceeded" $OUTPUT)
if [[ $VALUE -ge $THRESHOLD ]]; then
    echo "ERROR: Too many long read/insert/update operations: $VALUE (max allowed: $THRESHOLD)"
    ERROR=1
fi

if [[ $ERROR -ne 0 ]]; then
    echo FAILED
    exit 1
fi

echo SUCCESS
