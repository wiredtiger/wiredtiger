#! /bin/bash

# First argument needs to be the name of the script.

usage_message="Usage: ./create_test.sh [--dir directory] my_test"
if [[ $# -eq 0 || $# -gt 3 ]]; then
    echo $usage_message
    exit 128
elif [[ $1 == "-d" && $# -ne 3 ]]; then
    echo $usage_message
    exit 128
elif [[ $1 == "-d" && $# -eq 1 ]]; then
    echo $usage_message
    exit 128
fi

# If configured, grab the test directory name.
test_dir=""
if [[ $1 == "-d" ]]; then
    echo $1
    test_dir="$2"
    shift ; shift ;
fi

# Check the test name
if [[ $1 =~ ^[a-z][_a-z0-9]+$ ]]; then
    echo "Generating test: $1..."
else
    echo "Invalid test name. Test name $1 must match the regex '[a-z][_a-z0-9]+$'"
    exit 128
fi

# Check if the test already exists. Modify for all recrusive dirs
FILE=tests/$test_dir/$1.cpp
if test -f "$FILE"; then
    echo "$FILE cannot be created as it already exists."
    exit 1
fi

# Copy the default template.
cp tests/test_unit_test_template.cpp "$FILE"
echo "Created $FILE."

# Replace test_template with the new test name.
SEARCH="unit_test_template"
sed -i "s/$SEARCH/$1/" "$FILE"
echo "Updated test name in $FILE."

# Add the new test to the CMakeLists.txt
FILE_NAME="\        tests/$1.cpp"
SEARCH="test_unit_test_template.cpp"
sed -i "/$SEARCH/a $FILE_NAME" CMakeLists.txt

# Trigger s_all
echo "Running s_all.."
cd ../../dist || exit 1
./s_all

# Last changes to be done manually
echo "Follow the next steps to execute your new test:"
echo "1. Start editing $1.cpp"
echo "2. Compile your changes, run the unit test via test/unittest/catch2-unittests [tag_name]"
