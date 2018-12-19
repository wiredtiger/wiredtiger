#! /usr/bin/env bash
#
# This script is used to list and dump WiredTiger data files ('WiredTiger.wt') under WT_TEST directory.
#
# Supported return values:
#   0: list & dump are successful for all data files
#   1: list is not successful for some data file
#   2: dump is not successful for some table object
#   3: WT_TEST directory does not exist
#   4: Command argument setting is wrong

BUILD_DIR='build_posix'
WT_TEST_DIR='WT_TEST'

if [ $# -gt 1 ]; then
	echo "Usage: $0 [-v]"
	exit 4
fi

[ "$1" == "-v" ] && verbose=true || verbose=false

# Switch to the Git repo toplevel directory
toplevel_dir=$(git rev-parse --show-toplevel)
cd ${toplevel_dir}

# Check the existence of WT_TEST_DIR, firstly under BUILD_DIR, then under current directory
if [ -d "${BUILD_DIR}/${WT_TEST_DIR}" ]; then
        cd ${BUILD_DIR}
elif [ -d "${WT_TEST_DIR}" ]; then
	:	# do nothing
else
	echo "'${WT_TEST_DIR}' directory does not exist, exiting ..."
	exit 3
fi

dirs_include_datafile=$(find ${WT_TEST_DIR} -name WiredTiger.wt | xargs dirname)

# Loop through each data file under the TEST_DIR
for d in ${dirs_include_datafile}
do
	echo ${d}
	tables=$(./wt -h "${d}" list)
	rc=$?

	if [ "$rc" -ne "0" ]; then 
		echo "Failed to list '${d}' directory, exiting ..."
		exit 1
	fi

	# Loop through each table object in the data file
	for t in ${tables}
	do
		echo ${t}
		dump=$(./wt -h ${d} dump ${t})
		rc=$?

		if [ "$rc" -ne "0" ]; then 
			echo "Failed to dump '${t}' table under '${d}' directory, exiting ..."
			exit 2
		fi

		# Print the table dump out if verbose flag is on
		[ "${verbose}" = true ] && echo ${dump}
	done
done

# If reaching here, the testing result is positive
echo -e "\nAll the data files under '${d}' directory are listed and dumped successfully!"
exit 0 
