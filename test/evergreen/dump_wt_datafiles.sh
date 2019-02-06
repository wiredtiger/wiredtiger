#! /usr/bin/env bash
#
# This script is used to list and dump WiredTiger data files ('WiredTiger.wt') under WT_TEST directory.
#
# Supported return values:
#   0: list & dump are successful for all data files
#   1: list is not successful for some data file
#   2: dump is not successful for some table object
#   3: 'wt' binary file does not exist
#   4: 'WT_TEST' directory does not exist
#   5: Command argument setting is wrong

if [ $# -gt 1 ]; then
	echo "Usage: $0 [-v]"
	exit 5
fi

[ "$1" == "-v" ] && verbose=true || verbose=false

# Switch to the Git repo toplevel directory
cd $(git rev-parse --show-toplevel)

# Check the existence of 'WT_TEST' directory
wt_test_dir=$(find . -maxdepth 2 -type d -name WT_TEST | head -1)
if [ -z ${wt_test_dir} ]; then
	echo "'WT_TEST' directory does not exist, exiting ..."
	exit 4
fi

# Check the existence of 'wt' binary
wt_binary=$(find . -type f -executable -name wt | grep '.libs' | head -1)
if [ -z ${wt_binary} ]; then
	echo "'wt' file does not exist, exiting ..."
	exit 3
fi
lib_dir=$(dirname ${wt_binary})

# Work out the list of directories that include wt data files
dirs_include_datafile=$(find ${wt_test_dir} -type f -name WiredTiger.wt | xargs dirname)

# Loop through each data file under the TEST_DIR
for d in ${dirs_include_datafile}
do
	echo ${d}
	tables=$(LD_LIBRARY_PATH=${lib_dir} ${wt_binary} -h "${d}" list)
	rc=$?

	if [ "$rc" -ne "0" ]; then 
		echo "Failed to list '${d}' directory, exiting ..."
		exit 1
	fi

	# Loop through each table object in the data file
	for t in ${tables}
	do
		echo ${t}
		dump=$(LD_LIBRARY_PATH=${lib_dir} ${wt_binary} -h ${d} dump ${t})
		rc=$?

		if [ "$rc" -ne "0" ]; then
			# Expect an error when a table hasn't had it's column groups fully created.
			cg_not_created_error="cannot be used until all column groups are created"
			test_cg_not_created=$(cat ${d}/stderr.txt | grep -c "${cg_not_created_error}")

			if [ "$test_cg_not_created" -ne "0" ]; then
				echo "Can't dumped '${t}' under '${d}', its column groups are not" \
				    "created, continuing ..."
				continue
			fi

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
