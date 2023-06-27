#! /usr/bin/env bash
#
# Cycle through a list of test/format configurations that failed previously,
# and run format test against each of those configurations to capture issues.
#

set -u

set -e
# Switch to the Git repo toplevel directory
cd $(git rev-parse --show-toplevel)
# Walk into the test/format directory
cd cmake_build/test/format
set +e

# Check the existence of 't' binary
if [ ! -x "t" ]; then
	echo "'t' binary does not exist, exiting ..."
	exit 1
fi

success=0
failure=0
running=0
# Jobs to execute in parallel (hard coded to 8)
parallel_jobs=8
PID=""
declare -a PID_LIST

# Wait for other format runs.
wait_for_other_format_runs()
{
	for process in ${PID_LIST[@]};do
		# Remove the process id of the completed runs from the process id array.
		unset PID_LIST[0]
		PID_LIST=(${PID_LIST[@]})
		wait $process
		exit_status=$?
		let "running--"
		config_name=`egrep $process $tmp_file | awk -F ":" '{print $2}' | rev | awk -F "/" '{print $1}' | rev`
		if [ $exit_status -ne "0" ]; then
			let "failure++"
			[ -f WT_TEST_${config_name}/CONFIG ] && cat WT_TEST_${config_name}/CONFIG
		else
			let "success++"
			# Remove database files of successful jobs.
			[ -d WT_TEST_${config_name} ] && rm -rf WT_TEST_${config_name}
		fi

		return
	done
}

tmp_file="format_list.txt"
touch $tmp_file
# Cycle through format CONFIGs recorded under the "failure_configs" directory
for config in $(find ../../../test/format/failure_configs/ -name "CONFIG.*" | sort)
do
	echo -e "\nTesting CONFIG $config ...\n"
	basename_config=$(basename $config)
	./t -1 -c $config -h WT_TEST_$basename_config &
	let "running++"

	PID="$!"
	PID_LIST+=("$PID")

	echo "$PID:$config" >> $tmp_file

	if [ ${running} -ge ${parallel_jobs} ]; then
		wait_for_other_format_runs
		sleep 10
	fi
done

wait_for_other_format_runs

echo -e "\nSummary of '$(basename $0)': $success successful CONFIG(s), $failure failed CONFIG(s)\n"

[[ $failure -ne 0 ]] && exit 1
exit 0
