#!/bin/bash

# wtperf_run.sh - Reduce variance in wtperf measurements.
#
# Performance tests have a large variability from run to run, so we use this 
# script to run each wtperf test several times. The script runs a user-defined 
# number of times, drops the min and max values from each run, and reports the 
# average of the remaining values to a results_$TESTNAME.txt file in the current 
# directory. 
# This script should be run from the same directory containing the wtperf binary.
#
# Usage:
# 	wtperf_run.sh test_conf numruns [wtperf_args]
# 		test_conf:   The path the the wtperf configuration
# 		rumruns:     How many times to run the test
# 		wtperf_args: Additional arguments to pass to wtperf. 
# 					 If NOCREATE is provided the test uses the same DB for 
# 					 all test runs. If provided it must be the first argument.
# Example:
# 	../../../bench/wtperf/runners/wtperf_run.sh ../../../bench/wtperf/runners/checkpoint-stress.wtperf 1
#
if test "$#" -lt "2"; then
	echo "Must specify wtperf test to run and number of runs"
	exit 1
fi
wttest=$1
shift # Consume this arg
runmax=$1
shift # Consume this arg

wtarg=""
create=1
while [[ $# -gt 0 ]] ; do
	if test "$1" == "NOCREATE"; then
		create=0
	else
		wtarg+=" $1"
	fi
	shift # Consume this arg
done

home=./WT_TEST
logfile=./log_wtperf_run.txt
resultsfile=./results_$(basename "$wttest").txt
rm -f "$resultsfile"
rm -f $logfile
echo "Parsed $# args: test: $wttest runmax: $runmax args: $wtarg" >> $logfile

# Each of these has an entry for each op in ops below.
avg=(0 0 0 0 0 0)
max=(0 0 0 0 0 0)
min=(0 0 0 0 0 0)
sum=(0 0 0 0 0 0)
# Load needs floating point and bc, handle separately.
loadindex=7
avg[$loadindex]=0
max[$loadindex]=0
min[$loadindex]=0
sum[$loadindex]=0
ops=(insert modify read truncate update checkpoint)
outp=("Insert count:" "Modify count:" "Read count:" "Truncate count:" "Update count:" "Checkpoint count:" )
outp[$loadindex]="Load time:"

# getval min/max val cur
# Returns the minimum or maximum of val and cur.
# min == 0, max == 1.
getval()
{
	get_max="$1"
	val="$2"
	cur="$3"
	ret=$cur
	echo "getval: get_max $get_max val $val cur $cur" >> $logfile
	if test "$get_max" -eq "1"; then
		if test "$val" -gt "$cur"; then
			ret=$val
		fi
	elif test "$val" -lt "$cur"; then
			ret=$val
	fi
	echo "$ret"
}

isstable()
{
	min_val="$1"
	max_val="$2"
	tmp=$(echo "scale=3; $min_val * 1.03" | bc)
	if (($(bc <<< "$tmp < $max_val") )); then
		ret=0
	else
		ret=1
	fi
	echo "$ret"
}

getmin=0
getmax=1
run=1
while test "$run" -le "$runmax"; do
	if test "$create" -eq "1"; then
		rm -rf $home
		mkdir $home
	fi
	LD_PRELOAD=/usr/local/lib/libtcmalloc.so LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib ./wtperf -O "$wttest" "$wtarg"
	if test "$?" -ne "0"; then
		exit 1
	fi

	# Copy the artifacts from the run
	backup_dir=${home}_$(basename "$wttest")_${run}_$(date +"%s")
	rsync -r -m --include="*Stat*" --include="CONFIG.wtperf" --include="*monitor" --include="latency*" --include="test.stat" --exclude="*" $home/ "$backup_dir"

	# Load is always using floating point, so handle separately
	l=$(grep "^Load time:" ./WT_TEST/test.stat)
	if test "$?" -eq "0"; then
		load=$(echo "$l" | cut -d ' ' -f 3)
	else
		load=0
	fi
	cur[$loadindex]=$load
	sum[$loadindex]=$(echo "${sum[$loadindex]} + $load" | bc)
	echo "cur ${cur[$loadindex]} sum ${sum[$loadindex]}" >> $logfile
	for i in ${!ops[*]}; do
		l=$(grep "Executed.*${ops[$i]} operations" ./WT_TEST/test.stat)
		if test "$?" -eq "0"; then
			n=$(echo "$l" | cut -d ' ' -f 2)
		else
			n=0
		fi
		cur[$i]=$n
		sum[$i]=$((n + sum[i]))
	done
	#
	# Keep running track of min and max for each operation type.
	#
	if test "$run" -eq "1"; then
		for i in ${!cur[*]}; do
			min[$i]=${cur[$i]}
			max[$i]=${cur[$i]}
		done
	else
		for i in ${!cur[*]}; do
			if test "$i" -eq "$loadindex"; then
				if (($(bc <<< "${cur[$i]} < ${min[$i]}") )); then
					min[$i]=${cur[$i]}
				fi
				if (($(bc <<< "${cur[$i]} > ${max[$i]}") )); then
					max[$i]=${cur[$i]}
				fi
			else
				min[$i]=$(getval $getmin ${cur[$i]} ${min[$i]})
				max[$i]=$(getval $getmax ${cur[$i]} ${max[$i]})
			fi
		done
	fi
	#
	# After 3 runs see if this is a very stable test.  If so, we
	# can skip the remaining tests and just use these values.  We
	# define "very stable" to be that the min and max are within
	# 3% of each other.
	if test "$run" -eq "3"; then
		# Only if all values are stable, we can break.
		unstable=0
		for i in ${!min[*]}; do
			stable=$(isstable "${min[$i]}" "${max[$i]}")
			if test "$stable" -eq "0"; then
				unstable=1
				break
			fi
		done
		if test "$unstable" -eq "0"; then
			break
		fi
	fi
	run=$((run + 1))
done

skipminmax=0
if test "$runmax" -le "2"; then
	numruns=$(getval $getmin $run "$runmax")
	skipminmax=1
elif test "$run" -le "$runmax"; then
	numruns=$((run - 2))
else
	numruns=$((runmax - 2))
fi
if test "$numruns" -eq "0"; then
	numruns=1
fi
#
# The sum contains all runs.  Subtract out the min/max values.
# Average the remaining and write it out to the file.
#
for i in ${!min[*]}; do
	if test "$i" -eq "$loadindex"; then
		if test "$skipminmax" -eq "0"; then
			s=$(echo "scale=3; ${sum[$i]} - ${min[$i]} - ${max[$i]}" | bc)
		else
			s=${sum[$i]}
		fi
		avg[$i]=$(echo "scale=3; $s / $numruns" | bc)
	else
		if test "$skipminmax" -eq "0"; then
			s=$((sum[i] - min[i] - max[i]))
		else
			s=${sum[$i]}
		fi
		avg[$i]=$((s / numruns))
	fi
done
for i in ${!outp[*]}; do
	echo "${outp[$i]} ${avg[$i]}" >> $logfile
	if [[ $i -lt $loadindex && "${avg[$i]}" -ne "0" ]]; then
		echo "${outp[$i]} ${avg[$i]}" >> "$resultsfile"
	fi
done
exit 0
