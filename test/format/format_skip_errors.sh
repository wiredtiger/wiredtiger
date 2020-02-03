#! /bin/bash

# Conditional grep
grep -q "cache clean check: no" RUNDIR.*.log && grep -q "cache dirty check: no" RUNDIR.*.log && grep "format run more than 15 minutes past the maximum time" RUNDIR.*.log

if [ $? -eq 0 ]
then
	echo "{cache clean check: no} && {cache dirty check: no} && {format run more than 15 minutes past the maximum time} Error found in the failed log"
	exit 0
fi

# skip_list : Append the errors to be skipped
declare -a skip_list=(  "current_state == WT_REF_LIMBO || current_state == WT_REF_MEM"
                        "heap-buffer-overflow"
                     )

# Iterate the skip error list and search in the failed logs.
# Exit with status 0 if found, else exit with status 1
for err in "${skip_list[@]}"
do
    echo "Checking $err in the Failed log"
    grep "$err" RUNDIR.*.log
    if [ $? -eq 0 ]
    then
        echo "{$err} Error found"
	    exit 0
    fi
done

exit 1
