#!/bin/bash

input_logs=$1
output_folder=$2
output_folder_imgs=$3
extent_type=$4

if [ -z "$input_logs" ]; then
    echo "Error: input_logs argument is missing."
    exit 1
fi

if [ -z "$output_folder" ]; then
    echo "Error: output_folder argument is missing."
    exit 1
fi

if [ -z "$output_folder_imgs" ]; then
    echo "Error: output_folder_imgs argument is missing."
    exit 1
fi

if [ -z "$extent_type" ]; then
    echo "Error: extent_type argument is missing. Must be 'all', 'avail', or 'alloc'."
    exit 1
fi

if [[ "$extent_type" != "all" && "$extent_type" != "avail" && "$extent_type" != "alloc" ]]; then
    echo "Error: Invalid extent_type argument. Must be 'all', 'avail', or 'alloc'."
    exit 1
fi

python3 wt_ext_parse.py "$input_logs" -o "$output_folder" -e "$extent_type"
if [ $? -ne 0 ]; then
    echo "Error: wt_ext_parse.py failed."
    exit 1
fi

python3 fragmentation_visualiser/visualise_fragmentation.py "$output_folder" --output "$output_folder_imgs"
if [ $? -ne 0 ]; then
    echo "Error: fragmentation_visualiser/visualise_fragmentation.py failed."
    exit 1
fi

open "$output_folder_imgs"/viewer.html