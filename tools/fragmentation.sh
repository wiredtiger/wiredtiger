#!/bin/bash

input_logs=$1
output_folder=$2
output_folder_imgs=$3
extent_type=${4:-alloc} # Default to "alloc" if $4 is not provided
filename_filter=${5:-""} # Default to an empty string if $5 is not provided

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

# Validate extent_type even if it's the default or user-provided
if [[ "$extent_type" != "all" && "$extent_type" != "avail" && "$extent_type" != "alloc" ]]; then
    echo "Error: Invalid extent_type argument. Must be 'all', 'avail', or 'alloc'."
    exit 1
fi

# Construct arguments for wt_ext_parse.py
parse_args=("$input_logs" -o "$output_folder" -e "$extent_type")
if [ -n "$filename_filter" ]; then
    parse_args+=(-f "$filename_filter")
fi

python3 wt_ext_parse.py "${parse_args[@]}"
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