#!/bin/bash

input_logs=$1
output_folder=$2
output_folder_imgs=$3

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

python3 wt_ext_parse.py "$input_logs" -o "$output_folder"
python3 fragmentation_visualiser/visualise_fragmentation.py "$output_folder" --output "$output_folder_imgs"
open "$output_folder_imgs"/viewer.html