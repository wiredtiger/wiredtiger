#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# A python script to run the WT tool verify command.
# This script wraps the wt util tool and takes in arguments for the tool,
# handles the output and provides additional processing options on the output
# such as pretty printed output and visualisation options.

# This script only supports Row store and Variable Length Column Store (VLCS).
# Fixed Length Column Store (FLCS) is not supported.

import argparse
import os
import subprocess
import sys
from pathlib import Path


def visualize(data, visualization_type):
    """
    Visualizing data
    """
    pass


def parse_output(output):
    """
    Parse the output file
    """
    return output


def execute_command(command):
    """
    Execute a given command and return its output.
    """
    try:
        result = subprocess.run(command, shell=True, check=True,
                                stdout=subprocess.PIPE,
                                universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}", file=sys.stderr)
        sys.exit(1)
    
    return result.stdout


def find_wt_exec_path():
    """
    Find the path of the WT tool executable. 
    
    We expect to find exactly one wt binary. Otherwise exit and prompt the user to provide an explicit path to the binary
    """
    path = f"{os.path.dirname(os.path.abspath(__file__))}/../."

    try:
        result = subprocess.run(['find', path, '-maxdepth', '2', '-name', 'wt'],
                                stdout=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}", file=sys.stderr)
        sys.exit(1)

    list_of_paths = result.stdout.split('\n')
    # Sanity check to remove empty new lines from find output
    result = [path for path in list_of_paths if path != '']

    if len(result) > 1:
        print("Error: multiple wt executables found. Please provide wt executable path using the -wt flag:")
        for path in result:
            print(path)
        exit(1)
    if len(result) == 0:
        print("Error: wt executable not found. Please provide path using the -wt flag.")
        exit(1)

    return result[0]


def construct_command(args):
    """
    Construct the WiredTiger verify command based on provided arguments.
    """
    if args.wt_exec_path:
        command = f"{args.wt_exec_path} -h {args.home_dir} verify"
    else:
        command = f"{find_wt_exec_path()} -h {args.home_dir} verify"

    if args.unredacted:
        command += " -u"
    if args.keep_tx_ids:
        command += " -t"
    if args.dump_config:
        command += f" -d {args.dump_config}"
    if args.file_name:
        command += f" \"file:{args.file_name}\""
    return command


def main():
    parser = argparse.ArgumentParser(description="Script to run the WiredTiger verify command with specified options.")
    parser.add_argument('-hd', '--home_dir', required=True, help='Path to the WiredTiger database home directory.')
    parser.add_argument('-f', '--file_name', required=True, help='Name of the WiredTiger file to verify.')
    parser.add_argument('-wt', '--wt_exec_path', help='Path of the WT tool executable.')
    parser.add_argument('-o', '--output_file', help='Option to save output in given output file.')
    parser.add_argument('-d', '--dump_config', choices=['dump_pages', 'dump_blocks'], help='Option to specify dump_pages or dump_blocks configuration.')
    parser.add_argument('-t', '--keep_tx_ids', action='store_true', help='Option to keep transaction IDs during verification.')
    parser.add_argument('-u', '--unredacted', action='store_true', help='Option to display unredacted output.')  
    parser.add_argument('-p', '--print_output', action='store_true', default=False, help='Print the output (default is on)')
    parser.add_argument('-v', '--visualize', choices=['page_sizes', 'entries', 'dsk_image_sizes'], help='Type of visualization')

    args = parser.parse_args()

    try:
        command = construct_command(args)
        output = execute_command(command)
    except (RuntimeError, ValueError, TypeError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    parsed_data = parse_output(output)

    if args.output_file:
        try:
            with open(args.output_file, 'w') as file:
                file.write(parsed_data)
            print(f"Output written to {args.output_file}")
        except IOError as e:
            print(f"Failed to write output to file: {e}", file=sys.stderr)
            sys.exit(1)

    if args.print_output:
        print(parsed_data)

    if args.visualize:
        visualize(parsed_data, args.visualize)


if __name__ == "__main__":
    main()
