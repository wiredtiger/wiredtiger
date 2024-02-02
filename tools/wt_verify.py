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
import json
import sys
import matplotlib.pyplot as plt

def print_output(output):
    """
    Print output for parsing script
    """
    str = "{"
    for checkpoint in output:
        str += "\n\t" + checkpoint + ": {"
        for count, key in enumerate(output[checkpoint]):
            value = output[checkpoint][key]
            if count:
                str += ", "
            str += "\n\t\t" + key + ": {\n\t\t\t\"metadata\": " + json.dumps(value["metadata"]) 
            if "data" in value:
                str += "\n\t\t\t\"data\": " + json.dumps(value["data"]) 
            if "hs_modify" in value:
                str += "\n\t\t\t\"hs_modify\": " + json.dumps(value["hs_modify"]) 
            if "hs_update" in value:
                str += "\n\t\t\t\"hs_update\": " + json.dumps(value["hs_update"]) 
            str += "\n\t\t}"
        str += "\n\t}"
    str += "\n}"
    print(str)


def is_int(string):
    """
    Check if string can be converted to an integer
    """
    if isinstance(string, str):
        if string.isdigit():
            return int(string)
    return string


def string_to_iterable(line):
    """
    Helper that converts string to dictionaries and / or lists
    """
    dict = {}
    for x in line.split(" | "):
        kv_pair = x.split(": ", 1)
        if kv_pair[1][0] == "[" and kv_pair[1][-1] == "]":
            kv_pair[1] = kv_pair[1][1:-1].split(", ")
            kv_pair[1] = list(map(lambda n: is_int(n), kv_pair[1]))
        if kv_pair[0] == "addr": 
            temp = kv_pair[1][0].split(": ")
            dict.update({"object_id": is_int(temp[0]), "offset_range": temp[1], "size": is_int(kv_pair[1][1]), 
                         "checksum": is_int(kv_pair[1][2])})
        else:
            dict[kv_pair[0]] = is_int(kv_pair[1])
    return dict


def parse_output(file_path):
    """
    Parse the output file of dump_pages
    """
    separator = "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n"
    f = open(file_path, "r")
    output = {}
    checkpoint = {}
    line = f.readline() # separator
    cur_node = {"metadata": {}}
    cur_node_id = None
    page_type = None

    while line:
        if line == separator:
            line = f.readline() # checkpoint
            checkpoint_name = line.split(", ")[-1].split(": ")[-1][:-1]
            output[checkpoint_name] = checkpoint
        while line != separator and line:
            if line[0:2] == "- ": # start of a new node
                line = line[len("- "):-1]
                page_info = [line.split(": ")[0]] + line.split(": ")[1].split()
                page_type = page_info[-1]
                if page_info[0] == "Root":
                    cur_node_id = page_info[0]
                    cur_node["metadata"]["store_type"] = page_info[1]
                elif page_type == "internal" or page_type == "leaf":
                    checkpoint[cur_node_id] = cur_node 
                    cur_node_id = is_int(line.split(": ")[0])
                    cur_node = {"metadata": {}}
                else:
                    pass
            elif line[0:3] == "\t> ": # metadata for new node
                cur_node["metadata"].update(string_to_iterable(line[len("\t> "):-1]))
            line = f.readline() 
        if cur_node_id is not None:
            checkpoint[cur_node_id] = cur_node
    f.close()
    return output

def visualize(data, visualization_type):
    """
    Visualizing data
    """
    pass


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
    wiredtiger_root_dir = f"{os.path.dirname(os.path.abspath(__file__))}/../."

    try:
        result = subprocess.run(['find', wiredtiger_root_dir, '-maxdepth', '2', '-name', 'wt'],
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
        command = f"{args.wt_exec_path}"
    else:
        command = f"{find_wt_exec_path()}"

    command += f" -h {args.home_dir} verify -t"
    if args.dump:
        command += f" -d {args.dump}"
    if args.file_name:
        command += f" \"{args.file_name}\""
    return command


def main():
    parser = argparse.ArgumentParser(description="Script to run the WiredTiger verify command with specified options.")
    parser.add_argument('-hd', '--home_dir', default='.', help='Path to the WiredTiger database home directory (default is current directory).')
    parser.add_argument('-f', '--file_name', required=True, help='Name of the WiredTiger file to verify (such as file:foo.wt).')
    parser.add_argument('-wt', '--wt_exec_path', help='Path of the WT tool executable.')
    parser.add_argument('-o', '--output_file', help='Optionally save output to the provided output file.')
    parser.add_argument('-d', '--dump', required=True, choices=['dump_pages'], help='Option to specify dump_pages configuration.')
    parser.add_argument('-p', '--print_output', action='store_true', default=False, help='Print the output to stdout (default is off)')
    parser.add_argument('-v', '--visualize', choices=['page_sizes', 'entries', 'dsk_image_sizes'], nargs='+',
                        help='Type of visualization (multiple options allowed).')

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
