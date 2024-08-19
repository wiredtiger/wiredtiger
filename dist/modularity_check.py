#! /usr/bin/env python

# Commands to print all functions defined in a module and used elsewhere, or all 
# functions used in a module and defined elsewhere.
# Example Usage:
#   > python ./modularity_check.py who_uses evict
#   > python ./modularity_check.py who_is_used_by log
#     
#  Example output:
#     btree/
#         bt_cursor.c
#             __wt_log_op: 1
#     
#     conn/
#         conn_api.c
#             __wt_log_compat_verify: 1
#  ...
# 
#  NOTE!! This script makes some important assumptions.
#  - Header files have been manually grouped with their src/foo/ folders. This linking is done 
#    in `header_mappings.py`` and is a best-guess assumption. A function defined in src/log but 
#    called in log_inline.h is not considered an external call. Similarly for functions defined
#    in cache.h and called in src/evict/*
#  - Some common macros are intentionally ignored such as WT_RET.
#  - This script is not refined. There may be bugs. Currently the dependency graph reports 
#    288 in-links but only 285 out-links.

import argparse
import re
from collections import defaultdict, Counter

import networkx as nx

from dist import source_files
from header_mappings import header_mappings, skip_files

# Convert a file path (../src/evict/evict_lru.c) into it's module and file_name: 
#     (evict, evict_lru.c)
# NOTE!! This contains special handling for include/*.h files
# NOTE!! This assumes a flat directory. src/checksum has subfolders so we merge them all 
#        into a single checksum module
def file_path_to_module_and_file(file_path: str) -> (str, str):

    # strip the leading path. We only care about details at the module level
    assert(file_path.startswith("../src/"))
    fp = file_path[7:]

    fp_split = fp.split("/")

    if len(fp_split) != 2:
        # Special case. Only checksum has subdirectories. We'd like to know if this changes.
        assert(fp_split[0] == "checksum")

    module = fp_split[0]
    file_name = fp_split[-1]

    # Special handling. include/*.h files are located in include/ but they belong to a module in src
    if module == "include" and file_name.endswith(".h"):
        if file_name in header_mappings:
            module = header_mappings[file_name]
        else:
            if file_name not in skip_files:
                print(f"Unexpected file name! {file_name}")
                exit(0)

    return (module, file_name)


# Determine which functions are defined in which modules
def get_func_locations():

    # All functions have a function comment containing `func_name --` as enforced by s_all
    func_definition_regex = re.compile(r'(__wt_.*) --')
    
    # Except for macros that look like functions (e.g. #define __wt_page_in()
    macro_func_definition_regex = re.compile(r'#define (__wt_[^(]*)\(')

    # And generated functions like the __wt_atomics
    macro_generated_atomics_regex = re.compile(r'WT_ATOMIC_FUNC\(([^,]+)')

    # Finally we have the normal #define WT_ macros
    wt_macros_regex = re.compile(r'#define (WT_[^(]*)\(')
    
    func_locations = {}
    common_wt_macros = set()
    for file_path in source_files():

        if not file_path.startswith("../src"):
            continue

        (module, file_name) = file_path_to_module_and_file(file_path)

        if file_name in skip_files:
            continue

        with open(file_path, 'r') as f:
            for line in f:

                # Normal functions
                if match := func_definition_regex.search(line):
                    func_name = match.group(1)
                    func_locations[func_name] = (module, file_name)
                
                # Macros that look like functions
                if match := macro_func_definition_regex.search(line):
                    func_name = match.group(1)
                    func_locations[func_name] = (module, file_name)

                # macro generated atomics
                if match := macro_generated_atomics_regex.search(line):
                    ttype = match.group(1)
                    for op in ["cas", "add", "fetch_add", "sub", "load", "store"]:
                        func_name = f"__wt_atomic_{op}{ttype}"
                        func_locations[func_name] = (module, file_name)

                # WT_ macros
                if match := wt_macros_regex.search(line):
                    func_name = match.group(1)
                    func_locations[func_name] = (module, file_name)
                    if file_name in ["error.h", "stat.h", "gcc.h", "hardware.h"]:
                        common_wt_macros.add(func_name)

    return (func_locations, common_wt_macros)

# Walk the src/ folder and find all __wt_ or WT_ function calls. Map the module and file of the 
# function call to the module that the function belongs to in a 
def build_dependency_graph(func_locations, common_wt_macros) -> nx.DiGraph:
    # This regex warrants an up front apology. It finds all __wt_ functions or WT_ macro calls 
    # in the code. From the start:
    #   (?:__wt|WT)_  means we start with either __wt_ or WT_ and ?: stops the group () from 
    #                 being captured
    #   \w+)\(        continues until we see an opening bracket indicating the function call
    # As a result we capture just the function/macro's name in the capture group
    function_call_regex = re.compile(r'((?:__wt|WT)_\w+)\(')

    graph = nx.DiGraph()

    for file_path in source_files():

        if not file_path.startswith("../src"):
            continue

        (module, file_name) = file_path_to_module_and_file(file_path)

        if file_name in skip_files:
            continue

        with open(file_path, 'r') as f:
            for line in f:
                matches = function_call_regex.findall(line)
                for func in matches:

                    if func not in func_locations:
                        if func.startswith("__wt_stat_") or func.startswith("__wt_stats_") or \
                            func.startswith("__wt_crc32c_") \
                            or func.startswith("__wt_conn_config_init") \
                                or func.startswith("__wt_conn_config_discard"):
                            # Macro generated functions that don't respect the s_all rule. 
                            # Ignore them for now
                            continue

                        if func in ["__wt_tret", "__wt_tret_error_ok", "__wt_string_match"]:
                            # Pre-processor shenanigans. These functions are wrapped by a macro 
                            # we detect correctly
                            continue

                        if func == "WT_CRC32_ENTRY":
                            # TODO - we don't include the header???
                            continue

                        if (func.startswith("WT_ITEM") and "_WT_ITEM" in line):
                            # The __pack_encode_WT_ITEM functions use a non-standard naming format 
                            # causing the regex to match on the latter WT_ITEM_ section even though 
                            # it's not the start of the function_name. Ignore them
                            continue

                        comment_regex = re.compile(r"\s+(\*|//)")
                        if comment_regex.match(line):
                            # This line is a comment
                            continue

                        print(f"Cannot find the module for '{func}'!\n    current line {line}")
                        exit(1)
                    else:

                        # Filter out some of the more common WT_ macros. We don't get much value 
                        # from tracking calls to WT_RET
                        miscellaneous_ignore_macros = [
                            "WT_CLEAR", "WT_DATA_IN_ITEM", "WT_DECL_CONF", "WT_DECL_ITEM", 
                            "WT_IGNORE_RET", "WT_MAX", "WT_MIN", "WT_NOT_READ", "WT_STREQ", 
                            "WT_UNUSED"
                        ]
                        if func in common_wt_macros or func in miscellaneous_ignore_macros or \
                            func.startswith("WT_WITH") or func.startswith("__wt_atomic_"):
                            continue

                        out_edge = module
                        (in_edge, _) = func_locations[func]

                        if out_edge != in_edge: # No need to add reference to self
                            if graph.has_edge(out_edge, in_edge):
                                # we added this one before, just increase the weight by one
                                graph[out_edge][in_edge]['weight'] += 1
                                graph[out_edge][in_edge]['func_data'][file_name][func] += 1
                            else:
                                # new edge. add with weight=1
                                graph.add_edge(out_edge, in_edge, weight=1)
                                graph[out_edge][in_edge]['func_data'] = defaultdict(Counter)
                                graph[out_edge][in_edge]['func_data'][file_name][func] += 1

    return graph

# Parse the dependency graph and report all modules that use the specified `module`
# This reports the following format:
# calling_module
#     calling_file
#         function_called: num_calls
def who_uses(module: str, graph: nx.DiGraph):
    incoming_edges = graph.in_edges(module)
    func_data = nx.get_edge_attributes(graph, 'func_data')

    modules = set() 
    funcs = set()
 
    for edge in sorted(incoming_edges):
        (caller, _) = edge
        modules.add(caller)
        print(f"\n{caller}/")
        for file in sorted(func_data[edge]):
            print(f"    {file}")
            for func, freq in sorted(func_data[edge][file].items()):
                funcs.add(func)
                print(f"        {func}: {freq}")

    print(f"\n'{module}' is used by {len(modules)} modules that call {len(funcs)} functions.")

# Parse the dependency graph and report all calls to other modules made by the specified `module`
# This reports the following format:
# called_module
#     function_called: num_calls
def who_is_used_by(module: str, graph: nx.DiGraph, func_locations):
    incoming_edges = graph.out_edges(module)
    func_data = nx.get_edge_attributes(graph, 'func_data')

    modules = set() 
    funcs = set()

    for edge in sorted(incoming_edges):
        (_, callee) = edge # _ is always log. We're only looking at incoming edges
        modules.add(callee)
        print(f"\n{callee}/")

        # We don't report which file called the function. Aggregate all function calls 
        # across all files.
        file_func_freq = defaultdict(Counter)
        for calling_file in func_data[edge]:
            for func, freq in func_data[edge][calling_file].items():
                funcs.add(func)
                _, called_file = func_locations[func]
                file_func_freq[called_file][func] += freq

        for called_file, func_and_freq in sorted(file_func_freq.items()):
            print(f"    {called_file}")
            for func, freq in sorted(func_and_freq.items()):
                print(f"        {func}: {freq}")

    print(f"\n'{module}' uses {len(modules)} modules calling {len(funcs)} functions.")


def parse_args():
    parser = argparse.ArgumentParser(description="TODO")

    subparsers = parser.add_subparsers(dest='command', required=True)

    who_uses_parser = subparsers.add_parser('who_uses', help='Who uses this module?')
    who_uses_parser.add_argument('name', type=str, help='module name')

    who_is_used_by_parser = subparsers.add_parser(
        'who_is_used_by', help='Who this module is used by')
    who_is_used_by_parser.add_argument('name', type=str, help='module name')

    return parser.parse_args()

def main():

    args = parse_args()

    func_locations, common_wt_macros = get_func_locations()
    graph = build_dependency_graph(func_locations, common_wt_macros)

    if args.command == "who_uses":
        who_uses(args.name, graph)
    elif args.command == "who_is_used_by":
        who_is_used_by(args.name, graph, func_locations)

if __name__ == "__main__":
    main()


