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
    # Any call that starts with __wt or WT_ and ends with a (
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

                        comment_regex = re.compile(r"\s*(\*|//)")
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

def parse_structs_in_file(file_path: str, module_structs, struct_list, field_freq_list):

    # Regex-palooza
    struct_def_regex = re.compile(r"struct (\w+) \{")     # struct __wt_log {
    end_of_typedef_regex = re.compile(r"} ([a-zA-Z_]+);") # } wt_thread_t;
    func_ptr_regex = re.compile(r".*__F\(([^)]+)\)")      # int __F(next)(
    other_func_ptr_regex = re.compile(r".*\(\*(\w+)\)\(") # bool (*chk_func)(...
    comment_regex = re.compile(r"\s+(\*|//)") 
    # Lines like `#define mod_col_update u2.column_leaf.update` that hide a field access
    hash_define_regex = re.compile(r"#define ([a-z_]+) ([a-zA-Z0-9_.]|->)+$")

    # Capture struct definitions. These can be pointers, pointers to pointers, tables, or 
    # have multiple fields defined on the same line
    # I'm making the asumption that definitions are always single line and ends on a semicolon
    pointer = r"(?:\*+)?"     # **foo
    table = r"(?:\[[^]]+\])?" # foo[SOME_LEN]
    repeating_struct_field_regex = re.compile(rf"(?:{pointer}(\w+){table}[,;])+")

    (module, _) = file_path_to_module_and_file(file_path)

    with open(file_path, 'r') as f:

        cur_struct = ""
        in_struct = False
        in_typedef_struct = False
        for line in f:
            assert(not (in_struct and in_typedef_struct))

            # Strip trailing comments on the same line
            # WT_VERB_TRANSACTION,          /*!< Transaction messages. */
            trailing_comment_index = line.find("/*")
            if trailing_comment_index != -1:
                line = line[:trailing_comment_index]
                pass

            if (not in_struct) and (not in_typedef_struct):
                if match := struct_def_regex.match(line):
                    cur_struct = match[1]
                    in_struct = True
                    if cur_struct in struct_list:
                        print("Struct found twice!")
                        exit(1)

                    struct_contents = []

                elif line.startswith("typedef struct {"):
                    in_typedef_struct = True
                    struct_contents = []
                    
            else:
                if line.startswith("};"):
                    assert(in_struct)
                    module_structs[module].append(cur_struct)
                    struct_list[cur_struct] = struct_contents
                    cur_struct = ""
                    struct_contents = []
                    in_struct = False

                elif match := end_of_typedef_regex.match(line):
                    assert(in_typedef_struct)
                    cur_struct = match[1]
                    module_structs[module].append(cur_struct)
                    # TODO -> adding in upper case typename here
                    struct_list[cur_struct] = struct_contents
                    cur_struct = ""
                    struct_contents = []
                    in_typedef_struct = False

                elif match := hash_define_regex.match(line):
                    hash_def_field_name = match[1]
                    field_freq_list[hash_def_field_name] += 1
                    struct_contents += [hash_def_field_name]

                else:
                    if comment_regex.match(line):
                        continue

                    if match := func_ptr_regex.match(line):
                        field_name = match[1]
                        field_freq_list[field_name] += 1
                        struct_contents += [field_name]
                    elif match := other_func_ptr_regex.match(line):
                        field_name = match[1]
                        field_freq_list[field_name] += 1
                        struct_contents += [field_name]
                    else:
            
                        if ';' not in line:
                            # not a field or func pointer definition
                            # Down here because func pointers can break over multiple lines
                            continue

                        matches = repeating_struct_field_regex.finditer(line)
                        # False positive on __wt_blkcache_hash in 
                        # TAILQ_HEAD(__wt_blkcache_hash, __wt_blkcache_item) * hash;
                        # Ignoring it
                        n = 0
                        for match in matches:
                            n += 1
                            field_name = match[1]
                            field_freq_list[field_name] += 1
                            struct_contents += [field_name]
                            if line[match.end()] == ';':
                                break

def build_mappings(module_structs, struct_list, duplicate_field_names):
        # Build mapping from field to module/file
    struct_to_module = {}
    for (module, structs) in module_structs.items():
        for struct in structs:
            struct_to_module[struct] = module

    # field to structs
    field_to_struct = {}
    for (struct, struct_fields) in struct_list.items():
        for field in struct_fields:
            if field in duplicate_field_names:
                field_to_struct[field] = "MULTIPLE_CANDIDATES"
            else:
                field_to_struct[field] = struct

    # Lazy - hard code WT_LSN which is a union
    # The script doesn't complain about __wt_rand_state so I'll ignore it
    field_to_struct['l'] = "MULTIPLE_CANDIDATES"
    field_to_struct['file'] = "__wt_lsn"
    field_to_struct['offset'] = "MULTIPLE_CANDIDATES"
    field_to_struct['file_offset'] = "MULTIPLE_CANDIDATES"
    struct_to_module['__wt_lsn'] = "log"

    # Packed structs
    field_to_struct["indx"] = "__wt_col_rle"
    struct_to_module['__wt_col_rle'] = "btree"

    return (struct_to_module, field_to_struct)

# Detecting struct field accesses via regex is a messy job. This function is a list of hacks to ignore false positives.
# I make zero promises there aren't false positives for false negatives.
def should_skip_match(line: str, match, used_field) -> bool:
    following_char = line[match.end()]

    if following_char == '(':
        # The match is a function
        return True

    if following_char == '"':
        # Special case: We've matched against something like
        # foo = "WT_CACHE.bytes_image"
        # Here's hoping there's never a foo = "WT_CACHE.bytes.image" in the future
        return True

    if "WiredTiger." in line: # Well that didn't take long: "WiredTiger.basecfg.set", "WiredTiger.backup.tmp"
        return True

    if "\"WT_CURSOR.next_random" in line: # Hack
        return True

    if used_field == "py": # Probably a python filename referenced in a comment. Skip
        return True

    if used_field == "deleted": # There's a struct defined locally in __wti_rec_col_var it's guaranteed to be local
        return True

    if "__asm__" in line: # Nope
        return True

    if "__wt_config_getones" in line: # We might lose a field or two, but the config strings are a massive distraction"
        return True

    if "__wt_conf_gets_def" in line: # As above
        return True

    if used_field == "op": # __meta_track defines an inline enum. Not dealing with that
        return True

    return False

def find_field_uses(my_module: str, struct_to_module, field_to_struct):
    comment_regex = re.compile(r"^\s*(?:\*|/\*)")
    # `->foo` or `.foo`, there can be multiple fields one after the other: foo->bar.baz
    field_use_regex = re.compile(r"(?:\.|->)([a-zA-Z]\w+)")
    fields_used_in_file_set = defaultdict(set)

    mod_struct_field_used_by = {}
    my_module_fields_used_in_file_set = defaultdict(set)
    
    for file_path in source_files():

        if not file_path.startswith("../src"):
            continue

        (module, file_name) = file_path_to_module_and_file(file_path)

        if file_name == "queue.h":
            # ignore. Macros making life hard again (tqh_first->field where field is a macro arg)
            continue

        if file_name == "stat.h":
            # We ignore it for definitions above
            continue

        if module in ["os_common", "os_posix", "os_win"]:
            # I'm not dealing with all those libc structs
            # If the os layer is calling WT structs they're on their own
            continue

        if not file_name.endswith(".h") and not file_name.endswith(".c"):
            # Death to .S files
            continue

        with open(file_path, 'r') as f:
            for line in f:
                if comment_regex.match(line):
                    continue

                for match in field_use_regex.finditer(line):
                    used_field = match.group(1)

                    if should_skip_match(line, match, used_field):
                        continue

                    owning_struct = field_to_struct[used_field]
                    if owning_struct == "MULTIPLE_CANDIDATES":
                        fields_used_in_file_set["These fields belong to multiple structs. Please check them manually"].add(used_field)
                        if module == my_module:
                            my_module_fields_used_in_file_set["These fields belong to multiple structs. Please check them manually"].add(used_field)
                    else:
                        owning_module = struct_to_module[owning_struct]
                        fields_used_in_file_set[f"{owning_module}  {owning_struct}"].add(used_field)
                        if module == my_module:
                            my_module_fields_used_in_file_set[f"{owning_module}  {owning_struct}"].add(used_field)

                    if owning_struct != "MULTIPLE_CANDIDATES":
                        owning_module = struct_to_module[owning_struct]
                        if owning_module not in mod_struct_field_used_by:
                            mod_struct_field_used_by[owning_module] = {}

                        if owning_struct not in mod_struct_field_used_by[owning_module]:
                            mod_struct_field_used_by[owning_module][owning_struct] = {}

                        if used_field not in mod_struct_field_used_by[owning_module][owning_struct]:
                            mod_struct_field_used_by[owning_module][owning_struct][used_field] = set()

                        mod_struct_field_used_by[owning_module][owning_struct][used_field].add(module)

    return mod_struct_field_used_by, my_module_fields_used_in_file_set

def print_fields_and_where_they_are_accessed(my_module: str, module_structs, struct_list, mod_struct_field_used_by, duplicate_field_names):
    print("==============")
    print(f"All structs in `{my_module}` and who they are accessed by")
    print("note: 'not found in usage map!' is probably a script error. This code is pretty jank")
    print("==============")
    for struct in sorted(module_structs[my_module]):
        print(f"\n{struct}")
        if struct not in mod_struct_field_used_by[my_module]:
            print("    struct not found! Either its field names are all ambiguous (present in mutiple structs) or not used in the code")
            continue

        for field in sorted(struct_list[struct]):
            if field not in mod_struct_field_used_by[my_module][struct]:
                if field in duplicate_field_names:
                    print(f"        {field}: present in many structs. Please review manually")    
                else:
                    print(f"        {field}: not found in usage map!")
                continue
            used_by = mod_struct_field_used_by[my_module][struct][field]
            assert(len(used_by) > 0)
            used_by -= {my_module}
            if used_by == set():
                print(f"        {field}: is private")
            else:
                print(f"        {field}: {used_by}")

def print_struct_fields_used_by(my_module, my_module_fields_used_in_file_set):
    print()
    print("=================================")
    print(f"Struct fields that are accessed by the '{my_module}' module (Disclaimer: This code is *rough*)")
    print("=================================")
    for owner in sorted(my_module_fields_used_in_file_set):
        print(f"\n{owner}:\n     {sorted(my_module_fields_used_in_file_set[owner])}")


def find_all_structs(my_module: str, command: str):
    struct_list = {}
    field_freq_list = Counter()

    module_structs = defaultdict(list)

    # =====================
    # Build struct mapping
    # =====================
    # Format
    # module_struct: {module: [struct_name]}
    # struct_list: {struct_name: [fields]}
    # duplicate_field_names: set()

    for file_path in source_files():
        if not file_path.startswith("../src"):
            continue

        parse_structs_in_file(file_path, module_structs, struct_list, field_freq_list)

    parse_structs_in_file("../src/include/wiredtiger.in", module_structs, struct_list, field_freq_list)

    duplicate_field_names = {}
    for (field, in_n_structs) in field_freq_list.items():
        if in_n_structs > 1:
            duplicate_field_names[field] = field_freq_list[field]

    (struct_to_module, field_to_struct) = build_mappings(module_structs, struct_list, duplicate_field_names)
    (mod_struct_field_used_by, my_module_fields_used_in_file_set) = find_field_uses(my_module, struct_to_module, field_to_struct)


    if command == "who_uses_structs_in":
        print_fields_and_where_they_are_accessed(my_module, module_structs, struct_list, mod_struct_field_used_by, duplicate_field_names)
    elif command == "structs_used_by":
        print_struct_fields_used_by(my_module, my_module_fields_used_in_file_set)
    else:
        print(f"Unexpected command for find_all_structs!: {command}")
        exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="TODO")

    subparsers = parser.add_subparsers(dest='command', required=True)

    who_uses_parser = subparsers.add_parser('who_uses', help='Who uses this module?')
    who_uses_parser.add_argument('name', type=str, help='module name')

    who_is_used_by_parser = subparsers.add_parser(
        'who_is_used_by', help='Who this module is used by')
    who_is_used_by_parser.add_argument('name', type=str, help='module name')

    data_usage_parser = subparsers.add_parser('data_usage', help='')
    data_usage_parser.add_argument('name', type=str, help='module name')

    who_uses_structs_in_parser = subparsers.add_parser('who_uses_structs_in', help='List a structs in a module, and which fields are used by which other modules')
    who_uses_structs_in_parser.add_argument('name', type=str, help='module name')

    structs_used_by_parser = subparsers.add_parser('structs_used_by', help='List all structs and their fields that are accessed from inside the specified module')
    structs_used_by_parser.add_argument('name', type=str, help='module name')

    who_uses_parser = subparsers.add_parser('list_cycles', help='What cycles is this module involved in?')
    who_uses_parser.add_argument('name', type=str, help='module name')

    return parser.parse_args()

def main():

    args = parse_args()

    func_locations, common_wt_macros = get_func_locations()
    graph = build_dependency_graph(func_locations, common_wt_macros)

    if args.command == "who_uses":
        who_uses(args.name, graph)
    elif args.command == "who_is_used_by":
        who_is_used_by(args.name, graph, func_locations)
    elif args.command == "list_cycles":
        for c in nx.simple_cycles(graph, length_bound=5):
            if args.name in c:
                print(c)
    elif args.command == "who_uses_structs_in":
        find_all_structs(args.name, "who_uses_structs_in")
    elif args.command == "structs_used_by":
        find_all_structs(args.name, "structs_used_by")


if __name__ == "__main__":
    main()
