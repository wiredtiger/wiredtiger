#!/usr/bin/env python

import tree_sitter_c as tsc
from tree_sitter import Language, Parser

from dataclasses import dataclass, field
from collections import defaultdict, Counter
from typing import Tuple, Dict, List
import multiprocessing
import re

from header_mappings import header_mappings, skip_files
from dist import source_files

import networkx as nx

# Create a C parser for TreeSitter to use
parser = Parser()
C_LANGUAGE = Language(tsc.language())
parser.language = C_LANGUAGE

@dataclass
class Struct:
    name: str
    fields: set[str]

@dataclass
class Function:
    name: str
    functions_called: set[str]
    types_used: set[str]
    fields_accessed: set[str]
    note: str = ""

@dataclass
class File:
    name: str
    module: str
    functions: list[Function]
    types_defined: list[str]
    structs: list[Struct]

# Return a list of all child, grandchild, and so on nodes with the type `type`
# Optionally stop searching when an `ignore` node is encountered
def descendants_with_name(node, wanted: list[str], ignore: list[str]=[], top_level_only=False):
    matches = []

    if node.type in ignore:
        return []

    if node.type in wanted and top_level_only:
            return [node]

    if node.type in wanted:
        matches = [node]

    if node.descendant_count != 0:
        for child in node.children:
            matches += descendants_with_name(child, wanted, ignore, top_level_only=top_level_only)

    return matches

def has_descendant(node, type):
    if node.type == type:
        return True

    if node.descendant_count != 0:
        for child in node.children:
            if(has_descendant(child, type)):
                return True

    return False


# Parse a `struct __wt_foo {` node
def parse_struct(node):
    if node.child_by_field_name("name") != None:
        struct_name = node.child_by_field_name("name").text.decode()
        # Cast the type name from __wt_foo to WT_FOO
        struct_name = struct_name.upper().lstrip("__")
    else:
        struct_name = "NO_NAME_FOUND"

    # Bypass all the processing. Drill down to the field_identifier nodes and use those
    # TODO - dupe names when #ifdef?
    declared_fields = descendants_with_name(node, ["field_identifier"])
    field_names = [n.text.decode() for n in declared_fields]

    # Find #define accesses like `#define pg_intl_parent_ref u.intl.parent_ref`
    fields = node.child_by_field_name("body")

    if fields == None:
        print("====================")
        print(node.text.decode())
        raise Exception("No fields in struct!")

    preproc_defs = [f for f in fields.children if f.type == "preproc_def"]
    # We don't want const definitions, and these are almost always digits. Filter out the digits
    preproc_defs_no_const = [p for p in preproc_defs if p.child_by_field_name('value').text.decode()[0].isalpha()]

    hash_define_fields = [p.child_by_field_name('name').text.decode() for p in preproc_defs_no_const]

    all_fields = field_names + hash_define_fields

    return Struct(struct_name, set(all_fields))

# Parse a `typedef struct {} WT_FOO` node.
# This code just needs to read the typedef name and can reuse the struct parsing logic
def parse_typedef_struct(node):
    struct_node = node.child_by_field_name("type")
    struct = parse_struct(struct_node)

    struct.name = node.child_by_field_name("declarator").text.decode()
    return struct

def filter_common_calls(functions_called) -> List[str]:

    # libc funcs and variadics vars
    non_wt_funcs = [
        "memcmp", "memcpy", "memset","sscanf", "strlen", "strrchr", "va_copy", "va_end", 
        "va_start"
    ]

    common_func_prefixes = [
        "WT_RET", "WT_ERR", "WT_ASSERT", "__wt_verbose", "WT_STAT", "F_", "FLD_", "LF_", 
        "TAILQ", "__wt_atomic_", "S2BT", "S2C", "WT_UNUSED", "__wt_err", "WT_TRET", "WT_DECL_"
    ]

    filtered_list = []
    for func in functions_called:
        if func in non_wt_funcs:
            continue
        if any(func.startswith(prefix) for prefix in common_func_prefixes):
            continue
        if "BARRIER" in func: # Ignore memory barrier macros
            continue
        filtered_list.append(func)
    return filtered_list

# Parse a function node.
def parse_function(node):
    function_return_type = node.child_by_field_name("type").text.decode()
    function_header = node.child_by_field_name("declarator")
    function_body = node.child_by_field_name("body")

    if function_header.child_by_field_name("declarator") == None:
        raise Exception(f"Function header has no name! {node.text.decode()}")

    function_name = function_header.child_by_field_name("declarator").text.decode()

    # field accesses (foo.bar, foo->bar, foo->bar.baz)
    all_field_accesses = descendants_with_name(function_body, ["field_identifier"])
    all_field_access_names = [f.text.decode() for f in all_field_accesses]

    # function calls (__wt_foo(), WT_BAR())
    all_func_calls = descendants_with_name(function_body, ["call_expression"])
    all_func_call_nodes = [f.child_by_field_name("function") for f in all_func_calls]
    all_func_calls = [f.text.decode() for f in all_func_call_nodes]
    all_func_calls = filter_common_calls(all_func_calls)
    # Function pointers are reported as bm->free. Just report the actual function
    all_func_calls = [fc.split("->")[-1] for fc in all_func_calls]

    # Types used by function parameters
    func_parameters = descendants_with_name(function_header, ["parameter_declaration"])
    param_types = [param.child_by_field_name("type") for param in func_parameters]
    param_type_names = [param.text.decode() for param in param_types]

    # Types declared in the function
    # There won't be type declaration inside function calls or expressions
    type_declarations = descendants_with_name(function_body, ["declaration"], ignore=["call_expression", "expression_statement", "parenthesized_expression"])
    declared_types = [param.child_by_field_name("type") for param in type_declarations]
    declared_type_names = [param.text.decode() for param in declared_types]

    all_types_used = [function_return_type] + param_type_names + declared_type_names

    return Function(function_name, set(all_func_calls), set(all_types_used), set(all_field_access_names))

# Parse the file to find all macro function definitions:
# #define __wt_hazard_set(session, walk, busyp) ...
# #define WT_REF_SET_STATE(ref, s)
# TODO
# Small issue - tree-sitter treats the macro body as one big blob. This means we can't easily 
# identify function calls or field references inside the macro body.
# We can't re-parse the do { } while(0) loop either since string concatenation tokens (##) 
# aren't valid C. For now just track that the function exists.
def parse_macro_funcs(root_node):
    warning = "Note! Macro functions have parsing issues. All fields are empty."
    # macro functions won't be defined inside functions (I hope).
    all_preproc_funcs = descendants_with_name(root_node, ["preproc_function_def"], ignore=["function_definition"])
    preproc_func_names = [preproc.child_by_field_name("name").text.decode() for preproc in all_preproc_funcs]
    all_preproc_funcs = [Function(name, {}, {}, {}, warning) for name in preproc_func_names]
    return merge_duplicate_funcs(all_preproc_funcs)

# Parse a file and find all functions defined in the file
def parse_funcs(root_node):
    funcs: list[Function] = []
    # Ignore declarations as these are extern functions: `extern int wt_foo()`
    for func_node in descendants_with_name(root_node, ["function_definition", "function_declarator"], top_level_only=True, ignore=["declaration"]):
        if func_node.type == "function_definition":
            funcs.append(parse_function(func_node)) 
        elif func_node.type == "function_declarator":
            # Function pointers
            # print(f"func_pointer!\n{func_node.text.decode()}\n\n======================\n")
            func_ptr_decl_node = func_node.child_by_field_name("declarator")

            if len(func_ptr_decl_node.children) == 0:
                # Bit of a hack. Catches static func definitions like 
                # `static int __log_newfile(WT_SESSION_IMPL *, bool, bool *);`
                # We'll parse them when we encounter the definitions below
                continue

            func_pointer_name = func_ptr_decl_node.text.decode()
            # Sanity check. function pointer names should always be `(*foo)`
            assert(func_pointer_name.startswith("(*"))
            assert(func_pointer_name.endswith(")"))
            func_pointer_name = func_pointer_name[2:-1]

            note = "Function pointer! Name only"
            funcs.append(Function(name=func_pointer_name, types_used={}, fields_accessed={}, functions_called={}, note=note))
        else:
            raise Exception(f"Unexpected function type: {func_node.type}")

    return merge_duplicate_funcs(funcs)

# Find all type definitions in a file. These are structs, unions, or typedefs
def parse_types(root_node) -> Tuple[list[Struct], list[str]]:
    structs: list[Struct] = []
    non_struct_types: list[str] = []
    
    # All types are one of the following
    wanted_nodes = ["struct_specifier", "union_specifier", "type_definition"]
    # If a type is inside a function definition then it's function local and can be ignored.
    type_nodes = descendants_with_name(root_node, wanted_nodes, top_level_only=True, 
                                       ignore=["function_definition"])
    for type_node in type_nodes:

        if type_node.type in ["struct_specifier", "union_specifier"]:
            # Unions are rare in WT and they parse with the struct parsing code. Treat 'em identical
            if type_node.child_by_field_name("body") == None:
                # Ignore forward declarations `struct __wt_foo`
                continue

            structs.append(parse_struct(type_node))
        elif type_node.type == "type_definition":
            # There's three types of type definitions:
            # 1. `typedef struct {} WT_FOO` which we want,
            # 2. `typedef struct __wt_stuff WT_STUFF;` which is annoyingly outside of extern.h. Ignore
            # 3. Non struct typedefs (typedef int wt_int), where we just want the type name

            typedef_contents = type_node.child_by_field_name("type")
            if typedef_contents.type in ["struct_specifier", "union_specifier"]:
                if typedef_contents.child_by_field_name("body") == None:
                    # Case (2)
                    continue

                # Case (1)
                s = parse_typedef_struct(type_node)
                structs.append(s)
            elif typedef_contents.type in ["enum_specifier", "macro_type_specifier", "primitive_type", "type_identifier", "sized_type_specifier"]:
                # Case (3)
                enum_name = type_node.child_by_field_name("declarator")
                non_struct_types.append(enum_name.text.decode())
            else:
                raise Exception(f"Unknown typedef contents: {typedef_contents}\n{type_node.text.decode()}")

    # Sanity check. We didn't find multiple definitions of the a struct. (They aren't defined in #if branches)
    assert(len(structs) == len(set([s.name for s in structs])))

    # Non-struct types can be typedeffed in multiple #if/else branches. Merge them into one
    non_struct_types = list(set(non_struct_types))
    return (structs, non_struct_types)

# Due to #ifdefs a function may be defined multiple times. Merge the instances together
def merge_duplicate_funcs(funcs: list[Function]) -> list[Function]:
    seen_funcs: Dict[str, Function] = {}
    for func in funcs:
        if func.name in seen_funcs:
            # Already seen! Merge the instances
            seen_funcs[func.name].fields_accessed |= func.fields_accessed
            seen_funcs[func.name].functions_called |= func.functions_called
            seen_funcs[func.name].types_used |= func.types_used
            pass
        else:
            seen_funcs[func.name] = func
    return list(seen_funcs.values())

# Read a file and return a File struct
def parse_file(file_path, root_node):

    (module, file_name) = file_path_to_module_and_file(file_path)

    all_funcs = parse_funcs(root_node) + parse_macro_funcs(root_node)
    (structs, non_struct_types) = parse_types(root_node)
    all_types = [s.name for s in structs] + non_struct_types

    return File(name=file_name, module=module, functions=all_funcs, structs=structs, types_defined=all_types)

# Macros aren't part of the C language grammar and cause headaches for tree-sitter. 
# Resolve some of the problem cases with manual replacement.
def preprocess_file(bytes):
    # Convert to str for easier pre-processing
    as_str = bytes.decode()

    # These macros break tree-sitter's parsing as they're inside a struct but don't 
    # have a semi-colon. Easy fix
    as_str = as_str.replace("WT_CACHE_LINE_PAD_BEGIN", "WT_CACHE_LINE_PAD_BEGIN;")
    as_str = as_str.replace("WT_CACHE_LINE_PAD_END", "WT_CACHE_LINE_PAD_END;")

    as_str = re.sub(r"WT_PACKED_STRUCT_BEGIN\(([^)]+)\)", r"struct \1 {", as_str)
    as_str = re.sub(r"^WT_PACKED_STRUCT_END", r"\};", as_str, flags=re.MULTILINE)

    as_str = as_str.replace("WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))", "")
    as_str = as_str.replace("WT_ATTRIBUTE_LIBRARY_VISIBLE", "")

    # Expand __F(foo) macro to (*foo) 
    as_str = re.sub(r"__F\(([^)]+)\)", r"(*\1)", as_str)

    # Strip single comments inside macro bodies. The grammar can't handle them
    # Single line comments at end of a code line
    as_str = re.sub(r"(/\*.*\*.\s+)\\", r"\\", as_str)
    # Multi-line comments (any line starting with /* or *)
    as_str = re.sub(r"^\s+/?\*.*\\$", r"\\", as_str, flags=re.MULTILINE)

    # Interferes with determining function return types
    as_str = as_str.replace("WT_INLINE", "")
    as_str = as_str.replace("wt_shared", "")

    # # Just remove the macro string cat lines. I think we can ignore macro bodies, but the ## breaks parsing
    # as_str = re.sub(r"[^\n]#+", r"", as_str)

    return as_str.encode()


def process_file(file_path):
    with open(file_path, 'rb') as f:
        # Parse the source code
        bytes = f.read()
        bytes = preprocess_file(bytes)
        tree = parser.parse(bytes)

        file = parse_file(file_path, tree.root_node)
        return file


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

    # Special case: We have 5 os_* folders but they're all in the os_interface layer
    if module.startswith("os_"):
        module = "os_layer"

    # Special handling. include/*.h files are located in include/ but they belong to a module in src
    if module == "include" and file_name.endswith(".h"):
        if file_name in header_mappings:
            module = header_mappings[file_name]
        else:
            if file_name not in skip_files:
                raise Exception(f"Unexpected file name! {file_name}")

    return (module, file_name)

def parse_wiredtiger_files(debug=False):
    # We only want C files in the src/ folder
    files = [f for f in source_files() if f.startswith("../src/")]
    files = [f for f in files if f.endswith(".h") or f.endswith(".c")]
    # Other than wiredtiger.in which contains structs and functions
    files.append("../src/include/wiredtiger.in")

    # Remove extern files. They just contain externs
    files.remove("../src/include/extern.h")
    files.remove("../src/include/extern_darwin.h")
    files.remove("../src/include/extern_linux.h")
    files.remove("../src/include/extern_posix.h")
    files.remove("../src/include/extern_win.h")

    parsed_files: list[File] = []
    
    if debug:
        # Slow version. Easier for debugging
        for file in files:
            parsed_files.append(process_file(file))
    else:
        # Fast version. Run in parallel
        with multiprocessing.Pool() as pool:
            parsed_files = pool.map(process_file, files)

    return parsed_files

def incr_edge_struct_access(graph, calling_module, dest_module, field, struct):
    if calling_module != dest_module: # No need to add reference to self
        if not graph.has_edge(calling_module, dest_module):
            graph.add_edge(calling_module, dest_module)
            graph[calling_module][dest_module]['link_data'] = Link()
        graph[calling_module][dest_module]['link_data'].struct_accesses[struct][field] += 1

def incr_edge_func_call(graph, calling_module, dest_module, func_call):
    if calling_module != dest_module: # No need to add reference to self
        if not graph.has_edge(calling_module, dest_module):
            graph.add_edge(calling_module, dest_module)
            graph[calling_module][dest_module]['link_data'] = Link()
        graph[calling_module][dest_module]['link_data'].func_calls[func_call] += 1

def incr_edge_type_use(graph, calling_module, dest_module, type_use):
    if calling_module != dest_module: # No need to add reference to self
        if not graph.has_edge(calling_module, dest_module):
            graph.add_edge(calling_module, dest_module)
            graph[calling_module][dest_module]['link_data'] = Link()
        graph[calling_module][dest_module]['link_data'].types_used[type_use] += 1

@dataclass
class Link:
    func_calls: Counter= field(default_factory=lambda: Counter())
    types_used: Counter= field(default_factory=lambda: Counter())
    # struct -> field: num_accesses
    struct_accesses: defaultdict = field(default_factory=lambda: defaultdict(Counter)) 

    def print_struct_accesses(self):
        if len(self.struct_accesses) == 0:
            return
 
        print("    struct_accesses:")
        for struct, fields in sorted(self.struct_accesses.items()):
            print(f"        {struct}")
            for field in sorted(fields):
                print(f"            {field}: {fields[field]}")
            print()

    def print_func_calls(self):
        if len(self.func_calls) == 0:
            return
        print("    function_calls:")
        for func, num_calls in sorted(self.func_calls.items()):
            print(f"        {func}: {num_calls}")
        print()

    def print_type_uses(self):
        if len(self.types_used) == 0:
            return
        print(f"    types_used: {sorted(self.types_used)}")


# Walk all files and create a module-to-module dependency graph.
# We'll stick reason for the dependency (function call, struct access, 
# use of type) in the edge metadata
def build_graph(parsed_files: List[File]):

    field_to_struct_map = defaultdict(set)
    struct_to_module_map = defaultdict(set)
    type_to_module_map = defaultdict(set)
    func_to_module_map = defaultdict(set)

    # Build up the reverse mappings
    for file in parsed_files:
        for func in file.functions:
            func_to_module_map[func.name].add(file.module)

        for struct in file.structs:
            struct_to_module_map[struct.name].add(file.module)
            for field in struct.fields:
                field_to_struct_map[field].add(struct.name)

        for type in file.types_defined:
            type_to_module_map[type].add(file.module)

    graph = nx.DiGraph()
    # If we can't determine the module something links to track it in the ambiguous node.
    # These can be reviewed manually
    AMBIG_NODE = "Ambiguous linking or parsing failed"
    graph.add_node(AMBIG_NODE)

    for file in parsed_files:
        calling_module = file.module
        graph.add_node(file.module)
        # TODO - struct type decls
        for func in file.functions:
            for field_access in func.fields_accessed:
                structs_with_field = field_to_struct_map[field_access]
                if len(structs_with_field) == 1:
                    dest_struct = list(structs_with_field)[0]
                    dest_modules = struct_to_module_map[dest_struct]
                    if len(dest_modules) == 1:
                        dest_module = list(dest_modules)[0]
                        incr_edge_struct_access(graph, calling_module, dest_module, field_access, dest_struct)
                    else:
                        incr_edge_struct_access(graph, calling_module, AMBIG_NODE, field_access, dest_struct)
                else:
                    unknown_struct = "These fields belong to multiple structs! Please check manually"
                    incr_edge_struct_access(graph, calling_module, AMBIG_NODE, field_access, unknown_struct)

            for called_func in func.functions_called:
                called_modules = func_to_module_map[called_func]
                if len(called_modules) == 1:
                    dest_module = list(called_modules)[0]
                    incr_edge_func_call(graph, calling_module, dest_module, called_func)
                else:
                    incr_edge_func_call(graph, calling_module, AMBIG_NODE, called_func)

            for type_use in func.types_used:
                type_modules = type_to_module_map[type_use]
                if len(type_modules) == 1:
                    dest_module = list(type_modules)[0]
                    incr_edge_type_use(graph, calling_module, dest_module, type_use)
                else:
                    incr_edge_type_use(graph, calling_module, AMBIG_NODE, type_use)

    return graph

def who_uses(module: str, graph: nx.DiGraph):
    incoming_edges = graph.in_edges(module)
    link_data = nx.get_edge_attributes(graph, 'link_data')

    for edge in sorted(incoming_edges):
        (caller, _) = edge
        print(f"\n{caller}/")
        link_data[edge].print_type_uses()
        link_data[edge].print_struct_accesses()
        link_data[edge].print_func_calls()

def who_is_used_by(module: str, graph: nx.DiGraph):
    incoming_edges = graph.out_edges(module)
    link_data = nx.get_edge_attributes(graph, 'link_data')

    for edge in sorted(incoming_edges):
        (_, callee) = edge
        print(f"\n{callee}/")
        link_data[edge].print_type_uses()
        link_data[edge].print_struct_accesses()
        link_data[edge].print_func_calls()


def main():
    parsed_files = parse_wiredtiger_files(debug=False)
    graph = build_graph(parsed_files)
    # who_uses("log", graph)
    who_is_used_by("log", graph)

if __name__ == "__main__":
    main()
