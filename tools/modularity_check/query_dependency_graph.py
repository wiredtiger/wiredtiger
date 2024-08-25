import networkx as nx

from typing import List, Set

from parse_wt_ast import File, Struct
from build_dependency_graph import AMBIG_NODE

# Print the links that form the dependency of `from_node` on `to_node`
# This function assumes that the dependency exists
def print_edge(graph, from_node, to_node) -> bool:
    edge = (from_node, to_node)
    link_data = nx.get_edge_attributes(graph, 'link_data')
    link_data[edge].print_type_uses()
    link_data[edge].print_struct_accesses()
    link_data[edge].print_func_calls()

def who_uses(module: str, graph: nx.DiGraph):
    incoming_edges = graph.in_edges(module)

    for edge in sorted(incoming_edges):
        (caller, _) = edge
        print(f"\n{caller}/")
        print_edge(graph, caller, module)

def who_is_used_by(module: str, graph: nx.DiGraph):
    incoming_edges = graph.out_edges(module)

    for edge in sorted(incoming_edges):
        (_, callee) = edge
        print(f"\n{callee}/")
        print_edge(graph, module, callee)

# Given a list of nodes that describe a cycle explain the path to reach it
def explain_cycle(cycle: List[str], graph: nx.DiGraph):

    link_data = nx.get_edge_attributes(graph, 'link_data')
    
    # First check the cycle exists
    for i in range(0, len(cycle)):
        cur_node = cycle[i]
        next_node = cycle[(i + 1) % len(cycle)]
        if not graph.has_edge(cur_node, next_node):
            print(f"No cycle! There is no dependency from '{cur_node}' to '{next_node}'")
            return

    # If it does then dump the path
    for i in range(0, len(cycle)):
        cur_node = cycle[i]
        next_node = cycle[(i + 1) % len(cycle)]
        print(f"From {cur_node} to {next_node}:")
        print_edge(graph, cur_node, next_node)

# Report which structs and struct fields are private to the module
def privacy_report(module, graph, parsed_files: List[File], ambigious_fields: Set[str]):

    incoming_edges = graph.in_edges(module)
    link_data = nx.get_edge_attributes(graph, 'link_data')

    all_structs: List[Struct] = []
    num_private, num_fields = 0, 0

    for file in parsed_files:
        if file.module == module:
            all_structs += file.structs

    for struct in sorted(all_structs, key=lambda s: s.name):
        print(struct.name)
        for field in sorted(struct.fields):
            if field in ambigious_fields:
                print(f"    {field}: Ambigous! Please check manually")
                continue

            num_fields += 1
            used_by_modules = set()
            for edge in incoming_edges:
                (caller, _) = edge
                if link_data[edge]:
                    if link_data[edge].struct_accesses[struct.name][field]:
                        used_by_modules.add(caller)

            if len(used_by_modules) == 0:
                print(f"    {field}: Private")
                num_private += 1
            else:
                print(f"    {field}: {sorted(used_by_modules)}")
        print()

    private_pct = round((num_private / num_fields) * 100, 2)
    print(f"{num_private} of {num_fields} non-ambiguous fields ({private_pct}%) are private")

def generate_dependency_file(graph: nx.DiGraph):
    with open("dep_file.new", 'w') as f:
        f.write("# ====================================================== #\n")
        f.write("# WiredTiger has the following inter-module dependencies #\n")
        f.write("# ====================================================== #\n")
        for module in sorted(graph.nodes()):
            incoming_edges = graph.out_edges(module)
            for (caller, callee) in sorted(incoming_edges):
                if caller == AMBIG_NODE or callee == AMBIG_NODE:
                    # Don't report the ambigious node
                    continue
                f.write(f"{caller} -> {callee}\n")

    # TODO - Add logic to check for changes to a pre-existing dep_file. 
    # This should warn when a new line is added and notify when one is removed.
    # To check - if this is infrequent over past commits we might be able to add it to s_all
