import networkx as nx

from typing import List

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
