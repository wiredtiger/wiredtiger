import networkx as nx

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

