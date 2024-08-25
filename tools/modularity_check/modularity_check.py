#!/usr/bin/env python

import argparse

from parse_wt_ast import parse_wiredtiger_files
from build_dependency_graph import build_graph
from query_dependency_graph import who_uses, who_is_used_by

def parse_args():
    parser = argparse.ArgumentParser(description="TODO")

    subparsers = parser.add_subparsers(dest='command', required=True)

    who_uses_parser = subparsers.add_parser('who_uses', help='Who uses this module?')
    who_uses_parser.add_argument('module', type=str, help='module name')

    who_is_used_by_parser = subparsers.add_parser(
        'who_is_used_by', help='Who this module is used by')
    who_is_used_by_parser.add_argument('module', type=str, help='module name')

    return parser.parse_args()

def main():

    args = parse_args()

    parsed_files = parse_wiredtiger_files(debug=False)
    graph = build_graph(parsed_files)

    if args.command == "who_uses":
        who_uses(args.module, graph)
    elif args.command == "who_is_used_by":
        who_is_used_by(args.module, graph)
    else:
        print(f"Unrecognised command {args.command}!")
        exit(1)

if __name__ == "__main__":
    main()
