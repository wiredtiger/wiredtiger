
TODO - fill this out properly

# Modularity checkers

Tool to review the modularity of components in WiredTiger.

> [!WARNING]  
> This tool is not perfect. Please validate all results.

## Getting started:
```sh
virtualenv venv
(venv) pip install -r requirement.txt
```

## Using the tool:
```sh
./modularity_check.py --help
./modularity_check.py who_uses log                           # Report all the users of the log module
./modularity_check.py who_is_used_by evict                   # Report all other modules used by the log module
./modularity_check.py list_cycles conn                       # Report all dependency cycles up to length 3 that include conn/
./modularity_check.py explain_cycle "['log', 'meta', 'txn']" # Explain why the provided depenency cycle exists
./modularity_check.py privacy_report txn                     # Report which structs and fields in a module are private
./modularity_check.py generate_dependency_file               # WIP: Generate a text representation of the dependency graph. Can be used to detect dependency changes over time.
```

## Important notes:
- This script is not perfect!!! Please see the known issues below
- `header_mapping.py` makes an opinionated stand on which files belong to which modules
- `checksum` code has been merged into one module
- The `os_*` folders have been merged into a single `os_layer` module

## Known issues:
- Macros are not part of the C grammar, so tree-sitter struggles to deal with them. It's important to call out even though macros aren't part of the grammer the `tree-sitter-c` library does a very good job at working around this. Nonetheless this means a lot of hacking was required to make our source code compatible. See `parse_wt_ast.py::preprocess_file()` for said hackery.
- This script just parses the AST so there's no semantic data associated with fields. As such field accesses are mapped to their owning struct by their unique names. If a field is defined in two structs it is ambigous and can't be mapped. When this happens it is instead linked to an `Ambiguous linking or parsing failed` node in the graph to be reported to the user.
- There are some know incorrect parsing results, for example `who_is_used_by log` reports that log calls the function `(*func)`, which is actually part of a function_pointer argument passed used by `__wt_log_scan`. This may be fixed in the future, but for now is an acceptable innaccuracy in the tool.

## How to work with the script:
- `modularity_check` is the entry point
- `networkx` builds the graph
- `tree-sitter` parses the C code

TODO - Add a note on interacting with tree-sitter. In particular
- Understanding `print(node)`
- Tree Sitter uses bytes. To convert to a readable format use `node.text.decode()`
- The common interactions are `for child in node.children` and `node.child_by_field_name("field_name")`. You can use the results of `print(node)` to understand the field names (strings followed by `:`)