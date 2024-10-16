# Rules for Modularity

## Definition of a Module

* Whatever resides in a subdirectory under `src/` is considered to be a module.

* The module names are pre-configured as a list of subdirectories under `src/`, with some exclusions like `include`, `checksum`, `os*`, etc. These exclusions are because these directories don’t really contain modular code. If the module name, as deduced by any method, is not in this list, there is no associated module, and the content is exempt from modularity checks.

* Modules can have aliases. `block_cache` refers to `blkcache`, or `connection` is the same as `conn`.

## Modules Content and Visibility Rules

### 1. File Name/Path-Based Rules

1. The default module for the content of a file is defined by these rules:
   1. If the file is not in the `include` directory, the module is the topmost subdirectory name after `src/`.
   2. For files in `include/`, the module name is derived from the file name with `.h` and `_inline` stripped.

2. If the file name contains `_private`, everything in that file is considered private by default. Otherwise, everything is public by default.

### 2. Identifier Name-Based Rules

1. If the name starts with the `__wt_` prefix, the content is considered public.
2. If the name starts with the `__wti_` prefix, the content is private.
3. If the `__wt_` or `__wti_` prefix is followed by a valid module name and an underscore, the content is considered part of that module.

This applies to all identifiers like function or struct names, record members, variables, type names, etc.

### 3. Comment Tag-Based Rules

1. A comment is considered to describe an entity if it precedes the entity or follows it on the same line.
2. If the entity’s comment includes `#public` or `#private`, the visibility is set accordingly and it belongs to the current context’s module.
3. If the entity’s comment includes `#public(module)` or `#private(module)`, it gets the corresponding visibility and belongs to the specified module.

### 4. Nested Declarations/Definitions

1. Structs and unions can be nested. If a declaration is nested within an outer record, it inherits the visibility and module from the outer record.

### 5. Rule Precedence

1. Comment tags take the highest priority and also work as an escape hatch to override any other rules that are less explicit.
2. Visibility derived from the identifier name has higher priority than the one derived from the file name. This allows both private and public declarations within the same file.
3. The module name derived from the file name has higher precedence than the one from the identifier because the top-level declarations should likely belong to the file's module. If there's a conflict, an error is generated, as the identifier name suggests the wrong module where the symbol belongs.
4. If there are multiple declarations, the existing annotation overrides an absent one. For instance, if a function is forward-declared in a common `.h` file which is not bound to any module and the same function is defined in a module-bound source file, the function is deemed to belong to that module.
