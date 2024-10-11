# Implementation Details

The core concept of the library is the "Fat Token" which represents a higher-level abstraction of elements in C code rather than a simple sequence of characters. A Fat Token can represent items such as "words", "comments", "strings", "expressions within parentheses", etc.

With this this higher-level representation, the library enables a layered approach to parsing, focusing on high level structures without getting bogged down in unnecessary details. The parser extracts Fat Tokens from the C code and groups them into "statements" representing logical units like function definitions, struct declarations, variable declarations, etc.

For example, a function definition might be represented as a sequence of: "comment", "word", "word", "expression in parentheses", and "expression in curly braces". We can recognize this as a function definition without needing to parse the internal details of the function body or arguments list.

An application's template using this library might look like this:

```python
def main():
    setModules([
        Module("block"),
        Module("block_cache", sourceAliases = ["blkcache", "bm"]),
        Module("conn", fileAliases=["connection"], sourceAliases=["connection"]),
        Module("txn"),
        ...
    ])
    setRootPath(sys.argv[1])
    files = get_files()
    _globals = Codebase()
    _globals.scanFiles(files, twopass=True, multithread=True)
    AccessCheck(_globals).checkAccess(multithread=True)

if __name__ == "__main__":
    main()
```

## Classes

Most classes include `preComment` and `postComment` fields, which store the comments appearing before or after the associated entity.

### Basic Tokens

* **`Token`**

  A `Token` represents a single unit, or "Fat Token," in C code:

  * Spaces
  * Newlines (used to track same-line comments)
  * Comments (both single-line and multi-line)
  * Preprocessor directives
  * C string literals (single-quoted and double-quoted)
  * C operators (e.g., `+`, `-`, `*`, `/`, `==`, `!=`, `&&`, `||`, etc.)
  * Words (sequences of word-like characters)
  * Anything enclosed in parentheses `()`, braces `{}`, or brackets `[]`

  Each Token also includes information about its location in the source code (offset from the start), for calculation of line and column numbers. This information can be used to automate code editing.

### Statements

* **`Statement`**

  A `Statement` is a group of Tokens that represent a logical unit of code, such as a function definition, struct definition, variable declaration, etc.

  Statements are formed by identifying logical boundaries between Tokens. For instance, some sequences might end with a semicolon, while others conclude with a curly brace. Additionally, there is logic to associate comments with the appropriate statements.

* **`StatementKind`**

  `StatementKind` detects and categorizes the type of statement. The following kinds are defined:

  * `comment` – a comment.
  * `preproc` – a preprocessor directive.
  * `typedef` – a typedef.
  * `record` – a struct, union, or enum definition.
  * `function` – a function definition or declaration.
  * `function_def` – a function definition.
  * `function_decl` – a function declaration.
  * `statement` – a general statement, like a `do`/`while` loop.
  * `decl` – a variable declaration.
  * `expression` – a C expression.
  * `initialization` – a variable initialization.
  * `extern_c` – an `extern "C"` block.
  * `unnamed_record` – an unnamed struct or union, which is a special case where the members are pulled up into the parent record.

### C Language Elements

These objects are created from `Statement` objects that indicate corresponding type of statement.

* **`Variable`** – Represents a variable declaration. It includes:
  * `name` – the variable name.
  * `typename` – the C type of the variable.

* **`FunctionParts`** – Represents the components of a function definition. It includes:
  * `typename` – the return type of the function.
  * `name` – the function name.
  * `args` – the function arguments.
  * `body` – the function body (as text).
  * `getArgs()` – retrieves a list of arguments as `Variable` objects.
  * `getLocalVars()` – retrieves a list of local variables as `Variable` objects.

* **`RecordParts`** – Represents the components of a struct (or union) definition. It includes:
  * `recordKind` – the type of record (struct, union, enum).
  * `name` – the record name.
  * `typename` – the record’s type name (which can be auto-generated).
  * `body` – the record body (as text).
  * `members` – the record members, represented as `Variable` objects.
  * `typedefs` – types defined by this record, as `Variable` objects.
  * `vardefs` – variables defined by this record, as `Variable` objects.
  * `nested` – nested records, represented as `RecordParts` objects.
  * `parent` – the parent record, if this record is nested.
  * `is_unnamed` – whether this is an unnamed record (struct or union).

   Constructs like "`typedef struct`" are also considered record definitions.

### Macros

* **`MacroParts`** – Represents the components of a macro definition. It includes:
  * `name` – the macro name.
  * `args` – the macro arguments (if any).
  * `body` – the macro body (as text).
  * `is_va_args` – whether the macro is variadic.
  * `is_wellformed` – indicates if the macro expands to a well-formed C construct (i.e., no incomplete braces or similar issues).
  * `is_const` – indicates if the macro expands to a literal constant.

* **`MacroExpander`** – Handles the expansion of C macros. It includes:
  * `expand(txt, expand_const)` – expands all macros within the provided text. If `expand_const` is `False`, literal constant macros are not expanded, saving computation.
  * `insert_list` – a list of offsets and deltas after macro expansion, used for adjusting line numbers.

### Workspace and Code Environment

* **`Module`** – Describes a module. It includes:
  * `name` – the module name.
  * `dirname` – the module's directory.
  * `fileAliases` - aliases for file names and paths.
  * `sourceAliases` - aliases for source code elements.

* **`setModules(list[Module])`** – Sets the list of modules.

* **`setRootPath(path)`** – Sets the root path for the codebase.

* **`get_files()`**, **`get_h_files()`**, **`get_h_inline_files()`**, **`get_c_files()`** – Retrieves lists of files based on their type (e.g., all files, header files, inline header files, source files).

* **`fname_to_module()`** – Retrieves the module name based on the file name.

* **`File`** – Describes a file. It includes:
  * `name` – the file name.
  * `module` – the module name associated with the file.
  * `is_private` – indicates whether the file is private.
  * `lineOffsets` – offsets of lines in the file, used for line number calculations.
  * `fileKind` – the type of file (e.g., header, source, etc.).

* **`Scope`** – Represents a scope within a file (e.g., an `extern "C"` block, or a function's/struct's scope). It includes:
  * `file` – the file object.
  * `offset` – the offset in the file for line number calculation. Tokens store offsets relative to the scope.

### Code Facts collection

* **`Definition`** – Represents the definition of an entity in the code. It includes:
  * `name` – the name of the entity.
  * `kind` – the type of entity (e.g., function, variable, record).
  * `scope` – the scope object where the entity is defined.
  * `offset` – the offset in the file for line number calculation.
  * `module` – the name of the module to which the entity belongs.
  * `is_private` – whether the entity is private.
  * `details` – additional details about the entity (e.g., `FunctionParts`, `Variable`, etc.).

* **`Codebase`** – Represents a collection of definitions extracted from the code. It includes:

  * For records (structs, unions, enums):
    * `types` – a dictionary of type definitions.
    * `types_restricted` – the same type definitions where access is restricted.
    * `fields` – a dictionary of field definitions.

  * For functions, variables, other identifiers:
    * `names` – a collection of identifier names.
    * `names_restricted` – identifier names with restricted access.
    * `static_names` – file-local static identifiers.

  * For typedefs:
    * `typedefs` – typedef mappings.

  * For macros:
    * `macros` – collection of macros.

  Functions:

  * `untypedef(name)` – resolves type aliases to their base type.
  * `get_field_type(rec_type, field_name)` – retrieves the type of a field in a record.
  * `scanFiles(files, twopass, multithread)` – scans files for definitions.
    * `twopass` – do a two-pass scan: first to collect macros, then to scan sources with macros expanded.
    * `multithread` – use multithreaded scanning.

### Modularity Access Check

* **`AccessCheck(Codebase)`** – A checker for modularity access rules.
  * `checkAccess(multithread)` – Checks access rules for all definitions in the codebase.

### Error Output and Logging

* **`LOG(level, location, ...)`** – Logs a message with a specified level.

* Log levels are: `QUIET`, `FATAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`, `DEBUG2`, `DEBUG3`, `DEBUG4`, `DEBUG5`. Each log level has a corresponding function.
