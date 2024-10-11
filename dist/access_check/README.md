# "Fat Token" Parser and Modularity Checker for C Code

This library provides a parser for C code that extracts tokens and checks the modularity of the code based on the rules defined in the [Modularity](MODULARITY.md) document.

The core concept of the library is the "Fat Token" which represents a higher-level abstraction of elements in C code rather than a simple sequence of characters. A Fat Token can represent items such as "words", "comments", "strings", "expressions within parentheses", etc.

With this this higher-level representation, the library enables a layered approach to parsing, focusing on high level structures without getting bogged down in unnecessary details. The parser extracts Fat Tokens from the C code and groups them into "statements" representing logical units like function definitions, struct declarations, variable declarations, etc.

For example, a function definition might be represented as a sequence of: "comment", "word", "word", "expression in parentheses", and "expression in curly braces". We can recognize this as a function definition without needing to parse the internal details of the function body or arguments list.

This approach makes it possible to perform a variety of analyses on C code or other languages with clear syntax rules.

Implementation details are described in the [Implementation](IMPL.md) document.
