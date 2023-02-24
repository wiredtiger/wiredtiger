#!/usr/bin/env python3

import re

"""
* docstring: The documentation to attach to the generated function.
* name: The name to use for the generated function, i.e. `__wt_name_str`.
* define_regex: The regular expression to use for capturing the names of the
  macros. Note that this must also include the capture group.
* srcfile: The source file to look through when searching for `define_regex`.
* out: The output file handle.
"""
def generate_string_conversion(docstring, name, define_regex, srcfile, out):
    srcfile = '../' + srcfile
    pattern = re.compile(define_regex)

    out.write('/*\n')
    out.write(' * __wt_{}_str --\n'.format(name))
    out.write(' *     {}\n'.format(docstring))
    out.write(' */\n')
    out.write('static inline const char *\n')
    out.write('__wt_{}_str(uint8_t val)\n'.format(name))
    out.write('{\n')
    out.write('    switch (val) {\n')

    found = False
    with open(srcfile, 'r') as src:
        for line in src:
            matches = pattern.search(line)
            if matches:
                found = True
                out.write('    case {}:\n'.format(matches.group(1)))
                out.write('        return ("{}");\n'.format(matches.group(1)))

    if not found:
        raise Exception("generate_string_conversion: Failed to find {} in {}"
                        .format(define_regex, srcfile))

    out.write('    }\n')
    out.write('\n')
    out.write('    return ("{}_INVALID");\n'.format(name.upper()))
    out.write('}\n')

if __name__ == '__main__':
    with open('../src/include/str_inline.h', 'w+') as out:
        generate_string_conversion('Convert a prepare state to its string representation.',
                                   'prepare_state',
                                   r'^#define\s+(WT_PREPARE_[A-Z0-9_]+)\s+[0-9]+',
                                   'src/include/btmem.h',
                                   out)
        out.write('\n')

        generate_string_conversion('Convert an update type to its string representation.',
                                   'update_type',
                                   # Also match comments/space at end to avoid e.g. WT_UPDATE_SIZE.
                                   r'^#define\s+(WT_UPDATE_[A-Z0-9_]+)\s+[0-9]+\s+/\*',
                                   'src/include/btmem.h',
                                   out)
