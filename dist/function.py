#!/usr/bin/env python

# Check the style of WiredTiger C code.
import fnmatch, os, re, sys
from dist import all_c_files, compare_srcfile, source_files

# Complain if a function comment is missing.
def missing_comment():
    for f in source_files():
        skip_re = re.compile(r'DO NOT EDIT: automatically built')
        func_re = re.compile(
            r'(/\*(?:[^\*]|\*[^/])*\*/)?\n\w[\w \*]+\n(\w+)', re.DOTALL)
        s = open(f, 'r').read()
        if skip_re.search(s):
            continue
        for m in func_re.finditer(s):
            if not m.group(1) or \
               not m.group(1).startswith('/*\n * %s --\n' % m.group(2)):
                   print "%s:%d: missing comment for %s" % \
                           (f, s[:m.start(2)].count('\n'), m.group(2))

# Strip a prefix, used to remove "const" and "volatile" from declarations.
def function_args_strip(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

# Return the sort order of a variable declaration, or no-match.
#       This order isn't defensible: it's roughly how WiredTiger looked when we
# settled on a style, and it's roughly what the KNF/BSD styles look like.
def function_args(line):
    types = {
        'u_int16_t ' : -1,      # Not allowed
        'u_int32_t ' : -1,      # Not allowed
        'u_int64_t ' : -1,      # Not allowed
        'u_int8_t ' : -1,       # Not allowed
        'u_quad ' : -1,         # Not allowed
        'uint ' : -1,           # Not allowed
        'struct ' : 1,          # Types in argument display order. */
        'union ' : 2,
        'enum ' : 3,
        'WT_' : 4,
        'double ' : 5,
        'float ' : 6,
        'size_t ' : 7,
        'uint64_t ' : 8,
        'int64_t ' : 9,
        'uint32_t ' : 10,
        'int32_t ' : 11,
        'uint8_t ' : 12,
        'int8_t ' : 13,
        'u_int ' : 14,
        'int ' : 15,
        'u_char ' : 16,
        'char ' : 17,
        'bool ' : 18,
        'va_list ' : 19,
        'void ' : 20,

    }
    line = line.strip()
    line = function_args_strip(line, "const")
    line = function_args_strip(line, "volatile")

    # Let WT_UNUSED terminate the parse. It often appears at the beginning
    # of the function and looks like a WT_XXX variable declaration.
    if re.search('^WT_UNUSED', line):
        return False,0
    for m in types:
        # Don't list '{' as a legal character in a declaration, that's what
        # prevents us from sorting inline union/struct declarations.
        if re.search('^' + m + "\s*[\w(*]", line):
            return True,types[m]
    return False,0

# Put function arguments in correct sort order.
def function_declaration():
    tmp_file = '__tmp'
    for name in all_c_files():
        skip_re = re.compile(r'DO NOT EDIT: automatically built')
        s = open(name, 'r').read()
        if skip_re.search(s):
            continue

        # Read through the file, and for each function, do a style pass over
        # the local declarations. Quit tracking declarations as soon as we
        # find anything we don't understand, leaving it untouched.
        with open(name, 'r') as f:
            tfile = open(tmp_file, 'w')
            tracking = False
            for line in f:
                if not tracking:
                    tfile.write(line)
                    if re.search('^{$', line):
                        r = [[] for i in range(30)]
                        tracking = True;
                    continue

                found,value = function_args(line)
                if value == -1:
                    print >>sys.stderr, \
                        name + ": illegal declaration: " + line.strip()
                    sys.exit(1)
                if value == 0 or not found:
                    for arg in filter(None, r):
                        for p in sorted(arg): tfile.write(p)
                    tfile.write(line)
                    tracking = False
                    continue
                r[value].append(line)

            tfile.close()
            compare_srcfile(tmp_file, name)

# Report missing function comments.
#missing_comment()

# Update function argument declarations.
function_declaration()
