#!/usr/bin/env python

import os, re, sys, textwrap
import api_data
from dist import compare_srcfile

# Temporary file.
tmp_file = '__tmp'

# Parse out the set of APIs from wiredtiger.in
f='../src/include/wiredtiger.in'
handle_wrapper_re = re.compile(r'^struct __(wt_[a-z_]*) {')
function_name_re = re.compile(r'.*__F\(([a-z_]*)\).*')

# Generate the set of APIs
op_track_defines = ''
current_handle = ''
slot=-1
for line in open(f, 'r'):
    m = handle_wrapper_re.match(line)
    if m:
        current_handle = m.group(1)
        current_handle = current_handle.upper()
        continue

    # Skip any lines until we hit the declarations (including the definition
    # of the __F macro.
    if current_handle == '':
        continue

    m = function_name_re.match(line)
    if not m:
        continue
    method = m.group(1)

    slot += 1
    name = 'WT_OP_TYPE_'+ current_handle + '_' + method
    op_track_defines +=\
        '#define\t' + name + '\t' * \
            max(1, 6 - (len('WT_OP_TYPE_' + name) / 8)) + \
            "%2s" % str(slot) + '\n'

tfile = open(tmp_file, 'w')
skip = 0
for line in open('../src/include/flags.h', 'r'):
    if skip:
        if line.count('operation tracking section: END'):
            tfile.write('/*\n' + line)
            skip = 0
    else:
        tfile.write(line)
    if line.count('operation tracking section: BEGIN'):
        skip = 1
        tfile.write(' */\n')
        tfile.write(op_track_defines)
tfile.close()
compare_srcfile(tmp_file, '../src/include/flags.h')
