#!/usr/bin/env python

from __future__ import print_function
import re, sys
from dist import all_c_files, all_h_files, compare_srcfile

# Automatically build flags values: read through all of the header files, and
# for each group of flags, sort them and give them a unique value.
#
# To add a new flag declare it at the top of the flags list as:
# #define WT_NEW_FLAG_NAME      0x0u
#
# and it will be automatically alphabetized and assigned the proper value.
def flag_declare(name):
    tmp_file = '__tmp'
    with open(name, 'r') as f:
        tfile = open(tmp_file, 'w')

        lcnt = 0
        parsing = False
        start_flag = 0
        for line in f:
            lcnt = lcnt + 1
            if line.find('AUTOMATIC FLAG VALUE GENERATION START') != -1:
                m = re.search("\d+", line)
                if m == None:
                    print(name + ": automatic flag generation start line " + str(lcnt) +\
                        "contains no start value", file=sys.stderr)
                    sys.exit(1)
                start_flag = int(m.group(0))
                header = line
                defines = []
                parsing = True
            elif line.find('AUTOMATIC FLAG VALUE GENERATION STOP') != -1:
                m = re.search("\d+", line)
                if m == None:
                    print(name + ": automatic flag generation stop line " + str(lcnt) +\
                        "contains no stop value", file=sys.stderr)
                    sys.exit(1)
                limit = int(m.group(0))
                if len(defines) > limit:
                    print(name + ": line " + str(lcnt) +\
                          ": exceeds maximum {0} limit bit flags".format(limit), file=sys.stderr)
                    sys.exit(1)

                # Calculate number of hex bytes, create format string
                fmt = "0x%%0%dxu" % ((start_flag + len(defines) + 3) / 4)

                # Generate the flags starting from an offset set from the start value.
                tfile.write(header)
                v = 1 << start_flag
                for d in sorted(defines):
                    tfile.write(re.sub("0x[01248u]*", fmt % v, d))
                    v = v * 2
                tfile.write(line)

                parsing = False
                start_flag = 0
            elif parsing and line.find('#define') == -1:
                print(name + ": line " + str(lcnt) +\
                      ": unexpected flag line, no #define", file=sys.stderr)
                sys.exit(1)
            elif parsing:
                defines.append(line)
            else:
                tfile.write(line)

        tfile.close()
        compare_srcfile(tmp_file, name)

# Update function argument declarations.
for name in all_h_files():
    flag_declare(name)
for name in all_c_files():
    flag_declare(name)
