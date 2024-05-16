#!/usr/bin/env python3
import os, re, sys, textwrap

def get_name(type, func):
    return "\"" + type + "::"   + func + "\""

api_name_lookup = ["const char *WT_API_NAMES[] = {"]
api_name_line_count = 0

# Cursed code that outputs the string array in the correctly formatted fashion.
def append_api_name(append, first):
    global api_name_lookup
    global api_name_line_count
    newline = False
    if (len(api_name_lookup[api_name_line_count]) % 100 + len(append)) > 97:
        api_name_lookup[api_name_line_count] += ",\n"
        api_name_line_count += 1
        newline = True
    if not newline:
        if first:
            api_name_lookup[api_name_line_count] += append
        else:
            api_name_lookup[api_name_line_count] += ", " + append
    else:
        api_name_lookup.append("  " + append)

func_count = 0
api_string = None

first = True
with open('../src/include/api_func_map.h', "w") as api_func_h:
    api_func_h.write("/* This is an auto generated file, do not touch! */\n")
    api_func_h.write("#pragma once\n")
    with open('../src/include/wiredtiger.in') as api_def:
        for line in api_def:
            struct = re.search(r"struct ([a-z_]+) \{", line)
            if struct is not None:
                if re.search(r"__wt_connection", struct.group(1)):
                    api_string = "WT_CONNECTION"
                elif re.search(r"__wt_session", struct.group(1)):
                    api_string = "WT_SESSION"
                elif re.search(r"__wt_cursor", struct.group(1)):
                    api_string = "WT_CURSOR"
                else:
                    api_string = None
            func = re.search(r"__F\(([a-z_]+)\)", line)
            if func is not None and api_string is not None:
                # Exclude the macro defining __F
                if re.search(r"^func", func.group(1)):
                    continue
                # We've found an API function.
                api_func_h.write("#define WT_API_" + api_string + "_" + func.group(1) + " " + str(func_count) + "\n")
                append_api_name(get_name(api_string, func.group(1)), first)
                if first:
                    first = False
                func_count += 1
                # Handle some fake API wiredtiger has.
                if api_string == "WT_CURSOR" and func.group(1) == "insert":
                    api_func_h.write("#define WT_API_WT_CURSOR_insert_check " + str(func_count) + "\n")
                    append_api_name(get_name('WT_CURSOR', 'insert_check'), False)
                    func_count += 1
    api_func_h.write("#define WT_API_COUNT " + str(func_count) + "\n")
    api_func_h.write('extern const char* WT_API_NAMES[];\n')

api_name_lookup[api_name_line_count] += "};\n"
with open('../src/config/api_func_map.c', 'w') as api_func_c:
    for line in api_name_lookup:
        api_func_c.write(line)




