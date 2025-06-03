#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
import os
import re
# This script must be run from WT root directory or may lead to inconsistent results.

# Loop through all subdirectories recursively and search tsan logs. 
tsan_warnings_set = set()
for dirpath, _, filenames in os.walk("."):  
    for filename in filenames:  
        # Check if the file starts with "tsan."  
        if filename.startswith("tsan."):
            full_path = os.path.join(dirpath, filename)  
            with open(full_path, "r") as file:
                for line in file:
                    if (not line.startswith("SUMMARY:")):
                        continue
                    # Strip away the unnecessary information
                    pattern_to_remove = r"/data/mci/.*/wiredtiger/"  
                    cleaned_text = re.sub(pattern_to_remove, "", line).strip() 

                    tsan_warnings_set.add(cleaned_text)

print("\n".join(tsan_warnings_set)) 
print(f"Overall TSAN Warnings: {len(tsan_warnings_set)}")
