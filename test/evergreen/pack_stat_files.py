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

import argparse
import os
import re
import shutil
import tarfile

def collect_stat_files(destination_dir, source_dir, regex):

    dir_prefix = os.path.join(destination_dir, source_dir[2:].replace("/", "-"))

    if not os.path.exists(dir_prefix):
        os.makedirs(dir_prefix)

    for item in os.listdir(source_dir):
        path = os.path.join(source_dir, item)
        if os.path.isfile(path) and regex.match(item):
            file_path = os.path.join(source_dir, item)
            shutil.copy(file_path, destination_dir)
            shutil.move(os.path.join(destination_dir, item), dir_prefix + '/' + item)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--destination-dir', required=True, help='Directory to collect stat files into for packing.')
    args = parser.parse_args()

    destination_dir = args.destination_dir

    regex = re.compile(r'WiredTigerStat.*')
    for root, _, files in os.walk("."):
        if root == "./" + destination_dir: continue
        for file in files:
            if regex.match(file):

                # If current directory contains any stat files, collect them all into a single location for packing.
                collect_stat_files(destination_dir, root, regex)

                break   # Finished searching in this directory, move on to the next one.

if __name__ == "__main__":
    main()
