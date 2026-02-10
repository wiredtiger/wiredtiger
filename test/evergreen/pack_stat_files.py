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
import tarfile

def targz_pack_stat_files(destination_dir, source_dir, regex):
    target = os.path.join(destination_dir, source_dir[2:].replace("/", "\\") + "\WiredTigerStats.tar.gz")
    with tarfile.open(target, "w:gz") as tar:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if regex.match(file):
                    file_path = os.path.join(root, file)
                    tar.add(file_path, arcname=os.path.relpath(file_path, source_dir))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--destination-dir', required=True, help='Directory to store the packed stat files')
    args = parser.parse_args()

    destination_dir = args.destination_dir

    regex = re.compile(r'WiredTigerStat.*')
    for root, _, files in os.walk("."):
        for file in files:
            if regex.match(file):

                # If current directory contains any stat files, pack them all into a tar.gz (one archive per directory).
                targz_pack_stat_files(destination_dir, root, regex)

                break

if __name__ == "__main__":
    main()
