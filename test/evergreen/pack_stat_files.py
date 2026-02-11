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

    target_dir = os.path.join(destination_dir, source_dir[2:]) # Remove the leading "./" from source_dir

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if regex.match(file):
                file_path = os.path.join(root, file)
                shutil.copy(file_path, target_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--destination-dir', required=True, help='Directory to collect stat files into for packing.')
    args = parser.parse_args()

    destination_dir = args.destination_dir

    regex = re.compile(r'WiredTigerStat.*')
    for root, _, files in os.walk("."):
        for file in files:
            if regex.match(file):

                # If current directory contains any stat files, collect them all into a single location for packing.
                collect_stat_files(destination_dir, root, regex)

                break   # Finished searching in this directory, move on to the next one.

    # Pack the collected stat files into a tarball for uploading to S3.
    with tarfile.open(destination_dir + ".tar.gz", "w:gz") as tar:
        tar.add(destination_dir, arcname=os.path.basename(destination_dir))

if __name__ == "__main__":
    main()
