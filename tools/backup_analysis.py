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

# Compare WT data files in two different home directories.

import fnmatch, glob, os, sys, time

total_blocks_global = 0
total_bytes_global = 0
granularity = 0
pct20 = granularity // 5
pct80 = pct20 * 4
# For now just have replicated collections and indexes and everything else. Other considerations
# for more detailed types could be:
# * a type just for the 'oplog'
# * a type for 'local' collections and indexes (together, probably not enough to be separated)
# * a type for 'system' tables (i.e. anything not WT-owned and not beginning with collection or index)
# * a type for 'wt' tables (i.e. anything WT-owned, so the history store)
# This should be 1:1 between the names and types.
global_names = ['Replicated Collections', 'Replicated Indexes', 'Other tables']
global_types = ['coll', 'index', 'other']

blocks_global = dict()
bytes_global = dict()
files_changed = dict()
files_global = dict()
gran_blocks_global = dict()
pct20_global = dict()
pct80_global = dict()

for t in global_types:
    blocks_global[t] = 0
    bytes_global[t] = 0
    files_changed[t] = 0
    files_global[t] = 0
    gran_blocks_global[t] = 0
    pct20_global[t] = 0
    pct80_global[t] = 0

def usage_exit():
    print('Usage: backup_analysis.py dir1 dir2 [granularity]')
    print('  dir1 and dir2 are POSIX pathnames to WiredTiger backup directories.')
    print('  dir1 is assumed to be an older/earlier backup to dir2.')
    print('Options:')
    print('  granularity - an (optional) positive integer indicating the granularity')
    print('  size in integer number of bytes of the incremental backup blocks.')
    print('  The default is 16777216 (16MB).')
    sys.exit(1)

def die(reason):
    print('backup_analysis.py: error: ' + reason, file=sys.stderr)
    sys.exit(1)

# Check that the directory given is a backup. Look for the WiredTiger.backup file.
def check_backup(mydir):
    if not os.path.isdir(mydir):
        return False
    backup_file = mydir + "/WiredTiger.backup"
    if not os.path.exists(backup_file):
        return False
    return True

def compare_file(dir1, dir2, filename, cmp_size):
    global blocks_global
    global bytes_global
    global files_changed
    global files_global
    global global_names
    global global_types
    global gran_blocks_global
    global granularity
    global pct20
    global pct20_global
    global pct80
    global pct80_global
    global total_blocks_global
    global total_bytes_global

    f1_size = os.stat(os.path.join(dir1, filename)).st_size
    f2_size = os.stat(os.path.join(dir2, filename)).st_size
    min_size = min(f1_size, f2_size)
    #
    # Figure out what kind of table this file is. For now just go off the file name.
    # FIXME-WT-12396 when we get to parsing the backup file we want to send in the metadata string
    # here and look for "log=(enabled=false)" to know whether it is a replicated collection. Any
    # collection without that (i.e. logging enabled, the default) should go under other at that time.
    #
    # FIXME-WT-12396 when we parse the backup file, we can know which table is the oplog by looking
    # for a file:collection table that has app_metadata with the string "oplog" in it (the whole
    # string is "oplogKeyExtractionVersion" but "oplog" is likely least brittle in case the MongoDB
    # ever changes the name. When that happens we can consider adding a special 'oplog' type to the
    # set of counts we keep track of.
    #
    if "collection" in filename:
        count_type = 'coll'
    elif "index" in filename:
        count_type = 'index'
    else:
        count_type = 'other'
    files_global[count_type] += 1

    print(f'COMPARE FILE {filename}: Granularity {granularity} 20% {pct20} 80% {pct80}')
    # Initialize all of our counters per file.
    bytes_gran = 0
    gran_blocks = 0
    num_cmp_blocks = min_size // cmp_size
    offset = 0
    partial_cmp = min_size % cmp_size
    pct20_count = 0
    pct80_count = 0
    start_off = offset
    total_bytes_diff = 0
    fp1 = open(os.path.join(dir1, filename), "rb")
    fp2 = open(os.path.join(dir2, filename), "rb")
    # Time how long it takes to compare each file.
    start = time.asctime()
    # Compare the bytes in cmp_size blocks between both files.
    print("")
    for b in range(0, num_cmp_blocks + 1):
        # Compare the two blocks. We know both files are at least min_size so all reads should work.
        buf1 = fp1.read(cmp_size)
        buf2 = fp2.read(cmp_size)
        # If they're different, gather information.
        if buf1 != buf2:
            total_bytes_diff += cmp_size
            total_bytes_global += cmp_size
            # Count how many granularity level blocks changed.
            if bytes_gran == 0:
                gran_blocks += 1
                gran_blocks_global[count_type] += 1
                total_blocks_global += 1
            bytes_gran += cmp_size
            bytes_global[count_type] += cmp_size
        # Gather and report block information when we cross a granularity boundary or we're on
        # the last iteration.
        offset += cmp_size
        if offset % granularity == 0 or b == num_cmp_blocks:
            if bytes_gran != 0:
                print(f'{filename}: offset {start_off}: {bytes_gran} bytes differ in {granularity} bytes')
            # Account for small or large block changes.
            if bytes_gran != 0:
                if bytes_gran <= pct20:
                    pct20_count += 1
                    pct20_global[count_type] += 1
                elif bytes_gran >= pct80:
                    pct80_count += 1
                    pct80_global += 1
            # Reset for the next granularity block.
            start_off = offset
            bytes_gran = 0

    # Account for any partial blocks.
    if partial_cmp != 0:
        buf1 = fp1.read(partial_cmp)
        buf2 = fp2.read(partial_cmp)
        # If they're different, gather information.
        if buf1 != buf2:
            total_bytes_diff += partial_cmp
            total_bytes_global += partial_cmp
            bytes_gran += partial_cmp
            bytes_global[count_type] += partial_cmp
            part_bytes = offset + partial_cmp - start_off
            print(f'{filename}: offset {start_off}: {bytes_gran} bytes differ in {part_bytes} bytes')
    fp1.close()
    fp2.close()
    end = time.asctime()

    # Report for each file.
    print(f'{filename}: time: started {start} completed {end}')
    if f1_size < f2_size:
        change = "grew"
        change_diff = f2_size - f1_size
    elif f1_size > f2_size:
        change = "shrank"
        change_diff = f1_size - f2_size
    else:
        change = "remained equal"
        change_diff = 0
    if change_diff != 0:
        print(f'{filename}: size: {f1_size} {f2_size} {change} by {change_diff} bytes')
    else:
        print(f'{filename}: size: {f1_size} {f2_size} {change}')
    print(f'{filename}: common: {min_size} differs by {total_bytes_diff} bytes in {gran_blocks} granularity blocks')
    if gran_blocks != 0:
        files_changed[count_type] += 1
        pct20_blocks = round(abs(pct20_count / gran_blocks * 100))
        pct80_blocks = round(abs(pct80_count / gran_blocks * 100))
        print(f'{filename}: smallest 20%: {pct20_count} of {gran_blocks} blocks ({pct20_blocks}%) differ by {pct20} bytes or less of {granularity}')
        print(f'{filename}: largest 80%: {pct80_count} of {gran_blocks} blocks ({pct80_blocks}%) differ by {pct80} bytes or more of {granularity}')

#
# Print a detailed summary of all of the blocks and bytes over all of the files. We accumulated
# the changes as we went through each of the files.
#
def print_summary():
    print("")
    print('SUMMARY')
    print(f'Total: {total_bytes_global} bytes changed in {total_blocks_global} blocks')
    # Walk through all the types printing out final information per type.
    for n, t in zip(global_names, global_types):
        print(f'{n}: {files_changed[t]} of {files_global[t]} changed')
        if gran_blocks_global[t] != 0:
            if files_changed[t] == 1:
                word = 'file'
            else:
                word = 'files'
            print(f'{files_changed[t]} changed {word}: differs by {blocks_global[t]} granularity blocks in {gran_blocks_global[t]} total granularity blocks')
            print(f'{files_changed[t]} changed {word}: differs by {bytes_global[t]} bytes in {gran_blocks_global[t]} total granularity blocks')
            pct20_blocks = round(abs(pct20_global[t] / gran_blocks_global[t] * 100))
            pct80_blocks = round(abs(pct80_global[t] / gran_blocks_global[t] * 100))
            print(f'{n}: smallest 20%: {pct20_blocks} of {gran_blocks_global[t]} blocks ({pct20_blocks}%) differ by {pct20} bytes or less of {granularity}')
            print(f'{n}: largest 80%: {pct80_blocks} of {gran_blocks_global[t]} blocks ({pct80_blocks}%) differ by {pct80} bytes or more of {granularity}')

def compare_backups(dir1, dir2):
    files1=set(fnmatch.filter(os.listdir(dir1), "*.wt"))
    files2=set(fnmatch.filter(os.listdir(dir2), "*.wt"))

    common = files1.intersection(files2)
    # For now assume the first directory is the older one. Once we add functionality to parse the
    # text in WiredTiger.backup we can look at the checkpoint timestamp of a known table like the
    # history store and figure out which is older.
    # NOTE: Update the assumption in the usage statement also when this changes.
    for file in files1.difference(files2):
        print(file + ": dropped between backups")
    for file in files2.difference(files1):
        print(file + ": created between backups")
    #print(common)
    for f in common:
        # For now we're only concerned with changed blocks between backups.
        # So only compare the minimum size both files have in common.
        # FIXME: More could be done here to report extra blocks added/removed.
        compare_file(dir1, dir2, f, 4096)

def backup_analysis(args):
    global granularity
    global pct20
    global pct80

    if len(args) < 2:
        usage_exit()
    dir1 = args[0]
    dir2 = args[1]
    if len(args) > 2:
        granularity = int(args[2])
    else:
        granularity = 16*1024*1024
    pct20 = granularity // 5
    pct80 = pct20 * 4

    print(f'BACKUP: Granularity {granularity} 20% {pct20} 80% {pct80}')
    if dir1 == dir2:
        print("Same directory specified. " + dir1)
        usage_exit()

    # Verify both directories are backups.
    if check_backup(dir1) == False:
        print(dir1 + " is not a backup directory")
        usage_exit()
    if check_backup(dir2) == False:
        print(dir2 + " is not a backup directory")
        usage_exit()
    # Find the files that are in common or dropped or created between the backups
    # and compare them.
    compare_backups(dir1, dir2)
    print_summary()

if __name__ == "__main__":
    backup_analysis(sys.argv[1:])
