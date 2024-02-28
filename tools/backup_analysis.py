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

import fnmatch, glob, os, re, sys, time

granularity = 0
pct20 = granularity // 5
pct80 = pct20 * 4

# There should be 1:1 matching between the names and types.
global_names = ['Replicated Collections', 'Replicated Indexes', 'OpLog', 'Local tables', 'System tables']
global_types = ['coll', 'index', 'oplog', 'local', 'system']

assert(len(global_names) == len(global_types))
class TypeStats(object):
    def __init__(self, name):
        self.name = name
        self.bytes = 0
        self.files = 0
        self.files_changed = 0
        self.gran_blocks = 0
        self.pct20 = 0
        self.pct80 = 0

typestats = dict()
for n, t in zip(global_names, global_types):
    typestats[t] = TypeStats(n)

def get_metadata(mydir, filename):
    backup_file = mydir + "/WiredTiger.backup"
    uri_file = "file:" + filename
    with open(backup_file) as backup:
        for filekey in backup:
            # Read the metadata for whatever key we're on. We want to read the file in pairs.
            filemeta = next(backup)
            # Check for the URI in the key read. Don't use == because the line contains a newline
            # which negates the exact match and this is good enough.
            if uri_file in filekey:
                return filemeta
    return None

def get_checkpoint_time(meta):
    pattern = 'checkpoint=\(.*time=([^,)]*)[,)]'
    match = re.search(pattern, meta)
    if match == None:
        return 0
    return match.group(1)
    

# Determine which of our two directories is older.
def older_dir(dir1, dir2):
    wtfile = 'WiredTigerHS.wt'
    dir1_meta = get_metadata(dir1, wtfile)
    dir2_meta = get_metadata(dir2, wtfile)
    # Determine which is older by the checkpoint time. 
    time1 = get_checkpoint_time(dir1_meta)
    time2 = get_checkpoint_time(dir2_meta)
    if time1 <= time2:
        return dir1
    else:
        return dir2

# This function is where all of the fragile and brittle knowledge of how MongoDB uses tables
# is kept to analyze the state of the system.
def compute_type(filename, filemeta):
    # Figure out the type of file it is based on the name and metadata.
    # For collections and indexes, if logging is disabled, then they are replicated.
    #   if logging is enabled on an index, it is a local table.
    #   if logging is enabled on a collection, it is a local table unless it has 'oplog' in its
    #   app_private string. There should only be one oplog in a system.
    # Any other file name is a system table.
    disabled = 'log=(enabled=false)'
    is_oplog = 'oplogKeyExtraction'
    if 'collection' in filename:
        if disabled in filemeta:
            type = 'coll'
        elif is_oplog in filemeta:
            type = 'oplog'
        else:
            type = 'local'
    elif 'index' in filename:
        if disabled in filemeta:
            type = 'index'
        else:
            type = 'local'
    else:
        type = 'system'
    assert(type in global_types)
    return type

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

    f1_size = os.stat(os.path.join(dir1, filename)).st_size
    f2_size = os.stat(os.path.join(dir2, filename)).st_size
    min_size = min(f1_size, f2_size)
    #
    # Figure out what kind of table this file is. Use the first directory for basis.
    #
    metadata = get_metadata(dir1, filename)
    assert(metadata != None)
    count_type = compute_type(filename, metadata)
    ts = typestats[count_type]
    ts.files += 1

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
    for b in range(0, num_cmp_blocks + 1):
        # Compare the two blocks. We know both files are at least min_size so all reads should work.
        buf1 = fp1.read(cmp_size)
        buf2 = fp2.read(cmp_size)
        # If they're different, gather information.
        if buf1 != buf2:
            total_bytes_diff += cmp_size
            # Count how many granularity level blocks changed.
            if bytes_gran == 0:
                gran_blocks += 1
                ts.gran_blocks += 1
            bytes_gran += cmp_size
            ts.bytes += cmp_size
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
                    ts.pct20 += 1
                elif bytes_gran >= pct80:
                    pct80_count += 1
                    ts.pct80 += 1
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
            bytes_gran += partial_cmp
            ts.bytes += partial_cmp
            part_bytes = offset + partial_cmp - start_off
            print(f'{filename}: offset {start_off}: {bytes_gran} bytes differ in {part_bytes} bytes')
    fp1.close()
    fp2.close()
    end = time.asctime()

    # Report for each file.
    if f1_size < f2_size:
        change = "grew"
        change_diff = f2_size - f1_size
    elif f1_size > f2_size:
        change = "shrank"
        change_diff = f1_size - f2_size
    else:
        change = "remained equal"
        change_diff = 0
    # Print the time even if no changes because we may want to know how long it took to not
    # see any changes. Only print if the time is not identical.
    if start != end:
        print(f'{filename}: time: started {start} completed {end}')
    if total_bytes_diff == 0:
        # If the file is unchanged return now.
        print(f'{filename}: is unchanged')
        return

    # Otherwise print out the change information.
    if change_diff != 0:
        print(f'{filename}: size: {f1_size} {f2_size} {change} by {change_diff} bytes')
    else:
        print(f'{filename}: size: {f1_size} {f2_size} {change}')
    print(f'{filename}: common: {min_size} differs by {total_bytes_diff} bytes in {gran_blocks} granularity blocks')
    if gran_blocks != 0:
        ts.files_changed += 1
        pct20_blocks = round(abs(pct20_count / gran_blocks * 100))
        pct80_blocks = round(abs(pct80_count / gran_blocks * 100))
        print(f'{filename}: smallest 20%: {pct20_count} of {gran_blocks} blocks ({pct20_blocks}%) differ by {pct20} bytes or less of {granularity}')
        print(f'{filename}: largest 80%: {pct80_count} of {gran_blocks} blocks ({pct80_blocks}%) differ by {pct80} bytes or more of {granularity}')
    print("")

#
# Print a detailed summary of all of the blocks and bytes over all of the files. We accumulated
# the changes as we went through each of the files.
#
def print_summary():
    print('SUMMARY')
    # Calculate overall totals from each of the different types first.
    total_blocks = 0
    total_bytes = 0
    total_files = 0
    total_files_changed = 0
    total_gran_blocks = 0
    for t in global_types:
        ts = typestats[t]
        total_bytes += ts.bytes
        total_files += ts.files
        total_files_changed += ts.files_changed
        total_gran_blocks += ts.gran_blocks
    print(f'Total: {total_bytes} bytes changed in {total_gran_blocks} granularity-sized ({granularity}) blocks')
    if total_files_changed == 1:
        word = 'file'
    else:
        word = 'files'
    print(f'Total: {total_files_changed} {word} changed out of {total_files} total files')

    # Walk through all the types printing out final information per type.
    for n, t in zip(global_names, global_types):
        ts = typestats[t]
        if ts.files_changed == 1:
            word = 'file'
        else:
            word = 'files'
        if ts.files == 1:
            word2 = 'file'
        else:
            word2 = 'files'
        print(f'{n}: {ts.files_changed} {word} changed out of {ts.files} {word2}')
        if ts.gran_blocks != 0:
            if ts.files_changed == 1:
                word = 'file'
            else:
                word = 'files'
            print(f'{ts.files_changed} changed {word}: differs by {ts.gran_blocks} granularity blocks in {ts.gran_blocks} total granularity blocks')
            print(f'{ts.files_changed} changed {word}: differs by {ts.bytes} bytes in {ts.gran_blocks} total granularity blocks')
            pct20_blocks = round(abs(ts.pct20 / ts.gran_blocks * 100))
            pct80_blocks = round(abs(ts.pct80 / ts.gran_blocks * 100))
            print(f'{n}: smallest 20%: {pct20_blocks} of {ts.gran_blocks} blocks ({pct20_blocks}%) differ by {pct20} bytes or less of {granularity}')
            print(f'{n}: largest 80%: {pct80_blocks} of {ts.gran_blocks} blocks ({pct80_blocks}%) differ by {pct80} bytes or more of {granularity}')

def compare_backups(dir1, dir2):
    files1=set(fnmatch.filter(os.listdir(dir1), "*.wt"))
    files2=set(fnmatch.filter(os.listdir(dir2), "*.wt"))
    older = older_dir(dir1, dir2)
    if older == dir1:
        diff1 = 'dropped'
        diff2 = 'created'
    else:
        diff1 = 'created'
        diff2 = 'dropped'

    common = files1.intersection(files2)
    # For now assume the first directory is the older one. Once we add functionality to parse the
    # text in WiredTiger.backup we can look at the checkpoint timestamp of a known table like the
    # history store and figure out which is older.
    # NOTE: Update the assumption in the usage statement also when this changes.
    printed_diff = False
    for file in files1.difference(files2):
        print(f'{file}: {diff1} between backups')
        printed_diff = True
    for file in files2.difference(files1):
        print(f'{file}: {diff2} between backups')
        printed_diff = True
    if printed_diff:
        print("")
    #print(common)
    for f in sorted(common):
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
