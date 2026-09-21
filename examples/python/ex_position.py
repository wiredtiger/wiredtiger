#!/usr/bin/env python
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
#
# ex_position.py
#   Demonstrates WT_CURSOR.set_position and WT_CURSOR.get_position through the
#   Python binding: sampling at evenly spaced positions, progress reporting,
#   splitting a table into key ranges with key-only boundaries, cache-only
#   positioning, resuming a best-effort walk, and the anchor and direction
#   flags.
#
# Run it against a WiredTiger build configured with -DENABLE_PYTHON=1:
#
#   PYTHONPATH=<build>/lang/python python3 ex_position.py [home_directory]
#
# The home directory (default WT_HOME in the current directory) must not exist
# or must be an empty directory; the program creates it as needed and removes
# it at the end. Two "Invalid argument" messages on stderr
# are expected: WiredTiger logs the errors the program provokes on purpose in
# sections 2 and 3.
#
# Positions are soft: a position is a fraction in [0, 1] derived from the
# shape of the in-memory tree. It preserves key order but is not uniform, and
# it drifts as pages split, merge and are evicted. Positions suit sampling,
# progress reporting and best-effort resumption, never durable pointers.

import os, shutil, sys
import wiredtiger
from wiredtiger import wiredtiger_open, stat

NRECORDS = 50000
URI = 'table:position_demo'

# Small pages give the tree several levels with tens of thousands of records.
# String keys make key-only boundaries readable: they are prefixes of record
# keys and unpack as shorter strings.
TABLE_CONFIG = 'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=4KB'

# Statistics show that cache-only positioning performs no disk reads.
CONN_CONFIG = 'create,cache_size=100MB,statistics=(fast)'
SMALL_CACHE_CONFIG = 'cache_size=1MB,statistics=(fast)'

CACHE_ONLY = wiredtiger.WT_POSITION_CACHE_ONLY
KEY_ONLY = wiredtiger.WT_POSITION_KEY_ONLY
PREV = wiredtiger.WT_POSITION_PREV
EXACT = wiredtiger.WT_POSITION_ANCHOR_EXACT
FIRST = wiredtiger.WT_POSITION_ANCHOR_FIRST
MIDDLE = wiredtiger.WT_POSITION_ANCHOR_MIDDLE
LAST = wiredtiger.WT_POSITION_ANCHOR_LAST

def key_at(i):
    return 'key%08d' % i

# Record index encoded in a full key; used only to compare positions against
# the true fraction of the table.
def index_of(key):
    return int(key[3:])

def position(cursor, pos, flags=0):
    '''Position the cursor; returns (ret, position) with ret 0 or WT_NOTFOUND.'''
    p = wiredtiger.Position()
    p.pos = pos
    p.flags = flags
    return cursor.set_position(p), p

def conn_stat(session, key):
    c = session.open_cursor('statistics:')
    value = c[key][2]
    c.close()
    return value

def load(session):
    print('Loading %d records into %s' % (NRECORDS, URI))
    session.create(URI, TABLE_CONFIG)
    c = session.open_cursor(URI)
    for i in range(NRECORDS):
        c[key_at(i)] = 'v' * 100
    c.close()
    # Write the pages out so that a reopened connection sees the on-disk tree
    # shape. Records still on a page's insert list are visited by positioning
    # but never selected as a slot.
    session.checkpoint()

def sample_evenly(session, n):
    '''1. Sampling N records at evenly spaced positions.

    This replaces a random cursor when rebuilding truncate markers or gathering
    statistics: each call reads one leaf page in the common case and the
    samples arrive in key order. Positions follow the tree shape rather than
    the record count, so the sampled fraction is only roughly the requested one.
    '''
    print('\n1. Sampling %d records at evenly spaced positions' % (n + 1))
    print('   Each set_position reads one leaf page in the common case. pages_skipped is')
    print('   zero when the page the position addressed was used; a non-zero value means')
    print('   the addressed page was unusable and the result is approximately that many')
    print('   pages off (a step over an unreadable subtree counts once). Positions follow')
    print('   the tree shape the checkpoint produced, so the exact keys differ between runs.')
    c = session.open_cursor(URI)
    previous = ''
    for i in range(n + 1):
        ret, p = position(c, i / n)
        if ret == wiredtiger.WT_NOTFOUND:
            print('   pos %.2f -> no visible record at or after this position' % p.pos)
            continue
        key = c.get_key()
        print('   pos %.2f -> %s  (record fraction %.3f, pages_skipped %d)%s' % (p.pos, key,
            index_of(key) / NRECORDS, p.pages_skipped, '  OUT OF ORDER' if key < previous else ''))
        previous = key
    c.close()

def report_progress(session):
    '''2. Progress reporting during a full scan.

    get_position on a positioned cursor tells how far into the object it is
    without counting the records first. The value is monotonic along the scan
    but not proportional to the record count.
    '''
    print('\n2. Progress reporting during a full scan with get_position')
    c = session.open_cursor(URI)
    count = 0
    last = -1.0
    while c.next() == 0:
        count += 1
        if count % 10000 == 0 or count == NRECORDS:
            pos = c.get_position()
            print('   %6d records scanned (%5.1f%% of the count): position %.4f%s' % (count,
                100.0 * count / NRECORDS, pos, '  NOT MONOTONIC' if pos < last else ''))
            last = pos
    # A cursor that is not positioned has no position.
    try:
        c.get_position()
    except wiredtiger.WiredTigerError as e:
        print('   get_position after the scan ends (cursor reset): %s' % e)
    c.close()

def split_into_ranges(session, k):
    '''3. Splitting the table into K key ranges for parallel work.

    Key-only positioning returns the boundary key of the page a position
    addresses without reading any leaf page: only internal pages are visited,
    and those are almost always cached. Boundaries are page-aligned separators,
    so K-1 of them split the object into K ranges that workers can scan with
    bounded cursors, and a range ending at one can be truncated without
    reading the pages inside.
    '''
    print('\n3. Splitting the table into %d ranges with key-only boundaries' % k)
    print('   Boundary keys are separators, usually shorter than any record key. The')
    print('   leftmost boundary is the empty string, a sentinel for the start of the object')
    print('   that cannot be used as a search key or a bound, so range 0 has no lower bound.')

    c = session.open_cursor(URI)
    boundary = [''] * (k + 1)
    for i in range(1, k):
        # Without CACHE_ONLY, key-only mode never returns WT_NOTFOUND: an object
        # without internal pages yields the empty key for every position.
        ret, p = position(c, i / k, KEY_ONLY)
        boundary[i] = c.get_key()   # The cursor holds no page and no position.
        print('   boundary %d at pos %.2f: "%s"  (pages_skipped %d, always zero here)' % (i, p.pos,
            boundary[i], p.pages_skipped))
        if boundary[i] == '':
            print('   The tree has no internal pages, so every boundary is the sentinel.')

    # The cursor is not positioned after key-only mode, but its key is set,
    # exactly as after set_key: search_near reaches the first visible record
    # of that page.
    if boundary[1] != '':
        c.set_key(boundary[1])
        exact = c.search_near()
        print('   search_near from boundary 1 lands on %s (exact %d): the boundary is a' % (
            c.get_key(), exact))
        print('   prefix of the first key on its page, so the match is at or after it.')
    c.close()

    print('   Worker k scans from boundary k inclusive to boundary k+1 exclusive:')
    total = 0
    for i in range(k):
        worker = session.open_cursor(URI)
        if boundary[i] != '':
            worker.set_key(boundary[i])
            worker.bound('bound=lower')
        if i + 1 < k and boundary[i + 1] != '':
            worker.set_key(boundary[i + 1])
            worker.bound('bound=upper,inclusive=false')
        keys = [key for key, value in worker]
        total += len(keys)
        print('   range %d: %5d records, %s .. %s' % (i, len(keys), keys[0], keys[-1]))

        # A bounded cursor cannot be positioned by fraction; workers position by
        # key instead.
        if i == 0:
            try:
                position(worker, 0.5)
            except wiredtiger.WiredTigerError as e:
                print('   set_position on a bounded cursor: %s' % e)
        worker.close()
    print('   The ranges cover every record exactly once: %d of %d' % (total, NRECORDS))

def sample_cache_only(session, n):
    '''4. Cache-only sampling after a reopen with a small cache.

    WT_POSITION_CACHE_ONLY confines the call to pages already in memory: it
    never reads from disk and never waits for a locked page. If nothing usable
    is in memory the call returns WT_NOTFOUND; if a nearby page is in memory
    it is used and pages_skipped reports approximately how many pages were
    stepped over. This suits diagnostics and load-shedding heuristics that must
    not cause I/O.
    '''
    print('\n4. Cache-only sampling on a freshly opened connection with a 1MB cache')
    print('   Only the root page is in memory after the reopen, so most positions find')
    print('   nothing. No position ever causes a page read.')
    c = session.open_cursor(URI)

    def sample(label):
        reads_before = conn_stat(session, stat.conn.cache_read)
        hits = not_found = skipped = 0
        for i in range(n + 1):
            ret, p = position(c, i / n, CACHE_ONLY)
            if ret == wiredtiger.WT_NOTFOUND:
                not_found += 1
                print('   pos %.2f -> WT_NOTFOUND (pages_skipped %d)' % (p.pos, p.pages_skipped))
                continue
            hits += 1
            if p.pages_skipped != 0:
                skipped += 1
            print('   pos %.2f -> %s (pages_skipped %d)' % (p.pos, c.get_key(), p.pages_skipped))
        reads = conn_stat(session, stat.conn.cache_read) - reads_before
        print('   %s: %d hits (%d of them on a nearby page), %d not found, %d pages read from disk'
            % (label, hits, skipped, not_found, reads))

    sample('Fresh connection')

    # A page brought into memory by an ordinary search is found again at its
    # record's position. A second cursor stays on the page so that it cannot be
    # evicted in between.
    middle = key_at(NRECORDS // 2)
    pin = session.open_cursor(URI)
    pin.set_key(middle)
    pin.search()
    pos = pin.get_position()
    print('\n   After reading %s by key, its page is in memory at position %.4f:' % (middle, pos))
    reads_before = conn_stat(session, stat.conn.cache_read)
    ret, p = position(c, pos, CACHE_ONLY)
    print('   cache-only pos %.4f -> %s (pages_skipped %d)' % (pos,
        c.get_key() if ret == 0 else 'WT_NOTFOUND', p.pages_skipped))
    # A position a little before it addresses a page that is not in memory;
    # the walk forwards steps over pages until it reaches the resident one and
    # reports how many it skipped.
    ret, p = position(c, pos - 0.01, CACHE_ONLY)
    print('   cache-only pos %.4f -> %s (pages_skipped %d)' % (pos - 0.01,
        c.get_key() if ret == 0 else 'WT_NOTFOUND', p.pages_skipped))
    print('   pages read from disk by those two calls: %d' % (
        conn_stat(session, stat.conn.cache_read) - reads_before))
    pin.close()

    # A full scan through a cache much smaller than the table leaves only the
    # most recently read pages resident. Positions before them walk forwards to
    # the first resident page and report how many pages they stepped over.
    print('\n   After a full scan through the 1MB cache, only the tail of the table is resident:')
    c.reset()
    while c.next() == 0:
        pass
    sample('After the scan')
    c.close()

def pause_walk(session, stop_after):
    '''5. A resumable best-effort walk.

    A background pass saves get_position when it pauses and later continues
    with set_position followed by next. Because positions are soft, a few
    records near the pause point may be seen twice or skipped if the tree
    changed in between: this suits statistics gathering and compaction-style
    passes, not anything that must visit each record exactly once.
    '''
    print('\n5. A resumable best-effort walk')
    c = session.open_cursor(URI)
    for i in range(stop_after):
        c.next()
    paused_key = c.get_key()
    pos = c.get_position()
    print('   Scanned %d records, pausing on %s at position %.6f.' % (stop_after, paused_key, pos))
    print('   Closing the connection and reopening it...')
    c.close()
    return pos, paused_key

def resume_walk(session, pos, paused_key, scanned):
    c = session.open_cursor(URI)
    ret, p = position(c, pos)
    if ret == wiredtiger.WT_NOTFOUND:
        print('   Nothing at or after the saved position: the walk is complete.')
        c.close()
        return
    key = c.get_key()
    distance = index_of(key) - index_of(paused_key)
    if distance == 0:
        print('   set_position(%.6f) lands on %s, the record the walk paused on.' % (pos, key))
    else:
        print('   set_position(%.6f) lands on %s, %d records from the pause point:' % (pos, key,
            distance))
        print('   the tree changed shape in between, which positions do not survive exactly.')
    # The record the cursor landed on was already visited, so continue from
    # the next one.
    count = 0
    while c.next() == 0:
        count += 1
    print('   Resumed and visited %d more records; %d visited in total across the pause,' % (
        count, scanned + count))
    print('   against %d records in the table.' % NRECORDS)
    c.close()

def anchors_and_direction(session, pos):
    '''6. Anchors and direction on one position.

    A position addresses a page and, by default, a slot within it proportional
    to the remainder. The anchor flags pick the first, middle or last on-disk
    slot instead, and WT_POSITION_PREV walks backwards to a visible record
    rather than forwards. Anchor and direction are independent: ANCHOR_LAST
    alone moves forwards from the last slot, and reaches the next page if that
    slot is not visible, while ANCHOR_LAST with PREV is "the last visible
    on-disk slot of the addressed page".
    '''
    print('\n6. Anchors and direction at position %.2f' % pos)
    c = session.open_cursor(URI)
    variants = [
        ('ANCHOR_EXACT (default)', EXACT),
        ('ANCHOR_EXACT | PREV', EXACT | PREV),
        ('ANCHOR_FIRST', FIRST),
        ('ANCHOR_MIDDLE', MIDDLE),
        ('ANCHOR_LAST', LAST),
        ('ANCHOR_LAST | PREV', LAST | PREV),
    ]

    def show():
        for name, flags in variants:
            ret, p = position(c, pos, flags)
            print('   %-24s -> %s' % (name, c.get_key() if ret == 0 else 'WT_NOTFOUND'))

    print('   All records visible: every variant stays on the addressed page.')
    show()

    # Count the on-disk records of the addressed page: from the first slot to
    # the last.
    position(c, pos, LAST | PREV)
    last = c.get_key()
    position(c, pos, FIRST)
    on_page = 1
    while c.get_key() != last:
        c.next()
        on_page += 1
    print('   The addressed page holds %d records.' % on_page)

    # Removing the last record on the page leaves its slot in place but
    # invisible. The walk direction now decides which neighbor is returned.
    print('\n   After removing %s, the last record on the page:' % last)
    c.set_key(last)
    c.remove()
    print('   ANCHOR_LAST walks forwards off the page to the first record of the next one,')
    print('   while ANCHOR_LAST | PREV walks backwards to the last visible on-disk slot of this page.')
    show()
    c.close()

def main(home):
    if os.path.exists(home) and not (os.path.isdir(home) and not os.listdir(home)):
        sys.exit('%s exists and is not an empty directory' % home)
    os.makedirs(home, exist_ok=True)
    print('Database home: %s' % home)

    conn = wiredtiger_open(home, CONN_CONFIG)
    load(conn.open_session())
    # Reopen so the in-memory tree has the on-disk shape rather than one big
    # insert list.
    conn.close()
    conn = wiredtiger_open(home, CONN_CONFIG)
    session = conn.open_session()

    sample_evenly(session, 10)
    report_progress(session)
    split_into_ranges(session, 4)

    conn.close()
    conn = wiredtiger_open(home, SMALL_CACHE_CONFIG)
    session = conn.open_session()
    sample_cache_only(session, 10)

    scanned = 12345
    pos, paused_key = pause_walk(session, scanned)
    conn.close()
    conn = wiredtiger_open(home, CONN_CONFIG)
    session = conn.open_session()
    resume_walk(session, pos, paused_key, scanned)

    anchors_and_direction(session, 0.5)

    conn.close()
    shutil.rmtree(home)

if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'WT_HOME')
