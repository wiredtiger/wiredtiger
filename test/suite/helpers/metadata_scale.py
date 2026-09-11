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

# Shared helpers for ASC/DSC metadata-file scaling tests.



import os
import re

SHARED_META = 'file:WiredTigerShared.wt_stable'

# Filled by the ASC/DSC tests in this process so the second one can print vs_asc.
SCALE_RESULTS = {}

SHARED_KEYS_PER_IDENT = 4
DSC_KEYS_PER_IDENT = 5
ASC_KEYS_PER_IDENT = 3

def ident_name(i):
    return f'collection-{i:06d}-00000000-0000-4000-8000-000000000000'

def dsc_keys(name):
    return (
        f'table:{name}',
        f'colgroup:{name}',
        f'layered:{name}',
        f'file:{name}.wt_ingest',
        f'file:{name}.wt_stable',
    )

def shared_keys(name):
    return (
        f'table:{name}',
        f'colgroup:{name}',
        f'layered:{name}',
        f'file:{name}.wt_stable',
    )

def asc_keys(name):
    return (
        f'table:{name}',
        f'colgroup:{name}',
        f'file:{name}.wt',
    )

def fmt_bytes(n):
    n = float(n)
    for unit, scale in (('GiB', 1 << 30), ('MiB', 1 << 20), ('KiB', 1 << 10)):
        if abs(n) >= scale:
            return f'{n / scale:.2f} {unit}'
    return f'{int(n)} B'

def fmt_ratio(num, den):
    if den <= 0:
        return 'n/a'
    return f'{num / den:.2f}x'

def payload_bytes(entries):
    return sum(len(k) + len(v) for k, v in entries.items())

def user_entries(entries):
    return {k: v for k, v in entries.items() if 'collection-' in k}

def scan_metadata(session, uri='metadata:'):
    entries = {}
    cursor = session.open_cursor(uri)
    while cursor.next() == 0:
        entries[cursor.get_key()] = cursor.get_value()
    cursor.close()
    return entries

def checkpoint_size(session, uri):
    cursor = session.open_cursor('metadata:')
    cursor.set_key(uri)
    if cursor.search() != 0:
        cursor.close()
        return 0
    value = cursor.get_value()
    cursor.close()
    sizes = re.findall(r'(?:size|sz)=(\d+)', value)
    return int(sizes[-1]) if sizes else 0

def measure_local(session, home='.'):
    local = scan_metadata(session)
    user = user_entries(local)
    return {
        'wt_file': os.path.getsize(os.path.join(home, 'WiredTiger.wt')),
        'payload': payload_bytes(local),
        'user_payload': payload_bytes(user),
        'user_keys': len(user),
        'local': local,
    }

def measure_dsc(session):
    snap = measure_local(session)
    shared = scan_metadata(session, SHARED_META)
    shared_user = user_entries(shared)
    snap['shared_ckpt'] = checkpoint_size(session, SHARED_META)
    snap['shared_payload'] = payload_bytes(shared)
    snap['shared_user_payload'] = payload_bytes(shared_user)
    snap['shared_keys'] = len(shared_user)
    return snap

def above_baseline(snap, baseline, key):
    return snap[key] - baseline[key]
