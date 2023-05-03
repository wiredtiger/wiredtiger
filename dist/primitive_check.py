#!/usr/bin/env python3
# This is a temporary script to detect code changes to WiredTiger primitives.

import subprocess, re
primitives = [
    "WT_BARRIER",
    "WT_WRITE_BARRIER",
    "WT_PUBLISH",
    "WT_READ_BARRIER",
    "WT_ORDERED_READ",
    "WT_FULL_BARRIER",
    "__wt_atomic_.*",
    "FLD_SET_ATOMIC",
    "FLD_CLR_ATOMIC",
    "F_SET_ATOMIC",
    "F_CLR_ATOMIC",
    "WT_PAGE_ALLOC_AND_SWAP",
    "WT_REF_UNLOCK",
    "WT_REF_LOCK",
    "WT_REF_CAS_STATE",
    "REF_SET_STATE",
    "WT_INTL_INDEX_SET",
    "volatile",
    "WT_STAT_INCRV_ATOMIC",
    "WT_STAT_DECRV_ATOMIC",
    "WT_DHANDLE_ACQUIRE",
    "WT_DHANDLE_RELEASE",
    "FLD_SET_ATOMIC",
    "FLD_CLR_ATOMIC"
]

diff = subprocess.run(['git', 'diff'], capture_output=True, text=True).stdout
found = False
found_primitives = []
start_regex = "(\+|-).*"
for primitive in primitives:
    if (re.search(start_regex + primitive, diff)):
        found_primitives.append(primitive)
        found = True

if (found):
    print("Git diff indicates that the following WiredTiger primitives are being added or removed: "
        + str(found_primitives))
    print("If this is correct, please reach out via slack to Luke" +
          " or Andrew and inform them of the change")
