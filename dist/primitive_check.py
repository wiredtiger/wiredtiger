#!/usr/bin/env python3
import subprocess, re

# This is a temporary script to detect code changes to WiredTiger primitives.
# FIXME-WT-10861 That ticket will introduce a script to replace this one, delete this script when
# complete.
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

command = "git rev-parse --show-toplevel"
root = subprocess.run(command, capture_output=True, text=True, shell=True).stdout

command = "git diff $(git merge-base --fork-point develop) -- src/"
diff = subprocess.run(command, capture_output=True, shell=True, text=True, cwd=root.strip()).stdout
found = False
found_primitives = []
start_regex = "^(\+|-).*"
for primitive in primitives:
    if (re.search(start_regex + primitive, diff)):
        found_primitives.append(primitive)
        found = True

if (found):
    print("Code changes made since this branch diverged from develop include the following"
        " concurrency control primitives: " + str(found_primitives))
    print("If you have introduced or removed a primite it will impact the in progress shared"
          " variable review project. Please reach out accordingly.")
