#!/usr/bin/env python3
import subprocess, re
import common_functions

# This is a temporary script to detect code changes to WiredTiger primitives.
primitives = [
    "__wt_atomic_.*",
    "F_CLR_ATOMIC",
    "F_SET_ATOMIC",
    "FLD_CLR_ATOMIC"
    "FLD_CLR_ATOMIC",
    "FLD_SET_ATOMIC",
    "FLD_SET_ATOMIC",
    "REF_SET_STATE",
    "volatile",
    "WT_BARRIER",
    "WT_DHANDLE_ACQUIRE",
    "WT_DHANDLE_RELEASE",
    "WT_FULL_BARRIER",
    "WT_INTL_INDEX_SET",
    "WT_ORDERED_READ",
    "WT_PAGE_ALLOC_AND_SWAP",
    "WT_PUBLISH",
    "WT_READ_BARRIER",
    "WT_READ_ONCE",
    "WT_REF_CAS_STATE",
    "WT_REF_LOCK",
    "WT_REF_UNLOCK",
    "WT_STAT_DECRV_ATOMIC",
    "WT_STAT_INCRV_ATOMIC",
    "WT_WRITE_BARRIER",
]

command = "git rev-parse --show-toplevel"
root = subprocess.run(command, capture_output=True, text=True, shell=True).stdout

command = f"git diff {common_functions.last_commit_from_dev()} -- src/"
diff = subprocess.run(command, capture_output=True, cwd=root.strip(), text=True, shell=True).stdout
found = False
found_primitives = []
start_regex = "^(\+|-).*"
for primitive in primitives:
    if (re.search(start_regex + primitive, diff, re.MULTILINE)):
        found_primitives.append(primitive)
        found = True

if (found):
    print("Code changes made since this branch diverged from develop include the following"
        " concurrency control primitives: " + str(found_primitives))
    print("If you have introduced or removed a primitive it will impact the in progress shared"
          " variable review project. Please reach out accordingly.")
