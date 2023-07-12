#!/usr/bin/env python3
import subprocess, re

# This script reviews all changes made to the code base since it diverged from the develop branch.
# If the diff involves:
# - the use of WiredTiger concurrency primitives
# - variables that are accessed by multiple threads outside the protection of a mutex
#   (indicated by the variable having a `_shared` suffix)
# This script will warn the user so they can perform a careful review of the changes.
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
    "WT_REF_CAS_STATE",
    "WT_REF_LOCK",
    "WT_REF_UNLOCK",
    "WT_STAT_DECRV_ATOMIC",
    "WT_STAT_INCRV_ATOMIC",
    "WT_WRITE_BARRIER",
]

# These strings are false positives when matching against _shared. Ignore them in search results.
exclude = [
    "__wt_track_shared",
    "checkpoint_txn_shared",
    "is_tiered_shared",
    "now_shared",
    "remove_shared",
    "tiered_shared",
    "txn_shared",
    "was_shared",
]

# Regex for a modified line according to the diff.
start_regex = "^(\+|-).*"

# Get the changeset.
command = "git rev-parse --show-toplevel"
root = subprocess.run(command, capture_output=True, text=True, shell=True).stdout
command = "git diff $(git merge-base --fork-point develop) -- src/"
diff = subprocess.run(command, capture_output=True, cwd=root.strip(), text=True, shell=True).stdout

# Iterate over the primitive list and search for them.
found_primitives = []
for primitive in primitives:
    if re.search(start_regex + primitive, diff, re.MULTILINE):
        found_primitives.append(primitive)

# Iterate over the diff line by line and search for shared variables.
shared_vars = set()
for line in diff.split('\n'):
    if line.startswith("+") or line.startswith("-"):
        for m in re.finditer("([a-z_]+_shared)[^a-z_]", line):
            if m[1] not in exclude:
                shared_vars.add(m[1])

# If we found any primitives or shared variables log a warning.
if len(found_primitives) != 0:
    print("### Primitive check ###")
    print("Code changes made since this branch diverged from develop include the following"
        " concurrency control primitives: " + str(found_primitives))
    print("If you have made modifications to code involving a primitive it could have unexpected"
        " behaviour, please do so with care.")
        
if len(shared_vars) != 0:
    print("### Shared variable check ###")
    print("Code changes made since this branch diverged from develop include the following"
        " shared variables: " + str(shared_vars))
    print("If you have made modifications to code involving a shared variable it could have"
          " unexpected behaviour, please do so with care.")
    print("If you believe this is incorrect please add the shared variable to the exclude list.")
