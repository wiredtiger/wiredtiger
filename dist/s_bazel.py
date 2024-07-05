#!/usr/bin/env python3

import pprint

platform2files = {}
with open("filelist") as f:
    for a in [l.split() for l in f if l != "\n" and not l.startswith("#")]:
        file, platform = a[0], f"WT_FILELIST_{a[1]}" if len(a) > 1 else "WT_FILELIST"
        platform2files.setdefault(platform, [])
        platform2files[platform].append(file)

res = """
# DO NOT EDIT: automatically built by dist/s_bazel.

# This file is only used by external projects building WiredTiger via Bazel.
"""
for platform, files in platform2files.items():
    res += f"\n{platform} = " + pprint.pformat(files) + "\n"

try:
    with open("filelist.bzl") as f:
        if res == f.read():
            exit()
except OSError:
    pass

print("Updated dist/filelist.bzl")

with open("filelist.bzl", "w") as f:
    f.write(res)

