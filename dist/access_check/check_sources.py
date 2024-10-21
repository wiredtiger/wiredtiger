#!/usr/bin/env python3

""" Access checker script.

This script checks that WiredTiger sources comply with modularity rules
described in MODULARITY.md.

"""

import sys, os

# layercparse is a library written and maintained by the WiredTiger team.
import layercparse as lcp
from layercparse import Module

def main():
    # setLogLevel(LogLevel.WARNING)

    rootPath = os.path.realpath(sys.argv[1])
    lcp.setRootPath(rootPath)
    if lcp.workspace.errors:
        return False
    files = lcp.get_files()  # list of all source files
    files.insert(0, os.path.join(os.path.realpath(rootPath), "src/include/wiredtiger.in"))

    _globals = lcp.Codebase()
    _globals.scanFiles(files)
    lcp.AccessCheck(_globals).checkAccess()

    return not lcp.workspace.errors

if __name__ == "__main__":
    sys.exit(main())

