#!/usr/bin/env python3

""" Source scan script.

This script scans WiredTiger.

"""

import sys, os

# layercparse is a library written and maintained by the WiredTiger team.
import layercparse as lcp
from layercparse.scan_sources_tool import commandline, scan_sources_main

def load_wt_defs(rootPath):
    wt_defs_path = os.path.join(rootPath, 'dist', 'access_check', 'wt_defs.py')
    with open(wt_defs_path, "r") as f:
        code = f.read()
    wt_defs = {}
    exec(code, {}, wt_defs)
    return wt_defs

def main():
    args = commandline()
    wt_defs = load_wt_defs(args.home)

    lcp.Log.module_name_mismatch.enabled = False

    return scan_sources_main(extraFiles=wt_defs["extraFiles"],
                             modules=wt_defs["modules"],
                             extraMacros=wt_defs["extraMacros"])

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, BrokenPipeError):
        print("\nInterrupted")
        sys.exit(1)
    except OSError as e:
        print(f"\n{e.strerror}: {e.filename}")
        sys.exit(1)

