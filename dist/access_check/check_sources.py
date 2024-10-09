#!/usr/bin/env python3

import sys, os
from layercparse import *
from pprint import pprint, pformat

def main():
    # setLogLevel(LogLevel.WARNING)

    rootPath = os.path.realpath(sys.argv[1])
    setRootPath(rootPath)
    setModules([
        Module("block"),
        Module("block_cache", sourceAliases = ["blkcache", "bm"]),
        Module("bloom"),
        Module("btree"),
        Module("call_log"),
        # Module("checksum"),
        Module("conf"),
        Module("config"),
        Module("conn"),
        Module("cursor", sourceAliases=["cur", "btcur", "curbackup"]),
        Module("evict"),
        Module("history", sourceAliases = ["hs"]),
        Module("log"),
        Module("lsm", sourceAliases=["clsm"]),
        Module("meta", sourceAliases=["metadata"]),
        Module("optrack"),
        # Module("os", fileAliases = ["os_common", "os_darwin", "os_linux", "os_posix", "os_win"]),
        Module("packing", sourceAliases=["pack"]),
        Module("reconcile", sourceAliases = ["rec"]),
        Module("rollback_to_stable", sourceAliases = ["rts"]),
        Module("schema"),
        Module("session"),
        # Module("support"),
        Module("tiered"),
        Module("txn"),
        # Module("utilities"),
    ])
    files = get_files()
    files.insert(0, os.path.join(os.path.realpath(rootPath), "src/include/wiredtiger.in"))

    _globals = Codebase()
    _globals.scanFiles(files, twopass=True, multithread=True)
    # print(" ===== Globals:")
    # pprint(_globals, width=120, compact=False)
    # print(" =====")

    # print(" ===== Access check:")
    AccessCheck(_globals).checkAccess(multithread=True)

if __name__ == "__main__":
    main()

