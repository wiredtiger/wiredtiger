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
        Module("block_cache", fileAliases=["block_chunkcache"], sourceAliases = ["blkcache", "bm"]),
        Module("bloom"),
        Module("btree", fileAliases=["btmem", "btree_cmp", "dhandle", "modify", "ref", "serial"]),
        Module("call_log"),
        # Module("checksum"),
        Module("conf", sourceAliases=["conf_keys"]),
        Module("config"),
        Module("conn", fileAliases=["connection"], sourceAliases=["connection"]),
        Module("cursor", sourceAliases=["cur", "btcur", "curbackup"]),
        Module("evict", fileAliases=["cache"]),
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
        Module("txn", sourceAliases=["truncate"]),
        # Module("utilities"),

        Module("bitstring"),
        Module("cell"),
        Module("checkpoint", sourceAliases=["ckpt"]),
        Module("column", sourceAliases=["col"]),
        Module("compact"),
        Module("generation"),
        Module("pack", fileAliases=["intpack"]),
        Module("stat"),
    ])
    files = get_files()
    files.insert(0, os.path.join(os.path.realpath(rootPath), "src/include/wiredtiger.in"))

    _globals = Codebase()
    # print(" ===== Scan")
    _globals.scanFiles(files, twopass=True, multithread=True)
    # _globals.scanFiles(files, twopass=True, multithread=False)
    # print(" ===== Globals:")
    # pprint(_globals, width=120, compact=False)
    # print(" =====")

    # print(" ===== Access check:")
    # print(" ===== Check")
    AccessCheck(_globals).checkAccess(multithread=True)
    # AccessCheck(_globals).checkAccess(multithread=False)

    return not workspace.errors

if __name__ == "__main__":
    sys.exit(main())

