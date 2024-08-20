/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <cstdio>
#include <string>
#include <stdexcept>

#include "utils.h"

namespace utils {

/*
 * throwIfNonZero --
 *     Test result. Throw if it is non-zero.
 */
void
throwIfNonZero(int result)
{
    if (result != 0) {
        std::string errorMessage("Error result is " + std::to_string(result));
        throw std::runtime_error(errorMessage);
    }
}

/*
 * remove_wrapper --
 *     Delete file with path path.
 */
static int
remove_wrapper(std::string const &path)
{
    return std::remove(path.c_str());
}

/*
 * wiredtigerCleanup --
 *     Delete WiredTiger files in directory home and directory home.
 */
void
wiredtigerCleanup(std::string const &home)
{
    // Ignoring errors here; we don't mind if something doesn't exist.
    remove_wrapper(home + "/WiredTiger");
    remove_wrapper(home + "/WiredTiger.basecfg");
    remove_wrapper(home + "/WiredTiger.lock");
    remove_wrapper(home + "/WiredTiger.turtle");
    remove_wrapper(home + "/WiredTiger.wt");
    remove_wrapper(home + "/WiredTigerHS.wt");
    remove_wrapper(home + "/backup_test1.wt");
    remove_wrapper(home + "/backup_test2.wt");
    remove_wrapper(home + "/cursor_test.wt");

    remove_wrapper(home);
}

/*
 * ext_print_list --
 *     Print a skip list of WT_EXT *.
 */
void
ext_print_list(WT_EXT **head)
{
    WT_EXT *extp;
    int i;

    if (head == nullptr)
        return;

    for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        printf("L%d: ", i);

        extp = head[i];
        while (extp != nullptr) {
            printf("%p {off %" PRId64 ", size %" PRId64 ", end %" PRId64 "} -> ", extp, extp->off,
              extp->size, (extp->off + extp->size - 1));
            extp = extp->next[i];
        }

        printf("X\n");
    }
}

/*
 * extlist_print_off --
 *     Print an WT_EXTLIST and it's off skip list.
 */
void
extlist_print_off(WT_EXTLIST &extlist)
{
    printf("{name %s, bytes %" PRIu64 ", entries %" PRIu32 ", objectid %" PRIu32 ", offset %" PRId64
           ", checksum 0x%" PRIu32 ", size %" PRIu32 ", track_size %s, last %p",
      extlist.name, extlist.bytes, extlist.entries, extlist.objectid, extlist.offset,
      extlist.checksum, extlist.size, extlist.track_size ? "true" : "false", extlist.last);
    if (extlist.last != nullptr)
        printf(" {off %" PRId64 ", size %" PRId64 ", depth %" PRIu8 ", next %p}", extlist.last->off,
          extlist.last->size, extlist.last->depth, extlist.last->next);
    printf("\noff:\n");
    ext_print_list(extlist.off);
}

} // namespace utils
