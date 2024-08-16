#pragma once

#include <stdint.h>

/*
 * WT_LOG_DESC --
 *	The log file's description.
 */
struct __wt_log_desc {
#define WT_LOG_MAGIC 0x101064u
    uint32_t log_magic; /* 00-03: Magic number */
                        /*
                         * NOTE: We bumped the log version from 2 to 3 to make it convenient for
                         * MongoDB to detect users accidentally running old binaries on a newer
                         * release. There are no actual log file format changes in versions 2
                         * through 5.
                         */
#define WT_LOG_VERSION 5
    uint16_t version;  /* 04-05: Log version */
    uint16_t unused;   /* 06-07: Unused */
    uint64_t log_size; /* 08-15: Log file size */
};
