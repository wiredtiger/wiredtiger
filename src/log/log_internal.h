#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Order is significant. */
#include "wt_internal.h"
#include "error.h"
#include "misc.h"
#include "mutex.h"
#include "dhandle.h"
#include "session.h"

#include "log.h"


/*
 * WT_LOG_DESC --
 *	The log file's description.
 */
typedef struct __wt_log_desc {
#define WT_LOG_MAGIC 0x101064u
    uint32_t log_magic; /* 00-03: Magic number */
                        /*
                         * NOTE: We bumped the log version from 2 to 3 to make it convenient for
                         * MongoDB to detect users accidentally running old binaries on a newer
                         * release. There are no actual log file format changes in versions 2
                         * through 5.
                         */
    uint16_t version;  /* 04-05: Log version */
    uint16_t unused;   /* 06-07: Unused */
    uint64_t log_size; /* 08-15: Log file size */
} WT_LOG_DESC;

/*
 * Simple structure for sorting written slots.
 */
typedef struct {
    WT_LSN lsn;
    uint32_t slot_index;
} WT_LOG_WRLSN_ENTRY;



/*
 * __wt_log_desc_byteswap --
 *     Handle big- and little-endian transformation of the log file description block.
 */
static WT_INLINE void
__wt_log_desc_byteswap(WT_LOG_DESC *desc)
{
#ifdef WORDS_BIGENDIAN
    desc->log_magic = __wt_bswap32(desc->log_magic);
    desc->version = __wt_bswap16(desc->version);
    desc->unused = __wt_bswap16(desc->unused);
    desc->log_size = __wt_bswap64(desc->log_size);
#else
    WT_UNUSED(desc);
#endif
}

