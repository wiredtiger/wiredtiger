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

typedef struct __wt_myslot {
    WT_LOGSLOT *slot;    /* Slot I'm using */
    wt_off_t end_offset; /* My end offset in buffer */
    wt_off_t offset;     /* Slot buffer offset */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_MYSLOT_CLOSE 0x1u         /* This thread is closing the slot */
#define WT_MYSLOT_NEEDS_RELEASE 0x2u /* This thread is releasing the slot */
#define WT_MYSLOT_UNBUFFERED 0x4u    /* Write directly */
                                     /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
    uint32_t flags;
} WT_MYSLOT;

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

extern int __wti_log_fill(WT_SESSION_IMPL *session, WT_MYSLOT *myslot, bool force, WT_ITEM *record,
  WT_LSN *lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_slot_switch(WT_SESSION_IMPL *session, WT_MYSLOT *myslot, bool retry,
  bool forced, bool *did_work) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int64_t __wti_log_slot_release(WT_MYSLOT *myslot, int64_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_log_slot_join(
  WT_SESSION_IMPL *session, uint64_t mysize, uint32_t flags, WT_MYSLOT *myslot);
