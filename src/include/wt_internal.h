/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * 	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WT_INTERNAL_H
#define __WT_INTERNAL_H

#if defined(__cplusplus)
extern "C" {
#endif

#include "wt_prelude.h"

// TODO - I've manually included these paths. Find an automated way in future
// I'm also not 100% happy with including files from the directory above. Think on this
#include "../block/block.h"
#include "../block_cache/block_cache.h"
#include "../bloom/bloom.h"
#include "../btree/btree.h"
#include "../call_log/call_log.h"
#include "../conf/conf.h"
#include "../config/config.h"
#include "../conn/conn.h"
#include "../cursor/cursor.h"
#include "../evict/evict.h"
#include "../history/history.h"
#include "../log/log.h"
#include "../lsm/lsm.h"
#include "../meta/meta.h"
#include "../optrack/optrack.h"
#include "../os_common/os_common.h"
#include "../packing/packing.h"
#include "../reconcile/reconcile.h"
#include "../schema/schema.h"
#include "../session/session.h"
#include "../rollback_to_stable/rollback_to_stable.h"
#include "../support/support.h"
#include "../tiered/tiered.h"
#include "../txn/txn.h"

#ifdef _WIN32
#include "extern_win.h"
#else
#include "extern_posix.h"
#ifdef __linux__
#include "extern_linux.h"
#elif __APPLE__
#include "extern_darwin.h"
#endif
#endif

#include "cache_inline.h"   /* required by misc_inline.h */
#include "ctype_inline.h"   /* required by packing_inline.h */
#include "intpack_inline.h" /* required by cell_inline.h, packing_inline.h */
#include "misc_inline.h"    /* required by mutex_inline.h */

#include "buf_inline.h"       /* required by cell_inline.h */
#include "ref_inline.h"       /* required by btree_inline.h */
#include "timestamp_inline.h" /* required by btree_inline.h */
#include "cell_inline.h"      /* required by btree_inline.h */
#include "mutex_inline.h"     /* required by btree_inline.h */
#include "txn_inline.h"       /* required by btree_inline.h */

#include "bitstring_inline.h"
#include "block_inline.h"
#include "btree_inline.h" /* required by cursor_inline.h */
#include "btree_cmp_inline.h"
#include "column_inline.h"
#include "conf_inline.h"
#include "cursor_inline.h"
#include "log_inline.h"
#include "os_fhandle_inline.h"
#include "os_fs_inline.h"
#include "os_fstream_inline.h"
#include "packing_inline.h"
#include "reconcile_inline.h"
#include "serial_inline.h"
#include "str_inline.h"
#include "time_inline.h"

#if defined(__cplusplus)
}
#endif
#endif /* !__WT_INTERNAL_H */
