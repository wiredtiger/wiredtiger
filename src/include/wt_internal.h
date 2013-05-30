/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#if defined(__cplusplus)
extern "C" {
#endif

/*******************************************
 * WiredTiger public include file, and configuration control.
 *******************************************/
#include "wiredtiger_config.h"
#include "wiredtiger_ext.h"

/*******************************************
 * WiredTiger system include files.
 *******************************************/
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/uio.h>

#include <ctype.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#endif
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*******************************************
 * WiredTiger externally maintained include files.
 *******************************************/
#include "queue.h"

/*
 * DO NOT EDIT: automatically built by dist/s_typedef.
 * Forward type declarations for internal types: BEGIN
 */
enum __wt_page_state;
    typedef enum __wt_page_state WT_PAGE_STATE;
enum __wt_txn_isolation;
    typedef enum __wt_txn_isolation WT_TXN_ISOLATION;
struct __wt_addr;
    typedef struct __wt_addr WT_ADDR;
struct __wt_block;
    typedef struct __wt_block WT_BLOCK;
struct __wt_block_ckpt;
    typedef struct __wt_block_ckpt WT_BLOCK_CKPT;
struct __wt_block_desc;
    typedef struct __wt_block_desc WT_BLOCK_DESC;
struct __wt_block_header;
    typedef struct __wt_block_header WT_BLOCK_HEADER;
struct __wt_bloom;
    typedef struct __wt_bloom WT_BLOOM;
struct __wt_bloom_hash;
    typedef struct __wt_bloom_hash WT_BLOOM_HASH;
struct __wt_bm;
    typedef struct __wt_bm WT_BM;
struct __wt_btree;
    typedef struct __wt_btree WT_BTREE;
struct __wt_cache;
    typedef struct __wt_cache WT_CACHE;
struct __wt_cache_pool;
    typedef struct __wt_cache_pool WT_CACHE_POOL;
struct __wt_cell;
    typedef struct __wt_cell WT_CELL;
struct __wt_cell_unpack;
    typedef struct __wt_cell_unpack WT_CELL_UNPACK;
struct __wt_ckpt;
    typedef struct __wt_ckpt WT_CKPT;
struct __wt_col;
    typedef struct __wt_col WT_COL;
struct __wt_col_rle;
    typedef struct __wt_col_rle WT_COL_RLE;
struct __wt_colgroup;
    typedef struct __wt_colgroup WT_COLGROUP;
struct __wt_condvar;
    typedef struct __wt_condvar WT_CONDVAR;
struct __wt_config;
    typedef struct __wt_config WT_CONFIG;
struct __wt_config_check;
    typedef struct __wt_config_check WT_CONFIG_CHECK;
struct __wt_config_entry;
    typedef struct __wt_config_entry WT_CONFIG_ENTRY;
struct __wt_connection_impl;
    typedef struct __wt_connection_impl WT_CONNECTION_IMPL;
struct __wt_connection_stats;
    typedef struct __wt_connection_stats WT_CONNECTION_STATS;
struct __wt_cursor_backup;
    typedef struct __wt_cursor_backup WT_CURSOR_BACKUP;
struct __wt_cursor_backup_entry;
    typedef struct __wt_cursor_backup_entry WT_CURSOR_BACKUP_ENTRY;
struct __wt_cursor_btree;
    typedef struct __wt_cursor_btree WT_CURSOR_BTREE;
struct __wt_cursor_bulk;
    typedef struct __wt_cursor_bulk WT_CURSOR_BULK;
struct __wt_cursor_config;
    typedef struct __wt_cursor_config WT_CURSOR_CONFIG;
struct __wt_cursor_dump;
    typedef struct __wt_cursor_dump WT_CURSOR_DUMP;
struct __wt_cursor_index;
    typedef struct __wt_cursor_index WT_CURSOR_INDEX;
struct __wt_cursor_lsm;
    typedef struct __wt_cursor_lsm WT_CURSOR_LSM;
struct __wt_cursor_stat;
    typedef struct __wt_cursor_stat WT_CURSOR_STAT;
struct __wt_cursor_table;
    typedef struct __wt_cursor_table WT_CURSOR_TABLE;
struct __wt_data_handle;
    typedef struct __wt_data_handle WT_DATA_HANDLE;
struct __wt_data_handle_cache;
    typedef struct __wt_data_handle_cache WT_DATA_HANDLE_CACHE;
struct __wt_dlh;
    typedef struct __wt_dlh WT_DLH;
struct __wt_dsrc_stats;
    typedef struct __wt_dsrc_stats WT_DSRC_STATS;
struct __wt_evict_entry;
    typedef struct __wt_evict_entry WT_EVICT_ENTRY;
struct __wt_ext;
    typedef struct __wt_ext WT_EXT;
struct __wt_extlist;
    typedef struct __wt_extlist WT_EXTLIST;
struct __wt_fh;
    typedef struct __wt_fh WT_FH;
struct __wt_hazard;
    typedef struct __wt_hazard WT_HAZARD;
struct __wt_ikey;
    typedef struct __wt_ikey WT_IKEY;
struct __wt_index;
    typedef struct __wt_index WT_INDEX;
struct __wt_insert;
    typedef struct __wt_insert WT_INSERT;
struct __wt_insert_head;
    typedef struct __wt_insert_head WT_INSERT_HEAD;
struct __wt_lsm_chunk;
    typedef struct __wt_lsm_chunk WT_LSM_CHUNK;
struct __wt_lsm_data_source;
    typedef struct __wt_lsm_data_source WT_LSM_DATA_SOURCE;
struct __wt_lsm_tree;
    typedef struct __wt_lsm_tree WT_LSM_TREE;
struct __wt_lsm_worker_args;
    typedef struct __wt_lsm_worker_args WT_LSM_WORKER_ARGS;
struct __wt_lsm_worker_cookie;
    typedef struct __wt_lsm_worker_cookie WT_LSM_WORKER_COOKIE;
struct __wt_named_collator;
    typedef struct __wt_named_collator WT_NAMED_COLLATOR;
struct __wt_named_compressor;
    typedef struct __wt_named_compressor WT_NAMED_COMPRESSOR;
struct __wt_named_data_source;
    typedef struct __wt_named_data_source WT_NAMED_DATA_SOURCE;
struct __wt_page;
    typedef struct __wt_page WT_PAGE;
struct __wt_page_header;
    typedef struct __wt_page_header WT_PAGE_HEADER;
struct __wt_page_modify;
    typedef struct __wt_page_modify WT_PAGE_MODIFY;
struct __wt_page_track;
    typedef struct __wt_page_track WT_PAGE_TRACK;
struct __wt_process;
    typedef struct __wt_process WT_PROCESS;
struct __wt_ref;
    typedef struct __wt_ref WT_REF;
struct __wt_row;
    typedef struct __wt_row WT_ROW;
struct __wt_rwlock;
    typedef struct __wt_rwlock WT_RWLOCK;
struct __wt_salvage_cookie;
    typedef struct __wt_salvage_cookie WT_SALVAGE_COOKIE;
struct __wt_scratch_track;
    typedef struct __wt_scratch_track WT_SCRATCH_TRACK;
struct __wt_session_impl;
    typedef struct __wt_session_impl WT_SESSION_IMPL;
struct __wt_size;
    typedef struct __wt_size WT_SIZE;
struct __wt_stats;
    typedef struct __wt_stats WT_STATS;
struct __wt_table;
    typedef struct __wt_table WT_TABLE;
struct __wt_txn;
    typedef struct __wt_txn WT_TXN;
struct __wt_txn_global;
    typedef struct __wt_txn_global WT_TXN_GLOBAL;
struct __wt_txn_state;
    typedef struct __wt_txn_state WT_TXN_STATE;
struct __wt_update;
    typedef struct __wt_update WT_UPDATE;
/*
 * Forward type declarations for internal types: END
 * DO NOT EDIT: automatically built by dist/s_typedef.
 */

/*******************************************
 * WiredTiger internal include files.
 *******************************************/
#include "misc.h"
#include "mutex.h"
#include "posix.h"

#include "txn.h"			/* typedef for wt_txnid_t */
#include "stat.h"			/* required by dhandle.h */
#include "dhandle.h"			/* required by btree.h */

#include "api.h"
#include "block.h"
#include "bloom.h"
#include "btmem.h"
#include "btree.h"
#include "cache.h"
#include "config.h"
#include "cursor.h"
#include "dlh.h"
#include "error.h"
#include "flags.h"
#include "log.h"
#include "lsm.h"
#include "meta.h"
#include "os.h"
#include "schema.h"

#include "session.h"			/* required by connection.h */
#include "connection.h"

#include "extern.h"
#include "verify_build.h"

#include "intpack.i"			/* required by cell.i, packing.i */
#include "packing.i"
#include "cell.i"

#include "btree.i"			/* required by cursor.i */
#include "cache.i"			/* required by cursor.i */
#include "txn.i"			/* required by cursor.i */
#include "cursor.i"

#include "bitstring.i"
#include "column.i"
#include "log.i"
#include "mutex.i"
#include "serial_funcs.i"

#if defined(__cplusplus)
}
#endif
