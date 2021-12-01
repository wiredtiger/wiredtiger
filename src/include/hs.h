/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * History store runs in two modes: one history store file per data object, and one history store
 * file per database. The only time the modes intermix is when upgrading a database with a history
 * store file per database to have a history store file per data object. To make upgrade/downgrade
 * testing easy, there's a global variable that flags the mode in which we're running, defaulting to
 * "off", that is, by default it's a single history store file per database.
 */
#define WT_HS_MULTI __wt_process.multi_hs

/*
 * History store table support: when a page is being reconciled for eviction and has updates that
 * might be required by earlier readers in the system, the updates are written into the history
 * store table, and restored as necessary if the page is read.
 *
 * The first part of the key is comprised of a file ID, record key (byte-string for row-store,
 * record number for column-store) and timestamp. This allows us to search efficiently for a given
 * record key and read timestamp combination. The last part of the key is a monotonically increasing
 * counter to keep the key unique in the case where we have multiple transactions committing at the
 * same timestamp.
 * The value is the WT_UPDATE structure's:
 * 	- stop timestamp
 * 	- durable timestamp
 *	- update type
 *	- value.
 *
 * As the key for the history store table is different for row- and column-store, we store both key
 * types in a WT_ITEM, building/parsing them in the code, because otherwise we'd need two
 * history store files with different key formats. We could make the history store table's key
 * standard by moving the source key into the history store table value, but that doesn't make the
 * coding any simpler, and it makes the history store table's value more likely to overflow the page
 * size when the row-store key is relatively large.
 *
 * Note that we deliberately store the update type as larger than necessary (8 bytes vs 1 byte).
 * We've done this to leave room in case we need to store extra bit flags in this value at a later
 * point. If we need to store more information, we can potentially tack extra information at the end
 * of the "value" buffer and then use bit flags within the update type to determine how to interpret
 * it.
 *
 * We also configure a larger than default internal page size to accommodate for larger history
 * store keys. We do that to reduce the chances of having to create overflow keys on the page.
 */
#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
#define WT_HS_COMPRESSOR "snappy"
#else
#define WT_HS_COMPRESSOR "none"
#endif
#define WT_HS_KEY_FORMAT WT_UNCHECKED_STRING(IuQQ)
#define WT_HS_VALUE_FORMAT WT_UNCHECKED_STRING(QQQu)
#define WT_HS_CONFIG                                                   \
    "key_format=" WT_HS_KEY_FORMAT ",value_format=" WT_HS_VALUE_FORMAT \
    ",block_compressor=" WT_HS_COMPRESSOR                              \
    ",internal_page_max=16KB"                                          \
    ",leaf_value_max=64MB"                                             \
    ",prefix_compression=false"
