/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include "wt_internal.h"

void validate_and_free_ext_list(WT_BLOCK_MGR_SESSION *bms, int expected_items);
void free_ext_list(WT_BLOCK_MGR_SESSION *bms);
void validate_and_free_ext_block(WT_EXT *ext);
void validate_ext_list(WT_BLOCK_MGR_SESSION *bms, int expected_items);
void free_ext_block(WT_EXT *ext);
void validate_ext_block(WT_EXT *ext);
