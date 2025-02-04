/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_bitmap_init --
 *     Allocate a bitmap structure. out must already be allocated.
 */
int
__wt_bitmap_init(WT_SESSION_IMPL *session, size_t nbits, WT_BITMAP *out)
{
    size_t nbytes = WT_CEIL_POS((double)nbits / 8.0);

    WT_RET(__wt_calloc(session, nbytes, sizeof(uint8_t), &out->internal));
    out->size = nbits;

    return (0);
}

/*
 * __wt_bitmap_free --
 *     Free a bitmap structure, previously allocated by __wt_bitmap_init.
 */
void
__wt_bitmap_free(WT_SESSION_IMPL *session, WT_BITMAP *map)
{
    __wt_free(session, map->internal);
    map->size = 0;
}

/*
 * __wt_bitmap_find_first --
 *     Find the first unset bit in the given map.
 */
int
__wt_bitmap_find_first(WT_BITMAP *map, size_t *bit_index)
{
    size_t j;
    uint8_t map_byte;

    size_t map_nbytes = map->size / 8;

    for (size_t i = 0; i < map_nbytes; i++) {
        map_byte = map->internal[i];
        if (map_byte != 0xff) {
            j = 0;
            while ((map_byte & 1) != 0) {
                j++;
                map_byte >>= 1;
            }

            *bit_index = ((i * 8) + j);
            return (0);
        }
    }

    for (j = 0; j < (map->size) % 8; j++)
        if ((map->internal[map_nbytes] & (0x01 << j)) == 0) {
            *bit_index = ((map_nbytes * 8) + j);
            return (0);
        }

    return (ENOSPC);
}

/*
 * __wt_bitmap_set --
 *     Set a specific bit in the given map.
 */
void
__wt_bitmap_set(WT_BITMAP *map, size_t idx)
{
    /* Bit index should be less than the maximum number of chunks that can be allocated. */
    WT_ASSERT(NULL, idx < map->size);

    uint8_t map_byte_expected = map->internal[idx / 8];
    uint8_t map_byte_mask = (uint8_t)(0x01 << (idx % 8));

    map->internal[idx / 8] = map_byte_expected | map_byte_mask;
}

/*
 * __wt_bitmap_isset --
 *     Check if a specific bit in the given map is set.
 */
bool
__wt_bitmap_isset(WT_BITMAP *map, size_t idx)
{
    /* Bit index should be less than the maximum number of chunks that can be allocated. */
    WT_ASSERT(NULL, idx < map->size);

    uint8_t map_byte = map->internal[idx / 8];
    uint8_t map_byte_mask = (uint8_t)(0x01 << (idx % 8));

    return (map_byte & map_byte_mask) == 0 ? false : true;
}

/*
 * __wt_bitmap_clear --
 *     Clear a specific bit in the given map.
 */
void
__wt_bitmap_clear(WT_BITMAP *map, size_t idx)
{
    /* Bit index should be less than the maximum number of chunks that can be allocated. */
    WT_ASSERT(NULL, idx < map->size);

    uint8_t map_byte_mask = (uint8_t)(0x01 << (idx % 8));
    uint8_t map_byte_current = map->internal[idx / 8];
    map->internal[idx / 8] = map_byte_current & ~map_byte_mask;
}
