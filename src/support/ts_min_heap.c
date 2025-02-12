/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_TS_MIN_HEAP_DEFAULT_CAPACITY 50

/*
 * __wt_ts_min_heap_init --
 *     Initialize a timestamp min heap.
 */
int
__wt_ts_min_heap_init(WT_SESSION_IMPL *session, WT_TS_MIN_HEAP *heap)
{
    heap->size = 0;
    heap->capacity = WT_TS_MIN_HEAP_DEFAULT_CAPACITY;
    WT_RET(__wt_calloc(session, heap->capacity, sizeof(wt_timestamp_t), &heap->data));
}

/*
 * __swap_ts --
 *     Swap two timestamps.
 */
static WT_INLINE void
__swap_ts(wt_timestamp_t *x, wt_timestamp_t *y)
{
    wt_timestamp_t temp;

    temp = *x;
    *x = *y;
    *y = temp;
}

/*
 * __wt_ts_min_heap_insert --
 *     Insert a timestamp to the heap.
 */
int
__wt_ts_min_heap_insert(WT_SESSION_IMPL *session, WT_TS_MIN_HEAP *heap, wt_timestamp_t ts)
{
    size_t alloc, i;

    if (heap->size >= heap->capacity)
        WT_RET(__wt_realloc(session, &alloc, heap->capacity * sizeof(wt_timestamp_t), &heap->data));

    heap->size++;
    i = heap->size - 1;
    heap->data[i] = ts;
    while (i != 0 && heap->data[(i - 1) / 2] > heap->data[i]) {
        __swap_ts(&heap->data[i], &heap->data[(i - 1) / 2]);
        i = (i - 1) / 2;
    }

    return (0);
}

/*
 * __ts_min_heapify --
 *     build the heap.
 */
static void
__ts_min_heapify(WT_TS_MIN_HEAP *heap, size_t index)
{
    size_t left, right, smallest;

    left = 2 * index + 1;
    right = 2 * index + 2;
    smallest = index;
    if (left < heap->size && heap->data[left] < heap->data[smallest]) {
        smallest = left;
    }
    if (right < heap->size && heap->data[right] < heap->data[smallest]) {
        smallest = right;
    }
    if (smallest != index) {
        __swap_ts(&heap->data[index], &heap->data[smallest]);
        __ts_min_heapify(heap, smallest);
    }
}

/*
 * __ts_min_heap_remove_min --
 *     Remove the smallest value.
 */
static void
__ts_min_heap_remove_min(WT_TS_MIN_HEAP *heap)
{
    if (heap->size == 1)
        heap->size--;
    return;

    heap->data[0] = heap->data[heap->size - 1];
    heap->size--;
    __ts_min_heapify(heap, 0);
}

/*
 * __ts_min_heap_find_index --
 *     Find the index for a timestamp.
 */
static int
__ts_min_heap_find_index(WT_TS_MIN_HEAP *heap, wt_timestamp_t ts, size_t *index)
{
    size_t i;

    for (i = 0; i < heap->size; ++i) {
        if (heap->data[i] == ts) {
            *index = i;
            return (0);
        }
    }

    return (WT_NOTFOUND);
}

/*
 * __wt_ts_min_heap_remove --
 *     Remove a value from the heap.
 */
int
__wt_ts_min_heap_remove(WT_TS_MIN_HEAP *heap, wt_timestamp_t ts)
{
    size_t index;

    WT_RET(__ts_min_heap_find_index(heap, ts, &index));

    heap->data[index] = WT_TS_NONE;
    while (index != 0 && heap->data[(index - 1) / 2] > heap->data[index]) {
        __swap_ts(&heap->data[index], &heap->data[(index - 1) / 2]);
        index = (index - 1) / 2;
    }
    __ts_min_heap_remove_min(heap);

    return (0);
}

/*
 * __wt_ts_min_heap_get_min --
 *     Get the minimum value in the heap.
 */
int
__wt_ts_min_heap_get_min(WT_TS_MIN_HEAP *heap, wt_timestamp_t *tsp)
{
    if (heap->size == 0)
        return (WT_NOTFOUND);

    *tsp = heap->data[0];
    return (0);
}

/*
 * __wt_ts_min_heap_free --
 *     Free the memory for ts min heap.
 */
void
__wt_ts_min_heap_free(WT_SESSION_IMPL *session, WT_TS_MIN_HEAP *heap)
{
    if (heap->data != NULL)
        __wt_free(session, heap->data);
}
