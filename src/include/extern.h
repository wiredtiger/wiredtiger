#pragma once

/*
 * __wt_btcur_bounds_early_exit --
 *     Performs bound comparison to check if the key is within bounds, if not, increment the
 *     appropriate stat, early exit, and return WT_NOTFOUND.
 */
static WT_INLINE int __wt_btcur_bounds_early_exit(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  bool next, bool *key_out_of_boundsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_skip_page --
 *     Return if the cursor is pointing to a page with deleted records and can be skipped for cursor
 *     traversal.
 */
static WT_INLINE int __wt_btcur_skip_page(WT_SESSION_IMPL *session, WT_REF *ref, void *context,
  bool visible_all, bool *skipp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_block_free --
 *     Helper function to free a block from the current tree.
 */
static WT_INLINE int __wt_btree_block_free(WT_SESSION_IMPL *session, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_bytes_evictable --
 *     Return the number of bytes that can be evicted (i.e. bytes apart from the pinned root page).
 */
static WT_INLINE uint64_t __wt_btree_bytes_evictable(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_bytes_inuse --
 *     Return the number of bytes in use.
 */
static WT_INLINE uint64_t __wt_btree_bytes_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_bytes_updates --
 *     Return the number of bytes in use by dirty leaf pages.
 */
static WT_INLINE uint64_t __wt_btree_bytes_updates(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_dirty_inuse --
 *     Return the number of dirty bytes in use.
 */
static WT_INLINE uint64_t __wt_btree_dirty_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_dirty_leaf_inuse --
 *     Return the number of bytes in use by dirty leaf pages.
 */
static WT_INLINE uint64_t __wt_btree_dirty_leaf_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_disable_bulk --
 *     Disable bulk loads into a tree.
 */
static WT_INLINE void __wt_btree_disable_bulk(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_dominating_cache --
 *     Return if a single btree is occupying at least half of any of our target's cache usage.
 */
static WT_INLINE bool __wt_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_lsm_over_size --
 *     Return if the size of an in-memory tree with a single leaf page is over a specified maximum.
 *     If called on anything other than a simple tree with a single leaf page, returns true so our
 *     LSM caller will switch to a new tree.
 */
static WT_INLINE bool __wt_btree_lsm_over_size(WT_SESSION_IMPL *session, uint64_t maxsize)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_syncing_by_other_session --
 *     Returns true if the session's current btree is being synced by another thread.
 */
static WT_INLINE bool __wt_btree_syncing_by_other_session(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_extend --
 *     Grow a buffer that's currently in-use.
 */
static WT_INLINE int __wt_buf_extend(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_free --
 *     Free a buffer.
 */
static WT_INLINE void __wt_buf_free(WT_SESSION_IMPL *session, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_grow --
 *     Grow a buffer that may be in-use, and ensure that all data is local to the buffer.
 */
static WT_INLINE int __wt_buf_grow(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_init --
 *     Create an empty buffer at a specific size.
 */
static WT_INLINE int __wt_buf_init(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_initsize --
 *     Create an empty buffer at a specific size, and set the data length.
 */
static WT_INLINE int __wt_buf_initsize(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_set --
 *     Set the contents of the buffer.
 */
static WT_INLINE int __wt_buf_set(WT_SESSION_IMPL *session, WT_ITEM *buf, const void *data,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_setstr --
 *     Set the contents of the buffer to a NUL-terminated string.
 */
static WT_INLINE int __wt_buf_setstr(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *s)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_aggressive --
 *     Indicate if the cache is operating in aggressive mode.
 */
static WT_INLINE bool __wt_cache_aggressive(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_bytes_image --
 *     Return the number of page image bytes in use.
 */
static WT_INLINE uint64_t __wt_cache_bytes_image(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_bytes_inuse --
 *     Return the number of bytes in use.
 */
static WT_INLINE uint64_t __wt_cache_bytes_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_bytes_other --
 *     Return the number of bytes in use not for page images.
 */
static WT_INLINE uint64_t __wt_cache_bytes_other(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_bytes_plus_overhead --
 *     Apply the cache overhead to a size in bytes.
 */
static WT_INLINE uint64_t __wt_cache_bytes_plus_overhead(WT_CACHE *cache, uint64_t sz)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_bytes_updates --
 *     Return the number of bytes in use for updates.
 */
static WT_INLINE uint64_t __wt_cache_bytes_updates(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_decr_check_size --
 *     Decrement a size_t cache value and check for underflow.
 */
static WT_INLINE void __wt_cache_decr_check_size(WT_SESSION_IMPL *session, size_t *vp, size_t v,
  const char *fld) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_decr_check_uint64 --
 *     Decrement a uint64_t cache value and check for underflow.
 */
static WT_INLINE void __wt_cache_decr_check_uint64(WT_SESSION_IMPL *session, uint64_t *vp,
  uint64_t v, const char *fld) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_dirty_decr --
 *     Page switch from dirty to clean: decrement the cache dirty page/byte counts.
 */
static WT_INLINE void __wt_cache_dirty_decr(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_dirty_incr --
 *     Page switch from clean to dirty: increment the cache dirty page/byte counts.
 */
static WT_INLINE void __wt_cache_dirty_incr(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_dirty_inuse --
 *     Return the number of dirty bytes in use.
 */
static WT_INLINE uint64_t __wt_cache_dirty_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_dirty_leaf_inuse --
 *     Return the number of dirty bytes in use by leaf pages.
 */
static WT_INLINE uint64_t __wt_cache_dirty_leaf_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_eviction_check --
 *     Evict pages if the cache crosses its boundaries.
 */
static WT_INLINE int __wt_cache_eviction_check(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool *didworkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_full --
 *     Return if the cache is at (or over) capacity.
 */
static WT_INLINE bool __wt_cache_full(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_hs_dirty --
 *     Return if a major portion of the cache is dirty due to history store content.
 */
static WT_INLINE bool __wt_cache_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_byte_dirty_decr --
 *     Decrement the page's dirty byte count, guarding from underflow.
 */
static WT_INLINE void __wt_cache_page_byte_dirty_decr(WT_SESSION_IMPL *session, WT_PAGE *page,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_byte_updates_decr --
 *     Decrement the page's update byte count, guarding from underflow.
 */
static WT_INLINE void __wt_cache_page_byte_updates_decr(WT_SESSION_IMPL *session, WT_PAGE *page,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_evict --
 *     Evict pages from the cache.
 */
static WT_INLINE void __wt_cache_page_evict(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_image_decr --
 *     Decrement a page image's size to the cache.
 */
static WT_INLINE void __wt_cache_page_image_decr(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_image_incr --
 *     Increment a page image's size to the cache.
 */
static WT_INLINE void __wt_cache_page_image_incr(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_inmem_decr --
 *     Decrement a page's memory footprint in the cache.
 */
static WT_INLINE void __wt_cache_page_inmem_decr(WT_SESSION_IMPL *session, WT_PAGE *page,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_page_inmem_incr --
 *     Increment a page's memory footprint in the cache.
 */
static WT_INLINE void __wt_cache_page_inmem_incr(WT_SESSION_IMPL *session, WT_PAGE *page,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_pages_inuse --
 *     Return the number of pages in use.
 */
static WT_INLINE uint64_t __wt_cache_pages_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_read_gen --
 *     Get the current read generation number.
 */
static WT_INLINE uint64_t __wt_cache_read_gen(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_read_gen_bump --
 *     Update the page's read generation.
 */
static WT_INLINE void __wt_cache_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_read_gen_incr --
 *     Increment the current read generation number.
 */
static WT_INLINE void __wt_cache_read_gen_incr(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_read_gen_new --
 *     Get the read generation for a new page in memory.
 */
static WT_INLINE void __wt_cache_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cache_stuck --
 *     Indicate if the cache is stuck (i.e., not making progress).
 */
static WT_INLINE bool __wt_cache_stuck(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_get_ta --
 *     Get the underlying time aggregate from an unpacked address cell.
 */
static WT_INLINE void __wt_cell_get_ta(WT_CELL_UNPACK_ADDR *unpack_addr, WT_TIME_AGGREGATE **tap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_get_tw --
 *     Get the underlying time window from an unpacked value cell.
 */
static WT_INLINE void __wt_cell_get_tw(WT_CELL_UNPACK_KV *unpack_value, WT_TIME_WINDOW **twp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_leaf_value_parse --
 *     Return the cell if it's a row-store leaf page value, otherwise return NULL.
 */
static WT_INLINE WT_CELL *__wt_cell_leaf_value_parse(WT_PAGE *page, WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_addr --
 *     Pack an address cell.
 */
static WT_INLINE size_t __wt_cell_pack_addr(WT_SESSION_IMPL *session, WT_CELL *cell,
  u_int cell_type, uint64_t recno, WT_PAGE_DELETED *page_del, WT_TIME_AGGREGATE *ta, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_copy --
 *     Write a copy value cell.
 */
static WT_INLINE size_t __wt_cell_pack_copy(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle, uint64_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_del --
 *     Write a deleted value cell.
 */
static WT_INLINE size_t __wt_cell_pack_del(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_int_key --
 *     Set a row-store internal page key's WT_CELL contents.
 */
static WT_INLINE size_t __wt_cell_pack_int_key(WT_CELL *cell, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_leaf_key --
 *     Set a row-store leaf page key's WT_CELL contents.
 */
static WT_INLINE size_t __wt_cell_pack_leaf_key(WT_CELL *cell, uint8_t prefix, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_ovfl --
 *     Pack an overflow cell.
 */
static WT_INLINE size_t __wt_cell_pack_ovfl(WT_SESSION_IMPL *session, WT_CELL *cell, uint8_t type,
  WT_TIME_WINDOW *tw, uint64_t rle, size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_value --
 *     Set a value item's WT_CELL contents.
 */
static WT_INLINE size_t __wt_cell_pack_value(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle, size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_pack_value_match --
 *     Return if two value items would have identical WT_CELLs (except for their validity window and
 *     any RLE).
 */
static WT_INLINE int __wt_cell_pack_value_match(WT_CELL *page_cell, WT_CELL *val_cell,
  const uint8_t *val_data, bool *matchp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_rle --
 *     Return the cell's RLE value.
 */
static WT_INLINE uint64_t __wt_cell_rle(WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_total_len --
 *     Return the cell's total length, including data.
 */
static WT_INLINE size_t __wt_cell_total_len(void *unpack_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_type --
 *     Return the cell's type (collapsing special types).
 */
static WT_INLINE u_int __wt_cell_type(WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_type_raw --
 *     Return the cell's type.
 */
static WT_INLINE u_int __wt_cell_type_raw(WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_type_reset --
 *     Reset the cell's type.
 */
static WT_INLINE void __wt_cell_type_reset(WT_SESSION_IMPL *session, WT_CELL *cell, u_int old_type,
  u_int new_type) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_unpack_addr --
 *     Unpack an address WT_CELL into a structure.
 */
static WT_INLINE void __wt_cell_unpack_addr(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_ADDR *unpack_addr) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_unpack_kv --
 *     Unpack a value WT_CELL into a structure.
 */
static WT_INLINE void __wt_cell_unpack_kv(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_KV *unpack_value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cell_unpack_safe --
 *     Unpack a WT_CELL into a structure, with optional boundary checks.
 */
static WT_INLINE int __wt_cell_unpack_safe(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_ADDR *unpack_addr, WT_CELL_UNPACK_KV *unpack_value, const void *end)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_check_addr_validity --
 *     Check the address' validity window for sanity.
 */
static WT_INLINE int __wt_check_addr_validity(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta,
  bool expected_error) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checksum_alt_match --
 *     Return if a checksum matches the alternate calculation.
 */
extern bool __wt_checksum_alt_match(const void *chunk, size_t len, uint32_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checksum_match --
 *     Return if a checksum matches either the primary or alternate values.
 */
static WT_INLINE bool __wt_checksum_match(const void *chunk, size_t len, uint32_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checksum_match --
 *     Return if a checksum matches.
 */
static WT_INLINE bool __wt_checksum_match(const void *chunk, size_t len, uint32_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checksum_sw --
 *     Return a checksum for a chunk of memory, computed in software.
 */
extern uint32_t __wt_checksum_sw(const void *chunk, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checksum_with_seed_sw --
 *     Return a checksum for a chunk of memory, computed in software. Takes an initial starting CRC
 *     seed value.
 */
extern uint32_t __wt_checksum_with_seed_sw(uint32_t seed, const void *chunk, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_clock --
 *     Obtain a timestamp via either a CPU register or via a system call on platforms where
 *     obtaining it directly from the hardware register is not supported.
 */
static WT_INLINE uint64_t __wt_clock(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_clock_to_nsec --
 *     Convert from clock ticks to nanoseconds.
 */
static WT_INLINE uint64_t __wt_clock_to_nsec(uint64_t end, uint64_t begin)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_col_append_serial --
 *     Append a new column-store entry.
 */
static WT_INLINE int __wt_col_append_serial(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
  uint64_t *recnop, u_int skipdepth, bool exclusive)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_compare --
 *     The same as __wt_lex_compare, but using the application's collator function when configured.
 */
static WT_INLINE int __wt_compare(WT_SESSION_IMPL *session, WT_COLLATOR *collator,
  const WT_ITEM *user_item, const WT_ITEM *tree_item, int *cmpp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_compare_bounds --
 *     Return if the cursor key is within the bounded range. If upper is True, this indicates a next
 *     call and the key is checked against the upper bound. If upper is False, this indicates a prev
 *     call and the key is then checked against the lower bound.
 */
static WT_INLINE int __wt_compare_bounds(WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_ITEM *key,
  uint64_t recno, bool upper, bool *key_out_of_bounds)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_compare_skip --
 *     The same as __wt_lex_compare_skip, but using the application's collator function when
 *     configured.
 */
static WT_INLINE int __wt_compare_skip(WT_SESSION_IMPL *session, WT_COLLATOR *collator,
  const WT_ITEM *user_item, const WT_ITEM *tree_item, int *cmpp, size_t *matchp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_wait --
 *     Wait on a mutex, optionally timing out.
 */
static WT_INLINE void __wt_cond_wait(WT_SESSION_IMPL *session, WT_CONDVAR *cond, uint64_t usecs,
  bool (*run_func)(WT_SESSION_IMPL *)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_check_choice --
 *     Check the string value against a list of choices, if it is found, set up the value so it can
 *     be checked against a particular choice quickly.
 */
static WT_INLINE int __wt_conf_check_choice(
  WT_SESSION_IMPL *session, const char **choices, const char *str, size_t len, const char **result)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_check_one --
 *     Do all configuration checks for a single value.
 */
static WT_INLINE int __wt_conf_check_one(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *check,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_get_compiled --
 *     Return true if and only if the given string is a dummy compiled string, and if so, return the
 *     compiled structure.
 */
static WT_INLINE bool __wt_conf_get_compiled(WT_CONNECTION_IMPL *conn, const char *config,
  WT_CONF **confp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_gets_def_func --
 *     Get a value from the compiled configuration. If the value is a default, return that.
 */
static WT_INLINE int __wt_conf_gets_def_func(WT_SESSION_IMPL *session, const WT_CONF *conf,
  uint64_t keys, int def, WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_is_compiled --
 *     Return true if and only if the given string is a dummy compiled string.
 */
static WT_INLINE bool __wt_conf_is_compiled(WT_CONNECTION_IMPL *conn, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_get_btree --
 *     Convert a history store cursor to the underlying btree.
 */
static WT_INLINE WT_BTREE *__wt_curhs_get_btree(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_get_cbt --
 *     Convert a history store cursor to the underlying btree cursor.
 */
static WT_INLINE WT_CURSOR_BTREE *__wt_curhs_get_cbt(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curindex_get_valuev --
 *     Internal implementation of WT_CURSOR->get_value for index cursors
 */
static WT_INLINE int __wt_curindex_get_valuev(WT_CURSOR *cursor, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_bound_reset --
 *     Clear any bounds on the cursor if they are set.
 */
static WT_INLINE void __wt_cursor_bound_reset(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_dhandle_decr_use --
 *     Decrement the in-use counter in the cursor's data source.
 */
static WT_INLINE void __wt_cursor_dhandle_decr_use(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_dhandle_incr_use --
 *     Increment the in-use counter in the cursor's data source.
 */
static WT_INLINE void __wt_cursor_dhandle_incr_use(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_free_cached_memory --
 *     If a cached cursor is still holding memory, free it now.
 */
static WT_INLINE void __wt_cursor_free_cached_memory(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_func_init --
 *     Cursor call setup.
 */
static WT_INLINE int __wt_cursor_func_init(WT_CURSOR_BTREE *cbt, bool reenter)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_has_cached_memory --
 *     Return true if a cursor is holding memory in either key or value.
 */
static WT_INLINE bool __wt_cursor_has_cached_memory(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_localkey --
 *     If the key points into the tree, get a local copy.
 */
static WT_INLINE int __wt_cursor_localkey(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curtable_get_valuev --
 *     Internal implementation of WT_CURSOR->get_value for table cursors.
 */
static WT_INLINE int __wt_curtable_get_valuev(WT_CURSOR *cursor, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dsk_cell_data_ref_addr --
 *     Set a buffer to reference the data from an unpacked address cell.
 */
static WT_INLINE int __wt_dsk_cell_data_ref_addr(WT_SESSION_IMPL *session, int page_type,
  WT_CELL_UNPACK_ADDR *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dsk_cell_data_ref_kv --
 *     Set a buffer to reference the data from an unpacked key value cell. There are two versions
 *     because of WT_CELL_VALUE_OVFL_RM type cells. When an overflow item is deleted, its backing
 *     blocks are removed; if there are still running transactions that might need to see the
 *     overflow item, we cache a copy of the item and reset the item's cell to
 *     WT_CELL_VALUE_OVFL_RM. If we find a WT_CELL_VALUE_OVFL_RM cell when reading an overflow item,
 *     we use the page reference to look aside into the cache. So, calling the "dsk" version of the
 *     function declares the cell cannot be of type WT_CELL_VALUE_OVFL_RM, and calling the "page"
 *     version means it might be.
 */
static WT_INLINE int __wt_dsk_cell_data_ref_kv(WT_SESSION_IMPL *session, int page_type,
  WT_CELL_UNPACK_KV *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_epoch --
 *     Return the time since the Epoch.
 */
static WT_INLINE void __wt_epoch(WT_SESSION_IMPL *session, struct timespec *tsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_clean_needed --
 *     Return if an application thread should do eviction due to the total volume of data in cache.
 */
static WT_INLINE bool __wt_eviction_clean_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_clean_pressure --
 *     Return true if clean cache is stressed and will soon require application threads to evict
 *     content.
 */
static WT_INLINE bool __wt_eviction_clean_pressure(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_dirty_needed --
 *     Return if an application thread should do eviction due to the total volume of dirty data in
 *     cache.
 */
static WT_INLINE bool __wt_eviction_dirty_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_dirty_target --
 *     Return the effective dirty target (including checkpoint scrubbing).
 */
static WT_INLINE double __wt_eviction_dirty_target(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_needed --
 *     Return if an application thread should do eviction, and the cache full percentage as a
 *     side-effect.
 */
static WT_INLINE bool __wt_eviction_needed(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_eviction_updates_needed --
 *     Return if an application thread should do eviction due to the total volume of updates in
 *     cache.
 */
static WT_INLINE bool __wt_eviction_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_extlist_read_pair --
 *     Read an extent list pair.
 */
static WT_INLINE int __wt_extlist_read_pair(const uint8_t **p, wt_off_t *offp, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_extlist_write_pair --
 *     Write an extent list pair.
 */
static WT_INLINE int __wt_extlist_write_pair(uint8_t **p, wt_off_t off, wt_off_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_failpoint --
 *     A generic failpoint function, it will return true if the failpoint triggers. Takes an
 *     unsigned integer from 0 to 10000 representing an X in 10000 chance of occurring.
 */
static WT_INLINE bool __wt_failpoint(WT_SESSION_IMPL *session, uint64_t conn_flag,
  u_int probability) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fclose --
 *     Close a stream.
 */
static WT_INLINE int __wt_fclose(WT_SESSION_IMPL *session, WT_FSTREAM **fstrp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fextend --
 *     Extend a file.
 */
static WT_INLINE int __wt_fextend(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fflush --
 *     Flush a stream.
 */
static WT_INLINE int __wt_fflush(WT_SESSION_IMPL *session, WT_FSTREAM *fstr)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_file_lock --
 *     Lock/unlock a file.
 */
static WT_INLINE int __wt_file_lock(WT_SESSION_IMPL *session, WT_FH *fh, bool lock)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_filesize --
 *     Get the size of a file in bytes, by file handle.
 */
static WT_INLINE int __wt_filesize(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fprintf --
 *     ANSI C fprintf.
 */
static WT_INLINE int __wt_fprintf(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_directory_list --
 *     Return a list of files from a directory.
 */
static WT_INLINE int __wt_fs_directory_list(
  WT_SESSION_IMPL *session, const char *dir, const char *prefix, char ***dirlistp, u_int *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_directory_list_free --
 *     Free memory allocated by __wt_fs_directory_list.
 */
static WT_INLINE int __wt_fs_directory_list_free(WT_SESSION_IMPL *session, char ***dirlistp,
  u_int count) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_directory_list_single --
 *     Return a single matching file from a directory.
 */
static WT_INLINE int __wt_fs_directory_list_single(
  WT_SESSION_IMPL *session, const char *dir, const char *prefix, char ***dirlistp, u_int *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_exist --
 *     Return if the file exists.
 */
static WT_INLINE int __wt_fs_exist(WT_SESSION_IMPL *session, const char *name, bool *existp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_file_system --
 *     Get the active file system handle.
 */
static WT_INLINE WT_FILE_SYSTEM *__wt_fs_file_system(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_remove --
 *     Remove the file.
 */
static WT_INLINE int __wt_fs_remove(WT_SESSION_IMPL *session, const char *name, bool durable,
  bool locked) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_rename --
 *     Rename the file.
 */
static WT_INLINE int __wt_fs_rename(WT_SESSION_IMPL *session, const char *from, const char *to,
  bool durable) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fs_size --
 *     Return the size of a file in bytes, by file name.
 */
static WT_INLINE int __wt_fs_size(WT_SESSION_IMPL *session, const char *name, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fsync --
 *     POSIX fsync.
 */
static WT_INLINE int __wt_fsync(WT_SESSION_IMPL *session, WT_FH *fh, bool block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ftruncate --
 *     Truncate a file.
 */
static WT_INLINE int __wt_ftruncate(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_get_page_modify_ta --
 *     Returns the page modify stop time aggregate information if exists.
 */
static inline bool __wt_get_page_modify_ta(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_TIME_AGGREGATE **ta) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_getline --
 *     Get a line from a stream.
 */
static WT_INLINE int __wt_getline(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hex --
 *     Convert a byte to a hex character.
 */
static WT_INLINE u_char __wt_hex(int c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_insert_serial --
 *     Insert a row or column-store entry.
 */
static WT_INLINE int __wt_insert_serial(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
  u_int skipdepth, bool exclusive) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isalnum --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isalnum(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isalpha --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isalpha(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isascii --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isascii(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isdigit --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isdigit(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isprint --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isprint(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_isspace --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE bool __wt_isspace(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_leaf_page_can_split --
 *     Check whether a page can be split in memory.
 */
static WT_INLINE bool __wt_leaf_page_can_split(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lex_compare --
 *     Lexicographic comparison routine. Returns: < 0 if user_item is lexicographically < tree_item,
 *     = 0 if user_item is lexicographically = tree_item, > 0 if user_item is lexicographically >
 *     tree_item. We use the names "user" and "tree" so it's clear in the btree code which the
 *     application is looking at when we call its comparison function.
 */
static WT_INLINE int __wt_lex_compare(const WT_ITEM *user_item, const WT_ITEM *tree_item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lex_compare_short --
 *     Lexicographic comparison routine for short keys. Returns: < 0 if user_item is
 *     lexicographically < tree_item = 0 if user_item is lexicographically = tree_item > 0 if
 *     user_item is lexicographically > tree_item We use the names "user" and "tree" so it's clear
 *     in the btree code which the application is looking at when we call its comparison function.
 */
static WT_INLINE int __wt_lex_compare_short(const WT_ITEM *user_item, const WT_ITEM *tree_item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lex_compare_skip --
 *     Lexicographic comparison routine, skipping leading bytes. Returns: < 0 if user_item is
 *     lexicographically < tree_item = 0 if user_item is lexicographically = tree_item > 0 if
 *     user_item is lexicographically > tree_item We use the names "user" and "tree" so it's clear
 *     in the btree code which the application is looking at when we call its comparison function.
 */
static WT_INLINE int __wt_lex_compare_skip(WT_SESSION_IMPL *session, const WT_ITEM *user_item,
  const WT_ITEM *tree_item, size_t *matchp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_cmp --
 *     Compare 2 LSNs, return -1 if lsn1 < lsn2, 0if lsn1 == lsn2 and 1 if lsn1 > lsn2.
 */
static WT_INLINE int __wt_log_cmp(WT_LSN *lsn1, WT_LSN *lsn2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log_op --
 *     Return if an operation should be logged.
 */
static WT_INLINE bool __wt_log_op(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_lsn_offset --
 *     Return a log sequence number's offset.
 */
static WT_INLINE uint32_t __wt_lsn_offset(WT_LSN *lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_off_page --
 *     Return if a pointer references off-page data.
 */
static WT_INLINE bool __wt_off_page(WT_PAGE *page, const void *p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_op_timer_fired --
 *     Check the operations timers.
 */
static WT_INLINE bool __wt_op_timer_fired(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_op_timer_start --
 *     Start the operations timer.
 */
static WT_INLINE void __wt_op_timer_start(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_op_timer_stop --
 *     Stop the operations timer.
 */
static WT_INLINE void __wt_op_timer_stop(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_can_evict --
 *     Check whether a page can be evicted.
 */
static WT_INLINE bool __wt_page_can_evict(WT_SESSION_IMPL *session, WT_REF *ref, bool *inmem_splitp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_cell_data_ref_kv --
 *     Set a buffer to reference the data from an unpacked key value cell.
 */
static WT_INLINE int __wt_page_cell_data_ref_kv(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_CELL_UNPACK_KV *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_del_committed_set --
 *     Return if a truncate operation is resolved. (Since truncations that abort are removed
 *     immediately, "resolved" and "committed" are equivalent here.) The caller should have already
 *     locked the ref and confirmed that the ref's previous state was WT_REF_DELETED. The page_del
 *     argument should be the ref's page_del member. This function should only be used for pages in
 *     WT_REF_DELETED state. For deleted pages that have been instantiated in memory, the update
 *     list in the page modify structure should be checked instead, as the page_del structure might
 *     have been discarded already. (The update list is non-null if the transaction is unresolved.)
 */
static WT_INLINE bool __wt_page_del_committed_set(WT_PAGE_DELETED *page_del)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_del_visible --
 *     Return if a truncate operation is visible to the caller. The same considerations apply as in
 *     the visible_all version.
 */
static WT_INLINE bool __wt_page_del_visible(WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del,
  bool hide_prepared) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_del_visible_all --
 *     Check if a truncate operation is visible to everyone and the data under it is obsolete.
 */
static WT_INLINE bool __wt_page_del_visible_all(WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del,
  bool hide_prepared) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_dirty_and_evict_soon --
 *     Mark a page dirty and set it to be evicted as soon as possible.
 */
static WT_INLINE int __wt_page_dirty_and_evict_soon(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_evict_clean --
 *     Return if the page can be evicted without dirtying the tree.
 */
static WT_INLINE bool __wt_page_evict_clean(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_evict_retry --
 *     Avoid busy-spinning attempting to evict the same page all the time.
 */
static WT_INLINE bool __wt_page_evict_retry(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_evict_soon --
 *     Set a page to be evicted as soon as possible.
 */
static WT_INLINE void __wt_page_evict_soon(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_evict_soon_check --
 *     Check whether the page should be evicted urgently.
 */
static WT_INLINE bool __wt_page_evict_soon_check(WT_SESSION_IMPL *session, WT_REF *ref,
  bool *inmem_split) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_is_empty --
 *     Return if the page is empty.
 */
static WT_INLINE bool __wt_page_is_empty(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_is_modified --
 *     Return if the page is dirty.
 */
static WT_INLINE bool __wt_page_is_modified(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_is_reconciling --
 *     Return if the page is being reconciled.
 */
static WT_INLINE bool __wt_page_is_reconciling(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_modify_clear --
 *     Clean a modified page.
 */
static WT_INLINE void __wt_page_modify_clear(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_modify_init --
 *     A page is about to be modified, allocate the modification structure.
 */
static WT_INLINE int __wt_page_modify_init(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_modify_set --
 *     Mark the page and tree dirty.
 */
static WT_INLINE void __wt_page_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_only_modify_set --
 *     Mark the page (but only the page) dirty.
 */
static WT_INLINE void __wt_page_only_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_parent_modify_set --
 *     Mark the parent page, and optionally the tree, dirty.
 */
static WT_INLINE int __wt_page_parent_modify_set(WT_SESSION_IMPL *session, WT_REF *ref,
  bool page_only) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_release --
 *     Release a reference to a page.
 */
static WT_INLINE int __wt_page_release(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_swap_func --
 *     Swap one page's hazard pointer for another one when hazard pointer coupling up/down the tree.
 */
static WT_INLINE int __wt_page_swap_func(
  WT_SESSION_IMPL *session, WT_REF *held, WT_REF *want, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_type_str --
 *     Convert a page type to its string representation.
 */
static WT_INLINE const char *__wt_page_type_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_prepare_state_str --
 *     Convert a prepare state to its string representation.
 */
static WT_INLINE const char *__wt_prepare_state_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rdtsc --
 *     Get a timestamp from CPU registers.
 */
static WT_INLINE uint64_t __wt_rdtsc(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_read --
 *     POSIX pread.
 */
static WT_INLINE int __wt_read(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t len,
  void *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_read_shared_double --
 *     This function enables suppressing TSan warnings about reading doubles in a shared context.
 */
static WT_INLINE double __wt_read_shared_double(double *to_read)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_readgen_evict_soon --
 *     Return whether a page's read generation makes it eligible for immediate eviction. Read
 *     generations reserve a range of low numbers for special meanings and currently - with the
 *     exception of the generation not being set - these indicate the page may be evicted
 *     immediately.
 */
static WT_INLINE bool __wt_readgen_evict_soon(uint64_t *readgen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_auximage_copy --
 *     Copy a key/value cell and buffer pair into the new auxiliary image.
 */
static WT_INLINE void __wt_rec_auximage_copy(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  uint32_t count, WT_REC_KV *kv) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_auxincr --
 *     Update the memory tracking structure for a set of new entries in the auxiliary image.
 */
static WT_INLINE void __wt_rec_auxincr(WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_cell_build_addr --
 *     Process an address or unpack reference and return a cell structure to be stored on the page.
 */
static WT_INLINE void __wt_rec_cell_build_addr(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  WT_ADDR *addr, WT_CELL_UNPACK_ADDR *vpack, uint64_t recno, WT_PAGE_DELETED *page_del)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_cell_build_val --
 *     Process a data item and return a WT_CELL structure and byte string to be stored on the page.
 */
static WT_INLINE int __wt_rec_cell_build_val(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  const void *data, size_t size, WT_TIME_WINDOW *tw, uint64_t rle)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_dict_replace --
 *     Check for a dictionary match.
 */
static WT_INLINE int __wt_rec_dict_replace(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_TIME_WINDOW *tw, uint64_t rle, WT_REC_KV *val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_image_copy --
 *     Copy a key/value cell and buffer pair into the new image.
 */
static WT_INLINE void __wt_rec_image_copy(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REC_KV *kv)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_incr --
 *     Update the memory tracking structure for a set of new entries.
 */
static WT_INLINE void __wt_rec_incr(WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_need_split --
 *     Check whether adding some bytes to the page requires a split.
 */
static WT_INLINE bool __wt_rec_need_split(WT_RECONCILE *r, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_time_window_clear_obsolete --
 *     Where possible modify time window values to avoid writing obsolete values to the cell later.
 */
static WT_INLINE void __wt_rec_time_window_clear_obsolete(
  WT_SESSION_IMPL *session, WT_UPDATE_SELECT *upd_select, WT_CELL_UNPACK_KV *vpack, WT_RECONCILE *r)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_addr_copy --
 *     Return a copy of the WT_REF address information.
 */
static WT_INLINE bool __wt_ref_addr_copy(WT_SESSION_IMPL *session, WT_REF *ref, WT_ADDR_COPY *copy)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_block_free --
 *     Free the on-disk block for a reference and clear the address.
 */
static WT_INLINE int __wt_ref_block_free(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_is_root --
 *     Return if the page reference is for the root page.
 */
static WT_INLINE bool __wt_ref_is_root(WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_key --
 *     Return a reference to a row-store internal page key as cheaply as possible.
 */
static WT_INLINE void __wt_ref_key(WT_PAGE *page, WT_REF *ref, void *keyp, size_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_key_clear --
 *     Clear a WT_REF key.
 */
static WT_INLINE void __wt_ref_key_clear(WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_key_instantiated --
 *     Return if a WT_REF key is instantiated.
 */
static WT_INLINE WT_IKEY *__wt_ref_key_instantiated(WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_key_onpage_set --
 *     Set a WT_REF to reference an on-page key.
 */
static WT_INLINE void __wt_ref_key_onpage_set(WT_PAGE *page, WT_REF *ref,
  WT_CELL_UNPACK_ADDR *unpack) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key --
 *     Set a buffer to reference a row-store leaf page key as cheaply as possible.
 */
static WT_INLINE int __wt_row_leaf_key(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip,
  WT_ITEM *key, bool instantiate) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_free --
 *     Discard any memory allocated for an instantiated key.
 */
static WT_INLINE void __wt_row_leaf_key_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_info --
 *     Return a row-store leaf page key referenced by a WT_ROW if it can be had without unpacking a
 *     cell, and information about the cell, if the key isn't cheaply available.
 */
static WT_INLINE void __wt_row_leaf_key_info(WT_PAGE *page, void *copy, WT_IKEY **ikeyp,
  WT_CELL **cellp, void *datap, size_t *sizep, uint8_t *prefixp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_instantiate --
 *     Instantiate the keys on a leaf page as needed.
 */
static WT_INLINE int __wt_row_leaf_key_instantiate(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_set --
 *     Set a WT_ROW to reference an on-page row-store leaf key.
 */
static WT_INLINE void __wt_row_leaf_key_set(WT_PAGE *page, WT_ROW *rip, WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_value --
 *     Return the value for a row-store leaf page encoded key/value pair.
 */
static WT_INLINE bool __wt_row_leaf_value(WT_PAGE *page, WT_ROW *rip, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_value_cell --
 *     Return the unpacked value for a row-store leaf page key.
 */
static WT_INLINE void __wt_row_leaf_value_cell(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip,
  WT_CELL_UNPACK_KV *vpack) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_value_is_encoded --
 *     Return if the value for a row-store leaf page is an encoded key/value pair.
 */
static WT_INLINE bool __wt_row_leaf_value_is_encoded(WT_ROW *rip)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_value_set --
 *     Set a WT_ROW to reference an on-page row-store leaf key and value pair, if possible.
 */
static WT_INLINE void __wt_row_leaf_value_set(WT_ROW *rip, WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_safe_sub --
 *     Subtract unsigned integers, rounding to zero if the result would be negative.
 */
static WT_INLINE uint64_t __wt_safe_sub(uint64_t v1, uint64_t v2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_scr_free --
 *     Release a scratch buffer.
 */
static WT_INLINE void __wt_scr_free(WT_SESSION_IMPL *session, WT_ITEM **bufp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_seconds --
 *     Return the seconds since the Epoch.
 */
static WT_INLINE void __wt_seconds(WT_SESSION_IMPL *session, uint64_t *secondsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_seconds32 --
 *     Return the seconds since the Epoch in 32 bits.
 */
static WT_INLINE void __wt_seconds32(WT_SESSION_IMPL *session, uint32_t *secondsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_can_wait --
 *     Return if a session available for a potentially slow operation.
 */
static WT_INLINE bool __wt_session_can_wait(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_set_shared_double --
 *     This function enables suppressing TSan warnings about setting doubles in a shared context.
 */
static WT_INLINE void __wt_set_shared_double(double *to_set, double value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_skip_choose_depth --
 *     Randomly choose a depth for a skiplist insert.
 */
static WT_INLINE u_int __wt_skip_choose_depth(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_snprintf --
 *     snprintf convenience function, ignoring the returned size.
 */
static WT_INLINE int __wt_snprintf(char *buf, size_t size, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_snprintf_len_incr --
 *     snprintf convenience function, incrementing the returned size.
 */
static WT_INLINE int __wt_snprintf_len_incr(char *buf, size_t size, size_t *retsizep,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_snprintf_len_set --
 *     snprintf convenience function, setting the returned size.
 */
static WT_INLINE int __wt_snprintf_len_set(char *buf, size_t size, size_t *retsizep,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_backoff --
 *     Back off while spinning for a resource. This is used to avoid busy waiting loops that can
 *     consume enough CPU to block real work being done. The algorithm spins a few times, then
 *     yields for a while, then falls back to sleeping.
 */
static WT_INLINE void __wt_spin_backoff(uint64_t *yield_count, uint64_t *sleep_usecs)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_destroy --
 *     Destroy a spinlock.
 */
static WT_INLINE void __wt_spin_destroy(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_init --
 *     Initialize a spinlock.
 */
static WT_INLINE int __wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK *t, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_lock --
 *     Spin until the lock is acquired.
 */
static WT_INLINE void __wt_spin_lock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_lock_track --
 *     Spinlock acquisition, with tracking.
 */
static WT_INLINE void __wt_spin_lock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_locked --
 *     Check whether the spinlock is locked, irrespective of which session locked it.
 */
static WT_INLINE bool __wt_spin_locked(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_owned --
 *     Check whether the session owns the spinlock.
 */
static WT_INLINE bool __wt_spin_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_trylock --
 *     Try to lock a spinlock or fail immediately if it is busy.
 */
static WT_INLINE int __wt_spin_trylock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_trylock_track --
 *     Try to lock a spinlock or fail immediately if it is busy. Track if successful.
 */
static WT_INLINE int __wt_spin_trylock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_unlock --
 *     Release the spinlock.
 */
static WT_INLINE void __wt_spin_unlock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_spin_unlock_if_owned --
 *     Unlock the spinlock only if it is acquired by the specified session.
 */
static WT_INLINE void __wt_spin_unlock_if_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_descent_race --
 *     Return if we raced with an internal page split when descending the tree.
 */
static WT_INLINE bool __wt_split_descent_race(WT_SESSION_IMPL *session, WT_REF *ref,
  WT_PAGE_INDEX *saved_pindex) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strcat --
 *     A safe version of string concatenation, which checks the size of the destination buffer;
 *     return ERANGE on error.
 */
static WT_INLINE int __wt_strcat(char *dest, size_t size, const char *src)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strdup --
 *     ANSI strdup function.
 */
static WT_INLINE int __wt_strdup(WT_SESSION_IMPL *session, const char *str, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strnlen --
 *     Determine the length of a fixed-size string
 */
static WT_INLINE size_t __wt_strnlen(const char *s, size_t maxlen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_packv --
 *     Pack a byte string (va_list version).
 */
static WT_INLINE int __wt_struct_packv(WT_SESSION_IMPL *session, void *buffer, size_t size,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_size_adjust --
 *     Adjust the size field for a packed structure. Sometimes we want to include the size as a
 *     field in a packed structure. This is done by calling __wt_struct_size with the expected
 *     format and a size of zero. Then we want to pack the structure using the final size. This
 *     function adjusts the size appropriately (taking into account the size of the final size or
 *     the size field itself).
 */
static WT_INLINE void __wt_struct_size_adjust(WT_SESSION_IMPL *session, size_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_sizev --
 *     Calculate the size of a packed byte string (va_list version).
 */
static WT_INLINE int __wt_struct_sizev(WT_SESSION_IMPL *session, size_t *sizep, const char *fmt,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_unpackv --
 *     Unpack a byte string (va_list version).
 */
static WT_INLINE int __wt_struct_unpackv(WT_SESSION_IMPL *session, const void *buffer, size_t size,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_sync_and_rename --
 *     Flush and close a stream, then swap it into place.
 */
static WT_INLINE int __wt_sync_and_rename(WT_SESSION_IMPL *session, WT_FSTREAM **fstrp,
  const char *from, const char *to) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timer_evaluate_ms --
 *     Evaluate the difference between the current time and start time and output the difference in
 *     milliseconds.
 */
static WT_INLINE void __wt_timer_evaluate_ms(WT_SESSION_IMPL *session, WT_TIMER *start_time,
  uint64_t *time_diff_ms) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timer_start --
 *     Start the timer.
 */
static WT_INLINE void __wt_timer_start(WT_SESSION_IMPL *session, WT_TIMER *start_time)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timing_stress --
 *     Optionally add delay to stress code paths. Sleep for the specified amount of time if passed
 *     in the argument.
 */
static WT_INLINE void __wt_timing_stress(WT_SESSION_IMPL *session, uint32_t flag,
  struct timespec *tsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timing_stress_sleep_random --
 *     Sleep for a random time, with a bias towards shorter sleeps.
 */
static WT_INLINE void __wt_timing_stress_sleep_random(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tolower --
 *     Wrap the ctype function without sign extension.
 */
static WT_INLINE u_char __wt_tolower(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tree_modify_set --
 *     Mark the tree dirty.
 */
static WT_INLINE void __wt_tree_modify_set(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_activity_check --
 *     Check whether there are any running transactions.
 */
static WT_INLINE int __wt_txn_activity_check(WT_SESSION_IMPL *session, bool *txn_active)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_autocommit_check --
 *     If an auto-commit transaction is required, start one.
 */
static WT_INLINE int __wt_txn_autocommit_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_begin --
 *     Begin a transaction.
 */
static WT_INLINE int __wt_txn_begin(WT_SESSION_IMPL *session, WT_CONF *conf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_context_check --
 *     Complain if a transaction is/isn't running.
 */
static WT_INLINE int __wt_txn_context_check(WT_SESSION_IMPL *session, bool requires_txn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_context_prepare_check --
 *     Return an error if the current transaction is in the prepare state.
 */
static WT_INLINE int __wt_txn_context_prepare_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_cursor_op --
 *     Called for each cursor operation.
 */
static WT_INLINE void __wt_txn_cursor_op(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_err_set --
 *     Set an error in the current transaction.
 */
static WT_INLINE void __wt_txn_err_set(WT_SESSION_IMPL *session, int ret)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_has_newest_and_visible_all --
 *     Check whether a given time window is either globally visible or obsolete. Note that both the
 *     id and the timestamp have to be greater than 0 to be considered.
 */
static WT_INLINE bool __wt_txn_has_newest_and_visible_all(WT_SESSION_IMPL *session, uint64_t id,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_id_alloc --
 *     Allocate a new transaction ID.
 */
static WT_INLINE uint64_t __wt_txn_id_alloc(WT_SESSION_IMPL *session, bool publish)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_id_check --
 *     A transaction is going to do an update, allocate a transaction ID.
 */
static WT_INLINE int __wt_txn_id_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_idle_cache_check --
 *     If there is no transaction active in this thread and we haven't checked if the cache is full,
 *     do it now. If we have to block for eviction, this is the best time to do it.
 */
static WT_INLINE int __wt_txn_idle_cache_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_modify --
 *     Mark a WT_UPDATE object modified by the current transaction.
 */
static WT_INLINE int __wt_txn_modify(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_modify_check --
 *     Check if the current transaction can modify an item.
 */
static WT_INLINE int __wt_txn_modify_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd, wt_timestamp_t *prev_tsp, u_int modify_type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_modify_page_delete --
 *     Remember a page truncated by the current transaction.
 */
static WT_INLINE int __wt_txn_modify_page_delete(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_oldest_id --
 *     Return the oldest transaction ID that has to be kept for the current tree.
 */
static WT_INLINE uint64_t __wt_txn_oldest_id(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_delete_apply_prepare_state --
 *     Apply the correct prepare state and the timestamp to the ref and to any updates in the page
 *     del update list.
 */
static WT_INLINE void __wt_txn_op_delete_apply_prepare_state(WT_SESSION_IMPL *session, WT_REF *ref,
  bool commit) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_delete_commit_apply_timestamps --
 *     Apply the correct start and durable timestamps to any updates in the page del update list.
 */
static WT_INLINE void __wt_txn_op_delete_commit_apply_timestamps(
  WT_SESSION_IMPL *session, WT_REF *ref) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_set_key --
 *     Copy the given key onto the most recent transaction operation. This function early exits if
 *     the transaction cannot prepare.
 */
static WT_INLINE int __wt_txn_op_set_key(WT_SESSION_IMPL *session, const WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_set_recno --
 *     Set the latest transaction operation with the given recno.
 */
static WT_INLINE void __wt_txn_op_set_recno(WT_SESSION_IMPL *session, uint64_t recno)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_op_set_timestamp --
 *     Decide whether to copy a commit timestamp into an update. If the op structure doesn't have a
 *     populated update or ref field or is in prepared state there won't be any check for an
 *     existing timestamp.
 */
static WT_INLINE void __wt_txn_op_set_timestamp(WT_SESSION_IMPL *session, WT_TXN_OP *op)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_pinned_timestamp --
 *     Get the first timestamp that has to be kept for the current tree.
 */
static WT_INLINE void __wt_txn_pinned_timestamp(WT_SESSION_IMPL *session,
  wt_timestamp_t *pinned_tsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_read --
 *     Get the first visible update in a chain. This function will first check the update list
 *     supplied as a function argument. If there is no visible update, it will check the onpage
 *     value for the given key. Finally, if the onpage value is not visible to the reader, the
 *     function will search the history store for a visible update.
 */
static WT_INLINE int __wt_txn_read(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *key,
  uint64_t recno, WT_UPDATE *upd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_read_last --
 *     Called when the last page for a session is released.
 */
static WT_INLINE void __wt_txn_read_last(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_read_upd_list --
 *     Get the first visible update in a list (or NULL if none are visible).
 */
static WT_INLINE int __wt_txn_read_upd_list(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_read_upd_list_internal --
 *     Internal helper function to get the first visible update in a list (or NULL if none are
 *     visible).
 */
static WT_INLINE int __wt_txn_read_upd_list_internal(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd, WT_UPDATE **prepare_updp, WT_UPDATE **restored_updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_search_check --
 *     Check if a search by the current transaction violates timestamp rules.
 */
static WT_INLINE int __wt_txn_search_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_snap_min_visible --
 *     Can the current transaction snapshot minimum/read timestamp see the given ID/timestamp? This
 *     visibility check should only be used when assessing broader visibility based on aggregated
 *     time window. It does not reflect whether a specific update is visible to a transaction.
 */
static WT_INLINE bool __wt_txn_snap_min_visible(
  WT_SESSION_IMPL *session, uint64_t id, wt_timestamp_t timestamp, wt_timestamp_t durable_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_timestamp_visible --
 *     Can the current transaction see the given timestamp?
 */
static WT_INLINE bool __wt_txn_timestamp_visible(WT_SESSION_IMPL *session, wt_timestamp_t timestamp,
  wt_timestamp_t durable_timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_timestamp_visible_all --
 *     Check whether a given timestamp is either globally visible or obsolete.
 */
static WT_INLINE bool __wt_txn_timestamp_visible_all(WT_SESSION_IMPL *session,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_tw_start_visible --
 *     Is the given start time window visible?
 */
static WT_INLINE bool __wt_txn_tw_start_visible(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_tw_start_visible_all --
 *     Is the given start time window visible to all (possible) readers?
 */
static WT_INLINE bool __wt_txn_tw_start_visible_all(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_tw_stop_visible --
 *     Is the given stop time window visible?
 */
static WT_INLINE bool __wt_txn_tw_stop_visible(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_tw_stop_visible_all --
 *     Is the given stop time window visible to all (possible) readers?
 */
static WT_INLINE bool __wt_txn_tw_stop_visible_all(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_unmodify --
 *     If threads race making updates, they may discard the last referenced WT_UPDATE item while the
 *     transaction is still active. This function removes the last update item from the "log".
 */
static WT_INLINE void __wt_txn_unmodify(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_upd_value_visible_all --
 *     Is the given update value visible to all (possible) readers?
 */
static WT_INLINE bool __wt_txn_upd_value_visible_all(WT_SESSION_IMPL *session,
  WT_UPDATE_VALUE *upd_value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_upd_visible --
 *     Can the current transaction see the given update.
 */
static WT_INLINE bool __wt_txn_upd_visible(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_upd_visible_all --
 *     Is the given update visible to all (possible) readers?
 */
static WT_INLINE bool __wt_txn_upd_visible_all(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_upd_visible_type --
 *     Visible type of given update for the current transaction.
 */
static WT_INLINE WT_VISIBLE_TYPE __wt_txn_upd_visible_type(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_visible --
 *     Can the current transaction see the given ID/timestamp?
 */
static WT_INLINE bool __wt_txn_visible(
  WT_SESSION_IMPL *session, uint64_t id, wt_timestamp_t timestamp, wt_timestamp_t durable_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_visible_all --
 *     Check whether a given time window is either globally visible or obsolete. For global
 *     visibility checks, the commit times are checked against the oldest possible readers in the
 *     system. If all possible readers could always see the time window - it is globally visible.
 *     For obsolete checks callers should generally pass in the durable timestamp, since it is
 *     guaranteed to be newer than or equal to the commit time, and content needs to be retained
 *     (not become obsolete) until both the commit and durable times are obsolete. If the commit
 *     time is used for this check, it's possible that a transaction is committed with a durable
 *     time and made obsolete before it can be included in a checkpoint - which leads to bugs in
 *     checkpoint correctness.
 */
static WT_INLINE bool __wt_txn_visible_all(WT_SESSION_IMPL *session, uint64_t id,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_txn_visible_id_snapshot --
 *     Is the id visible in terms of the given snapshot?
 */
static WT_INLINE bool __wt_txn_visible_id_snapshot(
  uint64_t id, uint64_t snap_min, uint64_t snap_max, uint64_t *snapshot, uint32_t snapshot_count)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_upd_alloc --
 *     Allocate a WT_UPDATE structure and associated value and fill it in.
 */
static WT_INLINE int __wt_upd_alloc(WT_SESSION_IMPL *session, const WT_ITEM *value,
  u_int modify_type, WT_UPDATE **updp, size_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_upd_alloc_tombstone --
 *     Allocate a tombstone update.
 */
static WT_INLINE int __wt_upd_alloc_tombstone(WT_SESSION_IMPL *session, WT_UPDATE **updp,
  size_t *sizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_upd_value_assign --
 *     Point an update value at a given update. We're specifically not getting the value to own the
 *     memory since this exists in an update list somewhere.
 */
static WT_INLINE void __wt_upd_value_assign(WT_UPDATE_VALUE *upd_value, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_upd_value_clear --
 *     Clear an update value to its defaults.
 */
static WT_INLINE void __wt_upd_value_clear(WT_UPDATE_VALUE *upd_value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_list_memsize --
 *     The size in memory of a list of updates.
 */
static WT_INLINE size_t __wt_update_list_memsize(WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_serial --
 *     Update a row or column-store entry.
 */
static WT_INLINE int __wt_update_serial(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_PAGE *page, WT_UPDATE **srch_upd, WT_UPDATE **updp, size_t upd_size, bool exclusive)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_type_str --
 *     Convert an update type to its string representation.
 */
static WT_INLINE const char *__wt_update_type_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_usec_to_timespec --
 *     Initialize to represent specified number of microseconds.
 */
static WT_INLINE void __wt_usec_to_timespec(time_t usec, struct timespec *tsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vfprintf --
 *     ANSI C vfprintf.
 */
static WT_INLINE int __wt_vfprintf(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, const char *fmt,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vpack_int --
 *     Variable-sized packing for signed integers
 */
static WT_INLINE int __wt_vpack_int(uint8_t **pp, size_t maxlen, int64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vpack_negint --
 *     Packs a negative variable-length integer in the specified location.
 */
static WT_INLINE int __wt_vpack_negint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vpack_posint --
 *     Packs a positive variable-length integer in the specified location.
 */
static WT_INLINE int __wt_vpack_posint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vpack_uint --
 *     Variable-sized packing for unsigned integers
 */
static WT_INLINE int __wt_vpack_uint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsize_int --
 *     Return the packed size of a signed integer.
 */
static WT_INLINE size_t __wt_vsize_int(int64_t x) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsize_negint --
 *     Return the packed size of a negative variable-length integer.
 */
static WT_INLINE size_t __wt_vsize_negint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsize_posint --
 *     Return the packed size of a positive variable-length integer.
 */
static WT_INLINE size_t __wt_vsize_posint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsize_uint --
 *     Return the packed size of an unsigned integer.
 */
static WT_INLINE size_t __wt_vsize_uint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsnprintf --
 *     vsnprintf convenience function, ignoring the returned size.
 */
static WT_INLINE int __wt_vsnprintf(char *buf, size_t size, const char *fmt, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsnprintf_len_set --
 *     vsnprintf convenience function, setting the returned size.
 */
static WT_INLINE int __wt_vsnprintf_len_set(char *buf, size_t size, size_t *retsizep,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vunpack_int --
 *     Variable-sized packing for signed integers
 */
static WT_INLINE int __wt_vunpack_int(const uint8_t **pp, size_t maxlen, int64_t *xp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vunpack_negint --
 *     Reads a variable-length negative integer from the specified location.
 */
static WT_INLINE int __wt_vunpack_negint(const uint8_t **pp, size_t maxlen, uint64_t *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vunpack_posint --
 *     Reads a variable-length positive integer from the specified location.
 */
static WT_INLINE int __wt_vunpack_posint(const uint8_t **pp, size_t maxlen, uint64_t *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vunpack_uint --
 *     Variable-sized unpacking for unsigned integers
 */
static WT_INLINE int __wt_vunpack_uint(const uint8_t **pp, size_t maxlen, uint64_t *xp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_write --
 *     POSIX pwrite.
 */
static WT_INLINE int __wt_write(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t len,
  const void *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
