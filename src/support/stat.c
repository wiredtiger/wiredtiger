/* DO NOT EDIT: automatically built by dist/stat.py. */

#include "wt_internal.h"

void
__wt_stat_init_dsrc_stats(WT_DSRC_STATS *stats)
{
	/* Clear, so can also be called for reinitialization. */
	memset(stats, 0, sizeof(*stats));

	stats->block_extension.desc =
	    "block-manager: allocations requiring file extension";
	stats->block_alloc.desc = "block-manager: blocks allocated";
	stats->block_free.desc = "block-manager: blocks freed";
	stats->block_checkpoint_size.desc = "block-manager: checkpoint size";
	stats->allocation_size.desc =
	    "block-manager: file allocation unit size";
	stats->block_reuse_bytes.desc =
	    "block-manager: file bytes available for reuse";
	stats->block_magic.desc = "block-manager: file magic number";
	stats->block_major.desc = "block-manager: file major version number";
	stats->block_size.desc = "block-manager: file size in bytes";
	stats->block_minor.desc = "block-manager: minor version number";
	stats->btree_checkpoint_generation.desc =
	    "btree: btree checkpoint generation";
	stats->btree_column_fix.desc =
	    "btree: column-store fixed-size leaf pages";
	stats->btree_column_internal.desc =
	    "btree: column-store internal pages";
	stats->btree_column_deleted.desc =
	    "btree: column-store variable-size deleted values";
	stats->btree_column_variable.desc =
	    "btree: column-store variable-size leaf pages";
	stats->btree_fixed_len.desc = "btree: fixed-record size";
	stats->btree_maxintlkey.desc = "btree: maximum internal page key size";
	stats->btree_maxintlpage.desc = "btree: maximum internal page size";
	stats->btree_maxleafkey.desc = "btree: maximum leaf page key size";
	stats->btree_maxleafpage.desc = "btree: maximum leaf page size";
	stats->btree_maxleafvalue.desc = "btree: maximum leaf page value size";
	stats->btree_maximum_depth.desc = "btree: maximum tree depth";
	stats->btree_entries.desc = "btree: number of key/value pairs";
	stats->btree_overflow.desc = "btree: overflow pages";
	stats->btree_compact_rewrite.desc =
	    "btree: pages rewritten by compaction";
	stats->btree_row_internal.desc = "btree: row-store internal pages";
	stats->btree_row_leaf.desc = "btree: row-store leaf pages";
	stats->cache_bytes_read.desc = "cache: bytes read into cache";
	stats->cache_bytes_write.desc = "cache: bytes written from cache";
	stats->cache_eviction_checkpoint.desc =
	    "cache: checkpoint blocked page eviction";
	stats->cache_eviction_fail.desc =
	    "cache: data source pages selected for eviction unable to be evicted";
	stats->cache_eviction_hazard.desc =
	    "cache: hazard pointer blocked page eviction";
	stats->cache_inmem_split.desc = "cache: in-memory page splits";
	stats->cache_eviction_internal.desc = "cache: internal pages evicted";
	stats->cache_eviction_dirty.desc = "cache: modified pages evicted";
	stats->cache_read_overflow.desc =
	    "cache: overflow pages read into cache";
	stats->cache_overflow_value.desc =
	    "cache: overflow values cached in memory";
	stats->cache_eviction_deepen.desc =
	    "cache: page split during eviction deepened the tree";
	stats->cache_read.desc = "cache: pages read into cache";
	stats->cache_eviction_split.desc =
	    "cache: pages split during eviction";
	stats->cache_write.desc = "cache: pages written from cache";
	stats->cache_eviction_clean.desc = "cache: unmodified pages evicted";
	stats->compress_read.desc = "compression: compressed pages read";
	stats->compress_write.desc = "compression: compressed pages written";
	stats->compress_write_fail.desc =
	    "compression: page written failed to compress";
	stats->compress_write_too_small.desc =
	    "compression: page written was too small to compress";
	stats->compress_raw_fail_temporary.desc =
	    "compression: raw compression call failed, additional data available";
	stats->compress_raw_fail.desc =
	    "compression: raw compression call failed, no additional data available";
	stats->compress_raw_ok.desc =
	    "compression: raw compression call succeeded";
	stats->cursor_insert_bulk.desc =
	    "cursor: bulk-loaded cursor-insert calls";
	stats->cursor_create.desc = "cursor: create calls";
	stats->cursor_insert_bytes.desc =
	    "cursor: cursor-insert key and value bytes inserted";
	stats->cursor_remove_bytes.desc =
	    "cursor: cursor-remove key bytes removed";
	stats->cursor_update_bytes.desc =
	    "cursor: cursor-update value bytes updated";
	stats->cursor_insert.desc = "cursor: insert calls";
	stats->cursor_next.desc = "cursor: next calls";
	stats->cursor_prev.desc = "cursor: prev calls";
	stats->cursor_remove.desc = "cursor: remove calls";
	stats->cursor_reset.desc = "cursor: reset calls";
	stats->cursor_search.desc = "cursor: search calls";
	stats->cursor_search_near.desc = "cursor: search near calls";
	stats->cursor_update.desc = "cursor: update calls";
	stats->bloom_false_positive.desc = "LSM: bloom filter false positives";
	stats->bloom_hit.desc = "LSM: bloom filter hits";
	stats->bloom_miss.desc = "LSM: bloom filter misses";
	stats->bloom_page_evict.desc =
	    "LSM: bloom filter pages evicted from cache";
	stats->bloom_page_read.desc =
	    "LSM: bloom filter pages read into cache";
	stats->bloom_count.desc = "LSM: bloom filters in the LSM tree";
	stats->lsm_chunk_count.desc = "LSM: chunks in the LSM tree";
	stats->lsm_generation_max.desc =
	    "LSM: highest merge generation in the LSM tree";
	stats->lsm_lookup_no_bloom.desc =
	    "LSM: queries that could have benefited from a Bloom filter that did not exist";
	stats->lsm_checkpoint_throttle.desc =
	    "LSM: sleep for LSM checkpoint throttle";
	stats->lsm_merge_throttle.desc = "LSM: sleep for LSM merge throttle";
	stats->bloom_size.desc = "LSM: total size of bloom filters";
	stats->rec_dictionary.desc = "reconciliation: dictionary matches";
	stats->rec_suffix_compression.desc =
	    "reconciliation: internal page key bytes discarded using suffix compression";
	stats->rec_multiblock_internal.desc =
	    "reconciliation: internal page multi-block writes";
	stats->rec_overflow_key_internal.desc =
	    "reconciliation: internal-page overflow keys";
	stats->rec_prefix_compression.desc =
	    "reconciliation: leaf page key bytes discarded using prefix compression";
	stats->rec_multiblock_leaf.desc =
	    "reconciliation: leaf page multi-block writes";
	stats->rec_overflow_key_leaf.desc =
	    "reconciliation: leaf-page overflow keys";
	stats->rec_multiblock_max.desc =
	    "reconciliation: maximum blocks required for a page";
	stats->rec_overflow_value.desc =
	    "reconciliation: overflow values written";
	stats->rec_page_match.desc = "reconciliation: page checksum matches";
	stats->rec_pages.desc = "reconciliation: page reconciliation calls";
	stats->rec_pages_eviction.desc =
	    "reconciliation: page reconciliation calls for eviction";
	stats->rec_page_delete.desc = "reconciliation: pages deleted";
	stats->session_compact.desc = "session: object compaction";
	stats->session_cursor_open.desc = "session: open cursor count";
	stats->txn_update_conflict.desc = "transaction: update conflicts";
}

void
__wt_stat_refresh_dsrc_stats(void *stats_arg)
{
	WT_DSRC_STATS *stats;

	stats = (WT_DSRC_STATS *)stats_arg;
	WT_STATS_CLEAR(stats, block_extension);
	WT_STATS_CLEAR(stats, block_alloc);
	WT_STATS_CLEAR(stats, block_free);
	WT_STATS_CLEAR(stats, block_checkpoint_size);
	WT_STATS_CLEAR(stats, allocation_size);
	WT_STATS_CLEAR(stats, block_reuse_bytes);
	WT_STATS_CLEAR(stats, block_magic);
	WT_STATS_CLEAR(stats, block_major);
	WT_STATS_CLEAR(stats, block_size);
	WT_STATS_CLEAR(stats, block_minor);
	WT_STATS_CLEAR(stats, btree_column_fix);
	WT_STATS_CLEAR(stats, btree_column_internal);
	WT_STATS_CLEAR(stats, btree_column_deleted);
	WT_STATS_CLEAR(stats, btree_column_variable);
	WT_STATS_CLEAR(stats, btree_fixed_len);
	WT_STATS_CLEAR(stats, btree_maxintlkey);
	WT_STATS_CLEAR(stats, btree_maxintlpage);
	WT_STATS_CLEAR(stats, btree_maxleafkey);
	WT_STATS_CLEAR(stats, btree_maxleafpage);
	WT_STATS_CLEAR(stats, btree_maxleafvalue);
	WT_STATS_CLEAR(stats, btree_maximum_depth);
	WT_STATS_CLEAR(stats, btree_entries);
	WT_STATS_CLEAR(stats, btree_overflow);
	WT_STATS_CLEAR(stats, btree_compact_rewrite);
	WT_STATS_CLEAR(stats, btree_row_internal);
	WT_STATS_CLEAR(stats, btree_row_leaf);
	WT_STATS_CLEAR(stats, cache_bytes_read);
	WT_STATS_CLEAR(stats, cache_bytes_write);
	WT_STATS_CLEAR(stats, cache_eviction_checkpoint);
	WT_STATS_CLEAR(stats, cache_eviction_fail);
	WT_STATS_CLEAR(stats, cache_eviction_hazard);
	WT_STATS_CLEAR(stats, cache_inmem_split);
	WT_STATS_CLEAR(stats, cache_eviction_internal);
	WT_STATS_CLEAR(stats, cache_eviction_dirty);
	WT_STATS_CLEAR(stats, cache_read_overflow);
	WT_STATS_CLEAR(stats, cache_overflow_value);
	WT_STATS_CLEAR(stats, cache_eviction_deepen);
	WT_STATS_CLEAR(stats, cache_read);
	WT_STATS_CLEAR(stats, cache_eviction_split);
	WT_STATS_CLEAR(stats, cache_write);
	WT_STATS_CLEAR(stats, cache_eviction_clean);
	WT_STATS_CLEAR(stats, compress_read);
	WT_STATS_CLEAR(stats, compress_write);
	WT_STATS_CLEAR(stats, compress_write_fail);
	WT_STATS_CLEAR(stats, compress_write_too_small);
	WT_STATS_CLEAR(stats, compress_raw_fail_temporary);
	WT_STATS_CLEAR(stats, compress_raw_fail);
	WT_STATS_CLEAR(stats, compress_raw_ok);
	WT_STATS_CLEAR(stats, cursor_insert_bulk);
	WT_STATS_CLEAR(stats, cursor_create);
	WT_STATS_CLEAR(stats, cursor_insert_bytes);
	WT_STATS_CLEAR(stats, cursor_remove_bytes);
	WT_STATS_CLEAR(stats, cursor_update_bytes);
	WT_STATS_CLEAR(stats, cursor_insert);
	WT_STATS_CLEAR(stats, cursor_next);
	WT_STATS_CLEAR(stats, cursor_prev);
	WT_STATS_CLEAR(stats, cursor_remove);
	WT_STATS_CLEAR(stats, cursor_reset);
	WT_STATS_CLEAR(stats, cursor_search);
	WT_STATS_CLEAR(stats, cursor_search_near);
	WT_STATS_CLEAR(stats, cursor_update);
	WT_STATS_CLEAR(stats, bloom_false_positive);
	WT_STATS_CLEAR(stats, bloom_hit);
	WT_STATS_CLEAR(stats, bloom_miss);
	WT_STATS_CLEAR(stats, bloom_page_evict);
	WT_STATS_CLEAR(stats, bloom_page_read);
	WT_STATS_CLEAR(stats, bloom_count);
	WT_STATS_CLEAR(stats, lsm_chunk_count);
	WT_STATS_CLEAR(stats, lsm_generation_max);
	WT_STATS_CLEAR(stats, lsm_lookup_no_bloom);
	WT_STATS_CLEAR(stats, lsm_checkpoint_throttle);
	WT_STATS_CLEAR(stats, lsm_merge_throttle);
	WT_STATS_CLEAR(stats, bloom_size);
	WT_STATS_CLEAR(stats, rec_dictionary);
	WT_STATS_CLEAR(stats, rec_suffix_compression);
	WT_STATS_CLEAR(stats, rec_multiblock_internal);
	WT_STATS_CLEAR(stats, rec_overflow_key_internal);
	WT_STATS_CLEAR(stats, rec_prefix_compression);
	WT_STATS_CLEAR(stats, rec_multiblock_leaf);
	WT_STATS_CLEAR(stats, rec_overflow_key_leaf);
	WT_STATS_CLEAR(stats, rec_multiblock_max);
	WT_STATS_CLEAR(stats, rec_overflow_value);
	WT_STATS_CLEAR(stats, rec_page_match);
	WT_STATS_CLEAR(stats, rec_pages);
	WT_STATS_CLEAR(stats, rec_pages_eviction);
	WT_STATS_CLEAR(stats, rec_page_delete);
	WT_STATS_CLEAR(stats, session_compact);
	WT_STATS_CLEAR(stats, txn_update_conflict);
}

void
__wt_stat_aggregate_dsrc_stats(const void *child, const void *parent)
{
	WT_DSRC_STATS *c, *p;
	uint64_t v;

	c = (WT_DSRC_STATS *)child;
	p = (WT_DSRC_STATS *)parent;
	WT_STAT_WRITE_SIMPLE(p, block_extension) +=
	    (int64_t)WT_STAT_READ(c, block_extension);
	WT_STAT_WRITE_SIMPLE(p, block_alloc) +=
	    (int64_t)WT_STAT_READ(c, block_alloc);
	WT_STAT_WRITE_SIMPLE(p, block_free) +=
	    (int64_t)WT_STAT_READ(c, block_free);
	WT_STAT_WRITE_SIMPLE(p, block_checkpoint_size) +=
	    (int64_t)WT_STAT_READ(c, block_checkpoint_size);
	WT_STAT_WRITE_SIMPLE(p, block_reuse_bytes) +=
	    (int64_t)WT_STAT_READ(c, block_reuse_bytes);
	WT_STAT_WRITE_SIMPLE(p, block_size) +=
	    (int64_t)WT_STAT_READ(c, block_size);
	WT_STAT_WRITE_SIMPLE(p, btree_checkpoint_generation) +=
	    (int64_t)WT_STAT_READ(c, btree_checkpoint_generation);
	WT_STAT_WRITE_SIMPLE(p, btree_column_fix) +=
	    (int64_t)WT_STAT_READ(c, btree_column_fix);
	WT_STAT_WRITE_SIMPLE(p, btree_column_internal) +=
	    (int64_t)WT_STAT_READ(c, btree_column_internal);
	WT_STAT_WRITE_SIMPLE(p, btree_column_deleted) +=
	    (int64_t)WT_STAT_READ(c, btree_column_deleted);
	WT_STAT_WRITE_SIMPLE(p, btree_column_variable) +=
	    (int64_t)WT_STAT_READ(c, btree_column_variable);
	if ((v = WT_STAT_READ(c, btree_maxintlkey)) >
	    WT_STAT_READ(p, btree_maxintlkey)) {
		WT_STATS_CLEAR(p, btree_maxintlkey);
		WT_STAT_WRITE_SIMPLE(p, btree_maxintlkey) = (int64_t)v;
	}
	if ((v = WT_STAT_READ(c, btree_maxintlpage)) >
	    WT_STAT_READ(p, btree_maxintlpage)) {
		WT_STATS_CLEAR(p, btree_maxintlpage);
		WT_STAT_WRITE_SIMPLE(p, btree_maxintlpage) = (int64_t)v;
	}
	if ((v = WT_STAT_READ(c, btree_maxleafkey)) >
	    WT_STAT_READ(p, btree_maxleafkey)) {
		WT_STATS_CLEAR(p, btree_maxleafkey);
		WT_STAT_WRITE_SIMPLE(p, btree_maxleafkey) = (int64_t)v;
	}
	if ((v = WT_STAT_READ(c, btree_maxleafpage)) >
	    WT_STAT_READ(p, btree_maxleafpage)) {
		WT_STATS_CLEAR(p, btree_maxleafpage);
		WT_STAT_WRITE_SIMPLE(p, btree_maxleafpage) = (int64_t)v;
	}
	if ((v = WT_STAT_READ(c, btree_maxleafvalue)) >
	    WT_STAT_READ(p, btree_maxleafvalue)) {
		WT_STATS_CLEAR(p, btree_maxleafvalue);
		WT_STAT_WRITE_SIMPLE(p, btree_maxleafvalue) = (int64_t)v;
	}
	if ((v = WT_STAT_READ(c, btree_maximum_depth)) >
	    WT_STAT_READ(p, btree_maximum_depth)) {
		WT_STATS_CLEAR(p, btree_maximum_depth);
		WT_STAT_WRITE_SIMPLE(p, btree_maximum_depth) = (int64_t)v;
	}
	WT_STAT_WRITE_SIMPLE(p, btree_entries) +=
	    (int64_t)WT_STAT_READ(c, btree_entries);
	WT_STAT_WRITE_SIMPLE(p, btree_overflow) +=
	    (int64_t)WT_STAT_READ(c, btree_overflow);
	WT_STAT_WRITE_SIMPLE(p, btree_compact_rewrite) +=
	    (int64_t)WT_STAT_READ(c, btree_compact_rewrite);
	WT_STAT_WRITE_SIMPLE(p, btree_row_internal) +=
	    (int64_t)WT_STAT_READ(c, btree_row_internal);
	WT_STAT_WRITE_SIMPLE(p, btree_row_leaf) +=
	    (int64_t)WT_STAT_READ(c, btree_row_leaf);
	WT_STAT_WRITE_SIMPLE(p, cache_bytes_read) +=
	    (int64_t)WT_STAT_READ(c, cache_bytes_read);
	WT_STAT_WRITE_SIMPLE(p, cache_bytes_write) +=
	    (int64_t)WT_STAT_READ(c, cache_bytes_write);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_checkpoint) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_checkpoint);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_fail) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_fail);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_hazard) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_hazard);
	WT_STAT_WRITE_SIMPLE(p, cache_inmem_split) +=
	    (int64_t)WT_STAT_READ(c, cache_inmem_split);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_internal) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_internal);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_dirty) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_dirty);
	WT_STAT_WRITE_SIMPLE(p, cache_read_overflow) +=
	    (int64_t)WT_STAT_READ(c, cache_read_overflow);
	WT_STAT_WRITE_SIMPLE(p, cache_overflow_value) +=
	    (int64_t)WT_STAT_READ(c, cache_overflow_value);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_deepen) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_deepen);
	WT_STAT_WRITE_SIMPLE(p, cache_read) +=
	    (int64_t)WT_STAT_READ(c, cache_read);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_split) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_split);
	WT_STAT_WRITE_SIMPLE(p, cache_write) +=
	    (int64_t)WT_STAT_READ(c, cache_write);
	WT_STAT_WRITE_SIMPLE(p, cache_eviction_clean) +=
	    (int64_t)WT_STAT_READ(c, cache_eviction_clean);
	WT_STAT_WRITE_SIMPLE(p, compress_read) +=
	    (int64_t)WT_STAT_READ(c, compress_read);
	WT_STAT_WRITE_SIMPLE(p, compress_write) +=
	    (int64_t)WT_STAT_READ(c, compress_write);
	WT_STAT_WRITE_SIMPLE(p, compress_write_fail) +=
	    (int64_t)WT_STAT_READ(c, compress_write_fail);
	WT_STAT_WRITE_SIMPLE(p, compress_write_too_small) +=
	    (int64_t)WT_STAT_READ(c, compress_write_too_small);
	WT_STAT_WRITE_SIMPLE(p, compress_raw_fail_temporary) +=
	    (int64_t)WT_STAT_READ(c, compress_raw_fail_temporary);
	WT_STAT_WRITE_SIMPLE(p, compress_raw_fail) +=
	    (int64_t)WT_STAT_READ(c, compress_raw_fail);
	WT_STAT_WRITE_SIMPLE(p, compress_raw_ok) +=
	    (int64_t)WT_STAT_READ(c, compress_raw_ok);
	WT_STAT_WRITE_SIMPLE(p, cursor_insert_bulk) +=
	    (int64_t)WT_STAT_READ(c, cursor_insert_bulk);
	WT_STAT_WRITE_SIMPLE(p, cursor_create) +=
	    (int64_t)WT_STAT_READ(c, cursor_create);
	WT_STAT_WRITE_SIMPLE(p, cursor_insert_bytes) +=
	    (int64_t)WT_STAT_READ(c, cursor_insert_bytes);
	WT_STAT_WRITE_SIMPLE(p, cursor_remove_bytes) +=
	    (int64_t)WT_STAT_READ(c, cursor_remove_bytes);
	WT_STAT_WRITE_SIMPLE(p, cursor_update_bytes) +=
	    (int64_t)WT_STAT_READ(c, cursor_update_bytes);
	WT_STAT_WRITE_SIMPLE(p, cursor_insert) +=
	    (int64_t)WT_STAT_READ(c, cursor_insert);
	WT_STAT_WRITE_SIMPLE(p, cursor_next) +=
	    (int64_t)WT_STAT_READ(c, cursor_next);
	WT_STAT_WRITE_SIMPLE(p, cursor_prev) +=
	    (int64_t)WT_STAT_READ(c, cursor_prev);
	WT_STAT_WRITE_SIMPLE(p, cursor_remove) +=
	    (int64_t)WT_STAT_READ(c, cursor_remove);
	WT_STAT_WRITE_SIMPLE(p, cursor_reset) +=
	    (int64_t)WT_STAT_READ(c, cursor_reset);
	WT_STAT_WRITE_SIMPLE(p, cursor_search) +=
	    (int64_t)WT_STAT_READ(c, cursor_search);
	WT_STAT_WRITE_SIMPLE(p, cursor_search_near) +=
	    (int64_t)WT_STAT_READ(c, cursor_search_near);
	WT_STAT_WRITE_SIMPLE(p, cursor_update) +=
	    (int64_t)WT_STAT_READ(c, cursor_update);
	WT_STAT_WRITE_SIMPLE(p, bloom_false_positive) +=
	    (int64_t)WT_STAT_READ(c, bloom_false_positive);
	WT_STAT_WRITE_SIMPLE(p, bloom_hit) +=
	    (int64_t)WT_STAT_READ(c, bloom_hit);
	WT_STAT_WRITE_SIMPLE(p, bloom_miss) +=
	    (int64_t)WT_STAT_READ(c, bloom_miss);
	WT_STAT_WRITE_SIMPLE(p, bloom_page_evict) +=
	    (int64_t)WT_STAT_READ(c, bloom_page_evict);
	WT_STAT_WRITE_SIMPLE(p, bloom_page_read) +=
	    (int64_t)WT_STAT_READ(c, bloom_page_read);
	WT_STAT_WRITE_SIMPLE(p, bloom_count) +=
	    (int64_t)WT_STAT_READ(c, bloom_count);
	WT_STAT_WRITE_SIMPLE(p, lsm_chunk_count) +=
	    (int64_t)WT_STAT_READ(c, lsm_chunk_count);
	if ((v = WT_STAT_READ(c, lsm_generation_max)) >
	    WT_STAT_READ(p, lsm_generation_max)) {
		WT_STATS_CLEAR(p, lsm_generation_max);
		WT_STAT_WRITE_SIMPLE(p, lsm_generation_max) = (int64_t)v;
	}
	WT_STAT_WRITE_SIMPLE(p, lsm_lookup_no_bloom) +=
	    (int64_t)WT_STAT_READ(c, lsm_lookup_no_bloom);
	WT_STAT_WRITE_SIMPLE(p, lsm_checkpoint_throttle) +=
	    (int64_t)WT_STAT_READ(c, lsm_checkpoint_throttle);
	WT_STAT_WRITE_SIMPLE(p, lsm_merge_throttle) +=
	    (int64_t)WT_STAT_READ(c, lsm_merge_throttle);
	WT_STAT_WRITE_SIMPLE(p, bloom_size) +=
	    (int64_t)WT_STAT_READ(c, bloom_size);
	WT_STAT_WRITE_SIMPLE(p, rec_dictionary) +=
	    (int64_t)WT_STAT_READ(c, rec_dictionary);
	WT_STAT_WRITE_SIMPLE(p, rec_suffix_compression) +=
	    (int64_t)WT_STAT_READ(c, rec_suffix_compression);
	WT_STAT_WRITE_SIMPLE(p, rec_multiblock_internal) +=
	    (int64_t)WT_STAT_READ(c, rec_multiblock_internal);
	WT_STAT_WRITE_SIMPLE(p, rec_overflow_key_internal) +=
	    (int64_t)WT_STAT_READ(c, rec_overflow_key_internal);
	WT_STAT_WRITE_SIMPLE(p, rec_prefix_compression) +=
	    (int64_t)WT_STAT_READ(c, rec_prefix_compression);
	WT_STAT_WRITE_SIMPLE(p, rec_multiblock_leaf) +=
	    (int64_t)WT_STAT_READ(c, rec_multiblock_leaf);
	WT_STAT_WRITE_SIMPLE(p, rec_overflow_key_leaf) +=
	    (int64_t)WT_STAT_READ(c, rec_overflow_key_leaf);
	if ((v = WT_STAT_READ(c, rec_multiblock_max)) >
	    WT_STAT_READ(p, rec_multiblock_max)) {
		WT_STATS_CLEAR(p, rec_multiblock_max);
		WT_STAT_WRITE_SIMPLE(p, rec_multiblock_max) = (int64_t)v;
	}
	WT_STAT_WRITE_SIMPLE(p, rec_overflow_value) +=
	    (int64_t)WT_STAT_READ(c, rec_overflow_value);
	WT_STAT_WRITE_SIMPLE(p, rec_page_match) +=
	    (int64_t)WT_STAT_READ(c, rec_page_match);
	WT_STAT_WRITE_SIMPLE(p, rec_pages) +=
	    (int64_t)WT_STAT_READ(c, rec_pages);
	WT_STAT_WRITE_SIMPLE(p, rec_pages_eviction) +=
	    (int64_t)WT_STAT_READ(c, rec_pages_eviction);
	WT_STAT_WRITE_SIMPLE(p, rec_page_delete) +=
	    (int64_t)WT_STAT_READ(c, rec_page_delete);
	WT_STAT_WRITE_SIMPLE(p, session_compact) +=
	    (int64_t)WT_STAT_READ(c, session_compact);
	WT_STAT_WRITE_SIMPLE(p, session_cursor_open) +=
	    (int64_t)WT_STAT_READ(c, session_cursor_open);
	WT_STAT_WRITE_SIMPLE(p, txn_update_conflict) +=
	    (int64_t)WT_STAT_READ(c, txn_update_conflict);
}

void
__wt_stat_init_connection_stats(WT_CONNECTION_STATS *stats)
{
	/* Clear, so can also be called for reinitialization. */
	memset(stats, 0, sizeof(*stats));

	stats->async_cur_queue.desc = "async: current work queue length";
	stats->async_max_queue.desc = "async: maximum work queue length";
	stats->async_alloc_race.desc =
	    "async: number of allocation state races";
	stats->async_flush.desc = "async: number of flush calls";
	stats->async_alloc_view.desc =
	    "async: number of operation slots viewed for allocation";
	stats->async_full.desc =
	    "async: number of times operation allocation failed";
	stats->async_nowork.desc =
	    "async: number of times worker found no work";
	stats->async_op_alloc.desc = "async: total allocations";
	stats->async_op_compact.desc = "async: total compact calls";
	stats->async_op_insert.desc = "async: total insert calls";
	stats->async_op_remove.desc = "async: total remove calls";
	stats->async_op_search.desc = "async: total search calls";
	stats->async_op_update.desc = "async: total update calls";
	stats->block_preload.desc = "block-manager: blocks pre-loaded";
	stats->block_read.desc = "block-manager: blocks read";
	stats->block_write.desc = "block-manager: blocks written";
	stats->block_byte_read.desc = "block-manager: bytes read";
	stats->block_byte_write.desc = "block-manager: bytes written";
	stats->block_map_read.desc = "block-manager: mapped blocks read";
	stats->block_byte_map_read.desc = "block-manager: mapped bytes read";
	stats->cache_bytes_inuse.desc = "cache: bytes currently in the cache";
	stats->cache_bytes_read.desc = "cache: bytes read into cache";
	stats->cache_bytes_write.desc = "cache: bytes written from cache";
	stats->cache_eviction_checkpoint.desc =
	    "cache: checkpoint blocked page eviction";
	stats->cache_eviction_queue_empty.desc =
	    "cache: eviction server candidate queue empty when topping up";
	stats->cache_eviction_queue_not_empty.desc =
	    "cache: eviction server candidate queue not empty when topping up";
	stats->cache_eviction_server_evicting.desc =
	    "cache: eviction server evicting pages";
	stats->cache_eviction_server_not_evicting.desc =
	    "cache: eviction server populating queue, but not evicting pages";
	stats->cache_eviction_slow.desc =
	    "cache: eviction server unable to reach eviction goal";
	stats->cache_eviction_worker_evicting.desc =
	    "cache: eviction worker thread evicting pages";
	stats->cache_eviction_force_fail.desc =
	    "cache: failed eviction of pages that exceeded the in-memory maximum";
	stats->cache_eviction_hazard.desc =
	    "cache: hazard pointer blocked page eviction";
	stats->cache_inmem_split.desc = "cache: in-memory page splits";
	stats->cache_eviction_internal.desc = "cache: internal pages evicted";
	stats->cache_bytes_max.desc = "cache: maximum bytes configured";
	stats->cache_eviction_maximum_page_size.desc =
	    "cache: maximum page size at eviction";
	stats->cache_eviction_dirty.desc = "cache: modified pages evicted";
	stats->cache_eviction_deepen.desc =
	    "cache: page split during eviction deepened the tree";
	stats->cache_pages_inuse.desc =
	    "cache: pages currently held in the cache";
	stats->cache_eviction_force.desc =
	    "cache: pages evicted because they exceeded the in-memory maximum";
	stats->cache_eviction_force_delete.desc =
	    "cache: pages evicted because they had chains of deleted items";
	stats->cache_eviction_app.desc =
	    "cache: pages evicted by application threads";
	stats->cache_read.desc = "cache: pages read into cache";
	stats->cache_eviction_fail.desc =
	    "cache: pages selected for eviction unable to be evicted";
	stats->cache_eviction_split.desc =
	    "cache: pages split during eviction";
	stats->cache_eviction_walk.desc = "cache: pages walked for eviction";
	stats->cache_write.desc = "cache: pages written from cache";
	stats->cache_overhead.desc = "cache: percentage overhead";
	stats->cache_bytes_internal.desc =
	    "cache: tracked bytes belonging to internal pages in the cache";
	stats->cache_bytes_leaf.desc =
	    "cache: tracked bytes belonging to leaf pages in the cache";
	stats->cache_bytes_overflow.desc =
	    "cache: tracked bytes belonging to overflow pages in the cache";
	stats->cache_bytes_dirty.desc =
	    "cache: tracked dirty bytes in the cache";
	stats->cache_pages_dirty.desc =
	    "cache: tracked dirty pages in the cache";
	stats->cache_eviction_clean.desc = "cache: unmodified pages evicted";
	stats->file_open.desc = "connection: files currently open";
	stats->memory_allocation.desc = "connection: memory allocations";
	stats->memory_free.desc = "connection: memory frees";
	stats->memory_grow.desc = "connection: memory re-allocations";
	stats->cond_wait.desc =
	    "connection: pthread mutex condition wait calls";
	stats->rwlock_read.desc =
	    "connection: pthread mutex shared lock read-lock calls";
	stats->rwlock_write.desc =
	    "connection: pthread mutex shared lock write-lock calls";
	stats->read_io.desc = "connection: total read I/Os";
	stats->write_io.desc = "connection: total write I/Os";
	stats->cursor_create.desc = "cursor: cursor create calls";
	stats->cursor_insert.desc = "cursor: cursor insert calls";
	stats->cursor_next.desc = "cursor: cursor next calls";
	stats->cursor_prev.desc = "cursor: cursor prev calls";
	stats->cursor_remove.desc = "cursor: cursor remove calls";
	stats->cursor_reset.desc = "cursor: cursor reset calls";
	stats->cursor_search.desc = "cursor: cursor search calls";
	stats->cursor_search_near.desc = "cursor: cursor search near calls";
	stats->cursor_update.desc = "cursor: cursor update calls";
	stats->dh_sweep_ref.desc =
	    "data-handle: connection sweep candidate became referenced";
	stats->dh_sweep_close.desc =
	    "data-handle: connection sweep dhandles closed";
	stats->dh_sweep_remove.desc =
	    "data-handle: connection sweep dhandles removed from hash list";
	stats->dh_sweep_tod.desc =
	    "data-handle: connection sweep time-of-death sets";
	stats->dh_sweeps.desc = "data-handle: connection sweeps";
	stats->dh_session_handles.desc = "data-handle: session dhandles swept";
	stats->dh_session_sweeps.desc = "data-handle: session sweep attempts";
	stats->log_slot_closes.desc = "log: consolidated slot closures";
	stats->log_slot_races.desc = "log: consolidated slot join races";
	stats->log_slot_transitions.desc =
	    "log: consolidated slot join transitions";
	stats->log_slot_joins.desc = "log: consolidated slot joins";
	stats->log_slot_toosmall.desc =
	    "log: failed to find a slot large enough for record";
	stats->log_bytes_payload.desc = "log: log bytes of payload data";
	stats->log_bytes_written.desc = "log: log bytes written";
	stats->log_compress_writes.desc = "log: log records compressed";
	stats->log_compress_write_fails.desc =
	    "log: log records not compressed";
	stats->log_compress_small.desc =
	    "log: log records too small to compress";
	stats->log_release_write_lsn.desc =
	    "log: log release advances write LSN";
	stats->log_scans.desc = "log: log scan operations";
	stats->log_scan_rereads.desc =
	    "log: log scan records requiring two reads";
	stats->log_write_lsn.desc =
	    "log: log server thread advances write LSN";
	stats->log_sync.desc = "log: log sync operations";
	stats->log_sync_dir.desc = "log: log sync_dir operations";
	stats->log_writes.desc = "log: log write operations";
	stats->log_slot_consolidated.desc = "log: logging bytes consolidated";
	stats->log_max_filesize.desc = "log: maximum log file size";
	stats->log_prealloc_max.desc =
	    "log: number of pre-allocated log files to create";
	stats->log_prealloc_files.desc =
	    "log: pre-allocated log files prepared";
	stats->log_prealloc_used.desc = "log: pre-allocated log files used";
	stats->log_slot_toobig.desc = "log: record size exceeded maximum";
	stats->log_scan_records.desc = "log: records processed by log scan";
	stats->log_compress_mem.desc =
	    "log: total in-memory size of compressed records";
	stats->log_buffer_size.desc = "log: total log buffer size";
	stats->log_compress_len.desc = "log: total size of compressed records";
	stats->log_slot_coalesced.desc = "log: written slots coalesced";
	stats->log_close_yields.desc =
	    "log: yields waiting for previous log file close";
	stats->lsm_work_queue_app.desc =
	    "LSM: application work units currently queued";
	stats->lsm_work_queue_manager.desc =
	    "LSM: merge work units currently queued";
	stats->lsm_rows_merged.desc = "LSM: rows merged in an LSM tree";
	stats->lsm_checkpoint_throttle.desc =
	    "LSM: sleep for LSM checkpoint throttle";
	stats->lsm_merge_throttle.desc = "LSM: sleep for LSM merge throttle";
	stats->lsm_work_queue_switch.desc =
	    "LSM: switch work units currently queued";
	stats->lsm_work_units_discarded.desc =
	    "LSM: tree maintenance operations discarded";
	stats->lsm_work_units_done.desc =
	    "LSM: tree maintenance operations executed";
	stats->lsm_work_units_created.desc =
	    "LSM: tree maintenance operations scheduled";
	stats->lsm_work_queue_max.desc = "LSM: tree queue hit maximum";
	stats->rec_pages.desc = "reconciliation: page reconciliation calls";
	stats->rec_pages_eviction.desc =
	    "reconciliation: page reconciliation calls for eviction";
	stats->rec_split_stashed_bytes.desc =
	    "reconciliation: split bytes currently awaiting free";
	stats->rec_split_stashed_objects.desc =
	    "reconciliation: split objects currently awaiting free";
	stats->session_cursor_open.desc = "session: open cursor count";
	stats->session_open.desc = "session: open session count";
	stats->page_busy_blocked.desc =
	    "thread-yield: page acquire busy blocked";
	stats->page_forcible_evict_blocked.desc =
	    "thread-yield: page acquire eviction blocked";
	stats->page_locked_blocked.desc =
	    "thread-yield: page acquire locked blocked";
	stats->page_read_blocked.desc =
	    "thread-yield: page acquire read blocked";
	stats->page_sleep.desc =
	    "thread-yield: page acquire time sleeping (usecs)";
	stats->txn_begin.desc = "transaction: transaction begins";
	stats->txn_checkpoint_running.desc =
	    "transaction: transaction checkpoint currently running";
	stats->txn_checkpoint_generation.desc =
	    "transaction: transaction checkpoint generation";
	stats->txn_checkpoint_time_max.desc =
	    "transaction: transaction checkpoint max time (msecs)";
	stats->txn_checkpoint_time_min.desc =
	    "transaction: transaction checkpoint min time (msecs)";
	stats->txn_checkpoint_time_recent.desc =
	    "transaction: transaction checkpoint most recent time (msecs)";
	stats->txn_checkpoint_time_total.desc =
	    "transaction: transaction checkpoint total time (msecs)";
	stats->txn_checkpoint.desc = "transaction: transaction checkpoints";
	stats->txn_fail_cache.desc =
	    "transaction: transaction failures due to cache overflow";
	stats->txn_pinned_range.desc =
	    "transaction: transaction range of IDs currently pinned";
	stats->txn_pinned_checkpoint_range.desc =
	    "transaction: transaction range of IDs currently pinned by a checkpoint";
	stats->txn_sync.desc = "transaction: transaction sync calls";
	stats->txn_commit.desc = "transaction: transactions committed";
	stats->txn_rollback.desc = "transaction: transactions rolled back";
}

void
__wt_stat_refresh_connection_stats(void *stats_arg)
{
	WT_CONNECTION_STATS *stats;

	stats = (WT_CONNECTION_STATS *)stats_arg;
	WT_STATS_CLEAR(stats, async_cur_queue);
	WT_STATS_CLEAR(stats, async_alloc_race);
	WT_STATS_CLEAR(stats, async_flush);
	WT_STATS_CLEAR(stats, async_alloc_view);
	WT_STATS_CLEAR(stats, async_full);
	WT_STATS_CLEAR(stats, async_nowork);
	WT_STATS_CLEAR(stats, async_op_alloc);
	WT_STATS_CLEAR(stats, async_op_compact);
	WT_STATS_CLEAR(stats, async_op_insert);
	WT_STATS_CLEAR(stats, async_op_remove);
	WT_STATS_CLEAR(stats, async_op_search);
	WT_STATS_CLEAR(stats, async_op_update);
	WT_STATS_CLEAR(stats, block_preload);
	WT_STATS_CLEAR(stats, block_read);
	WT_STATS_CLEAR(stats, block_write);
	WT_STATS_CLEAR(stats, block_byte_read);
	WT_STATS_CLEAR(stats, block_byte_write);
	WT_STATS_CLEAR(stats, block_map_read);
	WT_STATS_CLEAR(stats, block_byte_map_read);
	WT_STATS_CLEAR(stats, cache_bytes_read);
	WT_STATS_CLEAR(stats, cache_bytes_write);
	WT_STATS_CLEAR(stats, cache_eviction_checkpoint);
	WT_STATS_CLEAR(stats, cache_eviction_queue_empty);
	WT_STATS_CLEAR(stats, cache_eviction_queue_not_empty);
	WT_STATS_CLEAR(stats, cache_eviction_server_evicting);
	WT_STATS_CLEAR(stats, cache_eviction_server_not_evicting);
	WT_STATS_CLEAR(stats, cache_eviction_slow);
	WT_STATS_CLEAR(stats, cache_eviction_worker_evicting);
	WT_STATS_CLEAR(stats, cache_eviction_force_fail);
	WT_STATS_CLEAR(stats, cache_eviction_hazard);
	WT_STATS_CLEAR(stats, cache_inmem_split);
	WT_STATS_CLEAR(stats, cache_eviction_internal);
	WT_STATS_CLEAR(stats, cache_eviction_dirty);
	WT_STATS_CLEAR(stats, cache_eviction_deepen);
	WT_STATS_CLEAR(stats, cache_eviction_force);
	WT_STATS_CLEAR(stats, cache_eviction_force_delete);
	WT_STATS_CLEAR(stats, cache_eviction_app);
	WT_STATS_CLEAR(stats, cache_read);
	WT_STATS_CLEAR(stats, cache_eviction_fail);
	WT_STATS_CLEAR(stats, cache_eviction_split);
	WT_STATS_CLEAR(stats, cache_eviction_walk);
	WT_STATS_CLEAR(stats, cache_write);
	WT_STATS_CLEAR(stats, cache_eviction_clean);
	WT_STATS_CLEAR(stats, memory_allocation);
	WT_STATS_CLEAR(stats, memory_free);
	WT_STATS_CLEAR(stats, memory_grow);
	WT_STATS_CLEAR(stats, cond_wait);
	WT_STATS_CLEAR(stats, rwlock_read);
	WT_STATS_CLEAR(stats, rwlock_write);
	WT_STATS_CLEAR(stats, read_io);
	WT_STATS_CLEAR(stats, write_io);
	WT_STATS_CLEAR(stats, cursor_create);
	WT_STATS_CLEAR(stats, cursor_insert);
	WT_STATS_CLEAR(stats, cursor_next);
	WT_STATS_CLEAR(stats, cursor_prev);
	WT_STATS_CLEAR(stats, cursor_remove);
	WT_STATS_CLEAR(stats, cursor_reset);
	WT_STATS_CLEAR(stats, cursor_search);
	WT_STATS_CLEAR(stats, cursor_search_near);
	WT_STATS_CLEAR(stats, cursor_update);
	WT_STATS_CLEAR(stats, dh_sweep_ref);
	WT_STATS_CLEAR(stats, dh_sweep_close);
	WT_STATS_CLEAR(stats, dh_sweep_remove);
	WT_STATS_CLEAR(stats, dh_sweep_tod);
	WT_STATS_CLEAR(stats, dh_sweeps);
	WT_STATS_CLEAR(stats, dh_session_handles);
	WT_STATS_CLEAR(stats, dh_session_sweeps);
	WT_STATS_CLEAR(stats, log_slot_closes);
	WT_STATS_CLEAR(stats, log_slot_races);
	WT_STATS_CLEAR(stats, log_slot_transitions);
	WT_STATS_CLEAR(stats, log_slot_joins);
	WT_STATS_CLEAR(stats, log_slot_toosmall);
	WT_STATS_CLEAR(stats, log_bytes_payload);
	WT_STATS_CLEAR(stats, log_bytes_written);
	WT_STATS_CLEAR(stats, log_compress_writes);
	WT_STATS_CLEAR(stats, log_compress_write_fails);
	WT_STATS_CLEAR(stats, log_compress_small);
	WT_STATS_CLEAR(stats, log_release_write_lsn);
	WT_STATS_CLEAR(stats, log_scans);
	WT_STATS_CLEAR(stats, log_scan_rereads);
	WT_STATS_CLEAR(stats, log_write_lsn);
	WT_STATS_CLEAR(stats, log_sync);
	WT_STATS_CLEAR(stats, log_sync_dir);
	WT_STATS_CLEAR(stats, log_writes);
	WT_STATS_CLEAR(stats, log_slot_consolidated);
	WT_STATS_CLEAR(stats, log_prealloc_files);
	WT_STATS_CLEAR(stats, log_prealloc_used);
	WT_STATS_CLEAR(stats, log_slot_toobig);
	WT_STATS_CLEAR(stats, log_scan_records);
	WT_STATS_CLEAR(stats, log_compress_mem);
	WT_STATS_CLEAR(stats, log_compress_len);
	WT_STATS_CLEAR(stats, log_slot_coalesced);
	WT_STATS_CLEAR(stats, log_close_yields);
	WT_STATS_CLEAR(stats, lsm_rows_merged);
	WT_STATS_CLEAR(stats, lsm_checkpoint_throttle);
	WT_STATS_CLEAR(stats, lsm_merge_throttle);
	WT_STATS_CLEAR(stats, lsm_work_units_discarded);
	WT_STATS_CLEAR(stats, lsm_work_units_done);
	WT_STATS_CLEAR(stats, lsm_work_units_created);
	WT_STATS_CLEAR(stats, lsm_work_queue_max);
	WT_STATS_CLEAR(stats, rec_pages);
	WT_STATS_CLEAR(stats, rec_pages_eviction);
	WT_STATS_CLEAR(stats, page_busy_blocked);
	WT_STATS_CLEAR(stats, page_forcible_evict_blocked);
	WT_STATS_CLEAR(stats, page_locked_blocked);
	WT_STATS_CLEAR(stats, page_read_blocked);
	WT_STATS_CLEAR(stats, page_sleep);
	WT_STATS_CLEAR(stats, txn_begin);
	WT_STATS_CLEAR(stats, txn_checkpoint);
	WT_STATS_CLEAR(stats, txn_fail_cache);
	WT_STATS_CLEAR(stats, txn_sync);
	WT_STATS_CLEAR(stats, txn_commit);
	WT_STATS_CLEAR(stats, txn_rollback);
}
