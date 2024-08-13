#pragma once

extern WT_EXT *__wt_block_off_srch_inclusive(WT_EXTLIST *el, wt_off_t off)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_addr_invalid(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr,
  size_t addr_size, bool live) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_addr_pack(WT_BLOCK *block, uint8_t **pp, uint32_t objectid, wt_off_t offset,
  uint32_t size, uint32_t checksum) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_addr_string(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  const uint8_t *addr, size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_addr_unpack(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *p,
  size_t addr_size, uint32_t *objectidp, wt_off_t *offsetp, uint32_t *sizep, uint32_t *checksump)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  WT_CKPT *ckptbase, bool data_checksum) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint_last(WT_SESSION_IMPL *session, WT_BLOCK *block, char **metadatap,
  char **checkpoint_listp, WT_ITEM *checkpoint) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint_load(WT_SESSION_IMPL *session, WT_BLOCK *block,
  const uint8_t *addr, size_t addr_size, uint8_t *root_addr, size_t *root_addr_sizep,
  bool checkpoint) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint_resolve(WT_SESSION_IMPL *session, WT_BLOCK *block, bool failed)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_checkpoint_unload(WT_SESSION_IMPL *session, WT_BLOCK *block, bool checkpoint)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_ckpt_decode(WT_SESSION *wt_session, WT_BLOCK *block, const uint8_t *ckpt,
  size_t ckpt_size, WT_BLOCK_CKPT *ci) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_close(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_compact_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_compact_page_rewrite(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t *addr,
  size_t *addr_sizep, bool *skipp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_compact_page_skip(
  WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr, size_t addr_size, bool *skipp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_compact_skip(WT_SESSION_IMPL *session, WT_BLOCK *block, bool *skipp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_compact_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_free(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_manager_create(WT_SESSION_IMPL *session, const char *filename,
  uint32_t allocsize) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_manager_drop(WT_SESSION_IMPL *session, const char *filename, bool durable)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_manager_drop_object(WT_SESSION_IMPL *session, WT_BUCKET_STORAGE *bstorage,
  const char *filename, bool durable) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_manager_named_size(WT_SESSION_IMPL *session, const char *name,
  wt_off_t *sizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_manager_size(WT_BM *bm, WT_SESSION_IMPL *session, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_open(WT_SESSION_IMPL *session, const char *filename, uint32_t objectid,
  const char *cfg[], bool forced_salvage, bool readonly, bool fixed, uint32_t allocsize,
  WT_BLOCK **blockp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_read_off_blind(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t offset,
  uint32_t *sizep, uint32_t *checksump) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_salvage_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_salvage_next(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t *addr,
  size_t *addr_sizep, bool *eofp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_salvage_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_salvage_valid(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t *addr,
  size_t addr_size, bool valid) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_verify_addr(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_verify_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_verify_start(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_CKPT *ckptbase,
  const char *cfg[]) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_write(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf, uint8_t *addr,
  size_t *addr_sizep, bool data_checksum, bool checkpoint_io)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_block_write_size(WT_SESSION_IMPL *session, WT_BLOCK *block, size_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bm_corrupt(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bm_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_block_compact_get_progress_stats(
  WT_SESSION_IMPL *session, WT_BM *bm, uint64_t *pages_reviewedp);
extern void __wt_block_compact_progress(WT_SESSION_IMPL *session, WT_BLOCK *block);
extern void __wt_block_stat(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_DSRC_STATS *stats);

#ifdef HAVE_UNITTEST
extern WT_EXT *__ut_block_off_srch_last(WT_EXT **head, WT_EXT ***stack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __ut_block_first_srch(WT_EXT **head, wt_off_t size, WT_EXT ***stack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __ut_ckpt_add_blkmod_entry(WT_SESSION_IMPL *session, WT_BLOCK_MODS *blk_mod,
  wt_off_t offset, wt_off_t len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __ut_block_off_srch(WT_EXT **head, wt_off_t off, WT_EXT ***stack, bool skip_off);
extern void __ut_block_size_srch(WT_SIZE **head, wt_off_t size, WT_SIZE ***stack);

#endif
