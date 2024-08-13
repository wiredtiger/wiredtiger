#pragma once

extern bool __wti_block_offset_invalid(WT_BLOCK *block, wt_off_t offset, uint32_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_alloc(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t *offp,
  wt_off_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_checkpoint_final(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  uint8_t **file_sizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ckpt_init(WT_SESSION_IMPL *session, WT_BLOCK_CKPT *ci, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ckpt_pack(WT_SESSION_IMPL *session, WT_BLOCK *block, uint8_t **pp,
  WT_BLOCK_CKPT *ci, bool skip_avail) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ckpt_unpack(WT_SESSION_IMPL *session, WT_BLOCK *block, const uint8_t *ckpt,
  size_t ckpt_size, WT_BLOCK_CKPT *ci) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_discard(WT_SESSION_IMPL *session, WT_BLOCK *block, size_t added_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ext_alloc(WT_SESSION_IMPL *session, WT_EXT **extp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ext_discard(WT_SESSION_IMPL *session, u_int max)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_ext_prealloc(WT_SESSION_IMPL *session, u_int max)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_check(WT_SESSION_IMPL *session, WT_EXTLIST *al, WT_EXTLIST *bl)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_init(WT_SESSION_IMPL *session, WT_EXTLIST *el, const char *name,
  const char *extname, bool track_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_merge(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *a,
  WT_EXTLIST *b) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_overlap(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_read(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el,
  wt_off_t ckpt_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_read_avail(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el,
  wt_off_t ckpt_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_truncate(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_extlist_write(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el,
  WT_EXTLIST *additional) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_insert_ext(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el,
  wt_off_t off, wt_off_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_misplaced(WT_SESSION_IMPL *session, WT_BLOCK *block, const char *list,
  wt_off_t offset, uint32_t size, bool live, const char *func, int line)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_off_free(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid,
  wt_off_t offset, wt_off_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_off_remove_overlap(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_EXTLIST *el,
  wt_off_t off, wt_off_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_read_off(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  uint32_t objectid, wt_off_t offset, uint32_t size, uint32_t checksum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_size_alloc(WT_SESSION_IMPL *session, WT_SIZE **szp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_truncate(WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_block_write_off(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  wt_off_t *offsetp, uint32_t *sizep, uint32_t *checksump, bool data_checksum, bool checkpoint_io,
  bool caller_locked) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_desc_write(WT_SESSION_IMPL *session, WT_FH *fh, uint32_t allocsize)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_verify_ckpt_load(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_verify_ckpt_unload(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_block_ckpt_destroy(WT_SESSION_IMPL *session, WT_BLOCK_CKPT *ci);
extern void __wti_block_configure_first_fit(WT_BLOCK *block, bool on);
extern void __wti_block_ext_free(WT_SESSION_IMPL *session, WT_EXT *ext);
extern void __wti_block_extlist_free(WT_SESSION_IMPL *session, WT_EXTLIST *el);
extern void __wti_block_size_free(WT_SESSION_IMPL *session, WT_SIZE *sz);
extern void __wti_ckpt_verbose(WT_SESSION_IMPL *session, WT_BLOCK *block, const char *tag,
  const char *ckpt_name, const uint8_t *ckpt_string, size_t ckpt_size);

#ifdef HAVE_UNITTEST

#endif
