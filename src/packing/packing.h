#pragma once

/*
 * __wt_ext_pack_close --
 *     WT_EXTENSION.pack_close
 */
extern int __wt_ext_pack_close(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, size_t *usedp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_pack_int --
 *     WT_EXTENSION.pack_int
 */
extern int __wt_ext_pack_int(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, int64_t i)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_pack_item --
 *     WT_EXTENSION.pack_item
 */
extern int __wt_ext_pack_item(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_pack_start --
 *     WT_EXTENSION.pack_start method.
 */
extern int __wt_ext_pack_start(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *format,
  void *buffer, size_t size, WT_PACK_STREAM **psp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_pack_str --
 *     WT_EXTENSION.pack_str
 */
extern int __wt_ext_pack_str(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, const char *s)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_pack_uint --
 *     WT_EXTENSION.pack_uint
 */
extern int __wt_ext_pack_uint(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, uint64_t u)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_struct_pack --
 *     Pack a byte string (extension API).
 */
extern int __wt_ext_struct_pack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, void *buffer,
  size_t len, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_struct_size --
 *     Calculate the size of a packed byte string (extension API).
 */
extern int __wt_ext_struct_size(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, size_t *lenp,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_struct_unpack --
 *     Unpack a byte string (extension API).
 */
extern int __wt_ext_struct_unpack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const void *buffer, size_t len, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_unpack_int --
 *     WT_EXTENSION.unpack_int
 */
extern int __wt_ext_unpack_int(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, int64_t *ip)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_unpack_item --
 *     WT_EXTENSION.unpack_item
 */
extern int __wt_ext_unpack_item(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_unpack_start --
 *     WT_EXTENSION.unpack_start
 */
extern int __wt_ext_unpack_start(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *format, const void *buffer, size_t size, WT_PACK_STREAM **psp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_unpack_str --
 *     WT_EXTENSION.unpack_str
 */
extern int __wt_ext_unpack_str(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, const char **sp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_unpack_uint --
 *     WT_EXTENSION.unpack_uint
 */
extern int __wt_ext_unpack_uint(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, uint64_t *up)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_check --
 *     Check that the specified packing format is valid, and whether it fits into a fixed-sized
 *     bitfield.
 */
extern int __wt_struct_check(WT_SESSION_IMPL *session, const char *fmt, size_t len, bool *fixedp,
  uint32_t *fixed_lenp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_confchk --
 *     Check that the specified packing format is valid, configuration version.
 */
extern int __wt_struct_confchk(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_pack --
 *     Pack a byte string.
 */
extern int __wt_struct_pack(WT_SESSION_IMPL *session, void *buffer, size_t len, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_repack --
 *     Return the subset of the packed buffer that represents part of the format. If the result is
 *     not contiguous in the existing buffer, a buffer is reallocated and filled.
 */
extern int __wt_struct_repack(WT_SESSION_IMPL *session, const char *infmt, const char *outfmt,
  const WT_ITEM *inbuf, WT_ITEM *outbuf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_size --
 *     Calculate the size of a packed byte string.
 */
extern int __wt_struct_size(WT_SESSION_IMPL *session, size_t *lenp, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_struct_unpack --
 *     Unpack a byte string.
 */
extern int __wt_struct_unpack(WT_SESSION_IMPL *session, const void *buffer, size_t len,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
