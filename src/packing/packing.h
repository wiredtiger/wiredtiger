#pragma once

extern int __wt_ext_pack_close(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, size_t *usedp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_pack_int(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, int64_t i)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_pack_item(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_pack_start(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *format,
  void *buffer, size_t size, WT_PACK_STREAM **psp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_pack_str(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, const char *s)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_pack_uint(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, uint64_t u)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_struct_pack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, void *buffer,
  size_t len, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_struct_size(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, size_t *lenp,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_struct_unpack(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const void *buffer, size_t len, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_unpack_int(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, int64_t *ip)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_unpack_item(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_unpack_start(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *format, const void *buffer, size_t size, WT_PACK_STREAM **psp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_unpack_str(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, const char **sp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_unpack_uint(WT_EXTENSION_API *wt_api, WT_PACK_STREAM *ps, uint64_t *up)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_check(WT_SESSION_IMPL *session, const char *fmt, size_t len, bool *fixedp,
  uint32_t *fixed_lenp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_confchk(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_pack(WT_SESSION_IMPL *session, void *buffer, size_t len, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_repack(WT_SESSION_IMPL *session, const char *infmt, const char *outfmt,
  const WT_ITEM *inbuf, WT_ITEM *outbuf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_size(WT_SESSION_IMPL *session, size_t *lenp, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_struct_unpack(WT_SESSION_IMPL *session, const void *buffer, size_t len,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
