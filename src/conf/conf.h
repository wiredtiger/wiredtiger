#pragma once

extern int __wt_conf_bind(WT_SESSION_IMPL *session, const char *compiled_str, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conf_compile(WT_SESSION_IMPL *session, const char *api, const char *format,
  const char **resultp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conf_compile_api_call(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *centry,
  u_int centry_index, const char *config, void *compile_buf, size_t compile_buf_size,
  WT_CONF **confp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conf_compile_init(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conf_gets_func(WT_SESSION_IMPL *session, const WT_CONF *orig_conf,
  uint64_t orig_keys, int def, bool use_def, WT_CONFIG_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_conf_compile_discard(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif
