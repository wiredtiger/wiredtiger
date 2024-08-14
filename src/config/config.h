#pragma once

extern bool __wt_config_get_choice(const char **choices, WT_CONFIG_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const WT_CONFIG_ENTRY *__wt_conn_config_match(const char *method)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const WT_CONFIG_ENTRY *__wt_test_config_match(const char *test_name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_check(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *entry,
  const char *config, size_t config_len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_collapse(WT_SESSION_IMPL *session, const char **cfg, char **config_ret)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_getone(WT_SESSION_IMPL *session, const char *config, WT_CONFIG_ITEM *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_getones(WT_SESSION_IMPL *session, const char *config, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_getones_none(WT_SESSION_IMPL *session, const char *config, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_gets(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_gets_def(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  int def, WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_gets_none(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_merge(WT_SESSION_IMPL *session, const char **cfg, const char *cfg_strip,
  const char **config_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_next(WT_CONFIG *conf, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_subget_next(WT_CONFIG *conf, WT_CONFIG_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_subgetraw(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, WT_CONFIG_ITEM *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_subgets(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_config_tiered_strip(WT_SESSION_IMPL *session, const char **cfg,
  const char **config_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_configure_method(WT_SESSION_IMPL *session, const char *method, const char *uri,
  const char *config, const char *type, const char *check)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_conn_config_init(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_config_get(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  WT_CONFIG_ARG *cfg_arg, const char *key, WT_CONFIG_ITEM *cval)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_config_get_string(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *config, const char *key, WT_CONFIG_ITEM *cval)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_config_parser_open(WT_EXTENSION_API *wt_ext, WT_SESSION *wt_session,
  const char *config, size_t len, WT_CONFIG_PARSER **config_parserp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_config_parser_open_arg(WT_EXTENSION_API *wt_ext, WT_SESSION *wt_session,
  WT_CONFIG_ARG *cfg_arg, WT_CONFIG_PARSER **config_parserp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_config_init(WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str);
extern void __wt_config_initn(
  WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str, size_t len);
extern void __wt_config_subinit(WT_SESSION_IMPL *session, WT_CONFIG *conf, WT_CONFIG_ITEM *item);
extern void __wt_conn_config_discard(WT_SESSION_IMPL *session);
extern void __wt_conn_foc_discard(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif
