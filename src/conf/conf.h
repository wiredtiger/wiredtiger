#pragma once

/*
 * __wt_conf_bind --
 *     Bind values to a configuration string.
 */
extern int __wt_conf_bind(WT_SESSION_IMPL *session, const char *compiled_str, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_compile --
 *     Compile a configuration string in a way that can be used by API calls.
 */
extern int __wt_conf_compile(WT_SESSION_IMPL *session, const char *api, const char *format,
  const char **resultp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_compile_api_call --
 *     Given an array of config strings, parse them, returning the compiled structure. This is
 *     called from an API call.
 */
extern int __wt_conf_compile_api_call(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *centry,
  u_int centry_index, const char *config, void *compile_buf, size_t compile_buf_size,
  WT_CONF **confp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_compile_discard --
 *     Discard compiled configuration info.
 */
extern void __wt_conf_compile_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_compile_init --
 *     Initialization for the configuration compilation system.
 */
extern int __wt_conf_compile_init(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conf_gets_func --
 *     Given a compiled structure of configuration strings, find the final value for a given key,
 *     represented as (up to 4) 16-bit key ids packed into a 64-bit key. If a default is given, it
 *     overrides any default found in the compiled structure.
 */
extern int __wt_conf_gets_func(WT_SESSION_IMPL *session, const WT_CONF *orig_conf,
  uint64_t orig_keys, int def, bool use_def, WT_CONFIG_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
