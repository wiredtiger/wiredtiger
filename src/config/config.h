#pragma once

/*
 * __wt_config_check --
 *     Check the keys in an application-supplied config string match what is specified in an array
 *     of check strings.
 */
extern int __wt_config_check(WT_SESSION_IMPL *session, const WT_CONFIG_ENTRY *entry,
  const char *config, size_t config_len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_collapse --
 *     Collapse a set of configuration strings into newly allocated memory. This function takes a
 *     NULL-terminated list of configuration strings (where the first one contains all the defaults
 *     and the values are in order from least to most preferred, that is, the default values are
 *     least preferred), and collapses them into newly allocated memory. The algorithm is to walk
 *     the first of the configuration strings, and for each entry, search all of the configuration
 *     strings for a final value, keeping the last value found. Notes: Any key not appearing in the
 *     first configuration string is discarded from the final result, because we'll never search for
 *     it. Nested structures aren't parsed. For example, imagine a configuration string contains
 *     "key=(k2=v2,k3=v3)", and a subsequent string has "key=(k4=v4)", the result will be
 *     "key=(k4=v4)", as we search for and use the final value of "key", regardless of field overlap
 *     or missing fields in the nested value.
 */
extern int __wt_config_collapse(WT_SESSION_IMPL *session, const char **cfg, char **config_ret)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_get_choice --
 *     Walk through list of legal choices looking for an item.
 */
extern bool __wt_config_get_choice(const char **choices, WT_CONFIG_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_getone --
 *     Get the value for a given key from a single config string.
 */
extern int __wt_config_getone(WT_SESSION_IMPL *session, const char *config, WT_CONFIG_ITEM *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_getones --
 *     Get the value for a given string key from a single config string.
 */
extern int __wt_config_getones(WT_SESSION_IMPL *session, const char *config, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_getones_none --
 *     Get the value for a given string key from a single config string. Treat "none" as empty.
 */
extern int __wt_config_getones_none(WT_SESSION_IMPL *session, const char *config, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_gets --
 *     Given a NULL-terminated list of configuration strings, find the final value for a given
 *     string key.
 */
extern int __wt_config_gets(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_gets_def --
 *     Performance hack: skip parsing config strings by hard-coding defaults. It's expensive to
 *     repeatedly parse configuration strings, so don't do it unless it's necessary in performance
 *     paths like cursor creation. Assume the second configuration string is the application's
 *     configuration string, and if it's not set (which is true most of the time), then use the
 *     supplied default value. This makes it faster to open cursors when checking for obscure open
 *     configuration strings like "next_random".
 */
extern int __wt_config_gets_def(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  int def, WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_gets_none --
 *     Given a NULL-terminated list of configuration strings, find the final value for a given
 *     string key. Treat "none" as empty.
 */
extern int __wt_config_gets_none(WT_SESSION_IMPL *session, const char **cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_init --
 *     Initialize a config handle, used to iterate through a NUL-terminated config string.
 */
extern void __wt_config_init(WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_initn --
 *     Initialize a config handle, used to iterate through a config string of specified length.
 */
extern void __wt_config_initn(WT_SESSION_IMPL *session, WT_CONFIG *conf, const char *str,
  size_t len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_merge --
 *     Merge a set of configuration strings into newly allocated memory, optionally discarding
 *     configuration items. This function takes a NULL-terminated list of configuration strings
 *     (where the values are in order from least to most preferred), and merges them into newly
 *     allocated memory. The algorithm is to walk the configuration strings and build a table of
 *     each key/value pair. The pairs are sorted based on the name and the configuration string in
 *     which they were found, and a final configuration string is built from the result.
 *     Additionally, a configuration string can be specified and those configuration values are
 *     removed from the final string. Note: Nested structures are parsed and merged. For example, if
 *     configuration strings "key=(k1=v1,k2=v2)" and "key=(k1=v2)" appear, the result will be
 *     "key=(k1=v2,k2=v2)" because the nested values are merged.
 */
extern int __wt_config_merge(WT_SESSION_IMPL *session, const char **cfg, const char *cfg_strip,
  const char **config_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_next --
 *     Get the next config item in the string and process the value.
 */
extern int __wt_config_next(WT_CONFIG *conf, WT_CONFIG_ITEM *key, WT_CONFIG_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_subget_next --
 *     Get the value for a given key from a config string and set the processed value in the given
 *     key structure. This is useful for unusual case of dealing with list in config string.
 */
extern int __wt_config_subget_next(WT_CONFIG *conf, WT_CONFIG_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_subgetraw --
 *     Get the value for a given key from a config string in a WT_CONFIG_ITEM. This is useful for
 *     dealing with nested structs in config strings.
 */
extern int __wt_config_subgetraw(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, WT_CONFIG_ITEM *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_subgets --
 *     Get the value for a given key from a config string in a WT_CONFIG_ITEM. This is useful for
 *     dealing with nested structs in config strings.
 */
extern int __wt_config_subgets(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *cfg, const char *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_subinit --
 *     Initialize a config handle, used to iterate through a config string extracted from another
 *     config string (used for parsing nested structures).
 */
extern void __wt_config_subinit(WT_SESSION_IMPL *session, WT_CONFIG *conf, WT_CONFIG_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_config_tiered_strip --
 *     Strip any configuration options that should not be persisted in the metadata from the
 *     configuration string.
 */
extern int __wt_config_tiered_strip(WT_SESSION_IMPL *session, const char **cfg,
  const char **config_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_configure_method --
 *     WT_CONNECTION.configure_method.
 */
extern int __wt_configure_method(WT_SESSION_IMPL *session, const char *method, const char *uri,
  const char *config, const char *type, const char *check)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_conn_foc_discard --
 *     Discard any memory the connection accumulated.
 */
extern void __wt_conn_foc_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_config_get --
 *     Given a NULL-terminated list of configuration strings, find the final value for a given
 *     string key (external API version).
 */
extern int __wt_ext_config_get(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  WT_CONFIG_ARG *cfg_arg, const char *key, WT_CONFIG_ITEM *cval)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_config_get_string --
 *     Given a configuration string, find the value for a given string key (external API version).
 */
extern int __wt_ext_config_get_string(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *config, const char *key, WT_CONFIG_ITEM *cval)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_config_parser_open --
 *     WT_EXTENSION_API->config_parser_open implementation
 */
extern int __wt_ext_config_parser_open(WT_EXTENSION_API *wt_ext, WT_SESSION *wt_session,
  const char *config, size_t len, WT_CONFIG_PARSER **config_parserp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_config_parser_open_arg --
 *     WT_EXTENSION_API->config_parser_open_arg implementation
 */
extern int __wt_ext_config_parser_open_arg(WT_EXTENSION_API *wt_ext, WT_SESSION *wt_session,
  WT_CONFIG_ARG *cfg_arg, WT_CONFIG_PARSER **config_parserp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
