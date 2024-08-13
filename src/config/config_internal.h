#pragma once

extern int __wti_config_get(WT_SESSION_IMPL *session, const char **cfg_arg, WT_CONFIG_ITEM *key,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
