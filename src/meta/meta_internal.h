#pragma once

extern int __wti_meta_track_insert(WT_SESSION_IMPL *session, const char *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_meta_track_update(WT_SESSION_IMPL *session, const char *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_turtle_read(WT_SESSION_IMPL *session, const char *key, char **valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_turtle_update(WT_SESSION_IMPL *session, const char *key, const char *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
