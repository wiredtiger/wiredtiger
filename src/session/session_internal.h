#pragma once

extern int __wti_session_compact(WT_SESSION *wt_session, const char *uri, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_session_compact_readonly(WT_SESSION *wt_session, const char *uri,
  const char *config) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_session_notsup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
