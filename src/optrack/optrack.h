#pragma once

extern void __wt_optrack_flush_buffer(WT_SESSION_IMPL *s);
extern void __wt_optrack_record_funcid(
  WT_SESSION_IMPL *session, const char *func, uint16_t *func_idp);

#ifdef HAVE_UNITTEST

#endif
