#pragma once

/*
 * __wt_optrack_flush_buffer --
 *     Flush optrack buffer. Returns the number of bytes flushed to the file.
 */
extern void __wt_optrack_flush_buffer(WT_SESSION_IMPL *s)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_optrack_record_funcid --
 *     Allocate and record optrack function ID.
 */
extern void __wt_optrack_record_funcid(WT_SESSION_IMPL *session, const char *func,
  uint16_t *func_idp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
