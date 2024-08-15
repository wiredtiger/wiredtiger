#pragma once
// TODO - prototypes.py overwrites this file and removes all of my beautiful dependency include work.
// Need to update s_all to stop doing this
#include "../include/wt_prelude.h"

#include "../support/support.h"         // Required dependency
#include "../btree/btree.h"             // Required dependency
#include "../txn/txn.h"                 // Required dependency
#include "../conn/conn.h"               // Single dependency __wt_conn_prefetch_clear_tree
#include "../session/session.h"         // Single dependency __wt_session_release_resources
#include "../os_common/os_common.h"     // Single dependency __wt_calloc_one
#include "../reconcile/reconcile.h"     // Single dependency __wt_reconcile
#include "../history/history.h"         // Single dependency __wt_hs_get_btree
#include "../cursor/cursor.h"           // Single dependency __wt_curhs_cache
#include "../packing/packing.h"         // Not needed by evict. Transitive dependency
#include "../optrack/optrack.h"         // Not needed by evict. Transitive dependency

#include "../include/extern_platform_specific.h"

#include "../include/mutex_inline.h"     // Required dependency
#include "../include/time_inline.h"      // Not needed by evict. Transitive dependency (ref_inline.h)
#include "../include/ref_inline.h"       // Single dependency __wt_ref_is_root
#include "../include/timestamp_inline.h" // Required dependency
#include "../include/log_inline.h"       // Not needed by evict. Transitive dependency (txn_inline.h)
#include "../include/txn_inline.h"       // Required dependency

#include "../include/misc_inline.h"      // Not needed by evict. Transitive dependency (cache_inline.h)
#include "../include/intpack_inline.h"   // Not needed by evict. Transitive dependency (cell_inline.h)
#include "../include/cell_inline.h"      // Not needed by evict. Transitive dependency (btree_inline.h)
#include "../include/buf_inline.h"       // Not needed by evict. Transitive dependency (btree_inline.h)
#include "../include/btree_cmp_inline.h" // Not needed by evict. Transitive dependency (btree_inline.h)

extern bool __wt_page_evict_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cache_eviction_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double pct_full) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_create(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_page_release_evict(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_curstat_cache_walk(WT_SESSION_IMPL *session);
extern void __wt_evict_file_exclusive_off(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_clear(WT_SESSION_IMPL *session);
extern void __wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v);
extern void __wt_evict_server_wake(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif

// Evict depends on these files, but they also depend on the functions defined above.
// TODO break these circular dependencies.
#include "../include/cache_inline.h"     // cache_inline uses __wt_cache_eviction_worker
#include "../include/btree_inline.h"     // Uses __wt_evict_file_exclusive_off
