/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Noop eviction implementation -- all stubs return meaningful no-op values so the system can
 * function without any eviction activity.
 */

/*
 * __wt_evict_noop_page --
 *     Noop evict page: restore the ref state and return EBUSY.
 */
int
__wt_evict_noop_page(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state, uint32_t flags)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(flags);

    WT_REF_SET_STATE(ref, previous_state);
    return (EBUSY);
}

/*
 * __wt_evict_noop_file --
 *     Noop evict file: nothing to do.
 */
int
__wt_evict_noop_file(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(syncop);
    return (0);
}

/*
 * __wt_evict_noop_config --
 *     Noop config: nothing to configure.
 */
int
__wt_evict_noop_config(WT_EVICT *evict, WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(cfg);
    WT_UNUSED(reconfig);
    return (0);
}

/*
 * __wt_evict_noop_destroy --
 *     Noop destroy: free the evict structure.
 */
int
__wt_evict_noop_destroy(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);

    __wt_free(session, S2C(session)->evict);
    return (0);
}

/*
 * __wt_evict_noop_stats_update --
 *     Noop stats update: nothing to do.
 */
void
__wt_evict_noop_stats_update(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_stats_init --
 *     Noop stats init: nothing to do.
 */
void
__wt_evict_noop_stats_init(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_server_wake --
 *     Noop server wake: no server to wake.
 */
void
__wt_evict_noop_server_wake(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_threads_create --
 *     Noop threads create: no threads needed.
 */
int
__wt_evict_noop_threads_create(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_threads_destroy --
 *     Noop threads destroy: no threads to destroy.
 */
int
__wt_evict_noop_threads_destroy(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_file_exclusive_on --
 *     Noop file exclusive on: nothing to do.
 */
int
__wt_evict_noop_file_exclusive_on(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_file_exclusive_off --
 *     Noop file exclusive off: nothing to do.
 */
void
__wt_evict_noop_file_exclusive_off(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_page_urgent --
 *     Noop page urgent: nothing is urgent.
 */
bool
__wt_evict_noop_page_urgent(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(ref);
    return (false);
}

/*
 * __wt_evict_noop_priority_set --
 *     Noop priority set: nothing to do.
 */
void
__wt_evict_noop_priority_set(WT_EVICT *evict, WT_SESSION_IMPL *session, uint64_t v)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(v);
}

/*
 * __wt_evict_noop_priority_clear --
 *     Noop priority clear: nothing to do.
 */
void
__wt_evict_noop_priority_clear(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_verbose_dump_cache --
 *     Noop verbose dump cache: nothing to dump.
 */
int
__wt_evict_noop_verbose_dump_cache(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_cache_stat_walk --
 *     Noop cache stat walk: nothing to walk.
 */
void
__wt_evict_noop_cache_stat_walk(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_aggressive --
 *     Noop aggressive: never aggressive.
 */
bool
__wt_evict_noop_aggressive(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (false);
}

/*
 * __wt_evict_noop_cache_stuck --
 *     Noop cache stuck: never stuck.
 */
bool
__wt_evict_noop_cache_stuck(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (false);
}

/*
 * __wt_evict_noop_clean_needed --
 *     Noop clean needed: eviction never needed.
 */
bool
__wt_evict_noop_clean_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, double *pct_fullp)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    if (pct_fullp != NULL)
        *pct_fullp = 0.0;
    return (false);
}

/*
 * __wt_evict_noop_clean_pressure --
 *     Noop clean pressure: no pressure.
 */
bool
__wt_evict_noop_clean_pressure(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (false);
}

/*
 * __wt_evict_noop_dirty_needed --
 *     Noop dirty needed: eviction never needed.
 */
bool
__wt_evict_noop_dirty_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, double *pct_fullp)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    if (pct_fullp != NULL)
        *pct_fullp = 0.0;
    return (false);
}

/*
 * __wt_evict_noop_needed --
 *     Noop needed: eviction never needed.
 */
bool
__wt_evict_noop_needed(WT_EVICT *evict, WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool ignore_updates_dirty, double *pct_fullp)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(busy);
    WT_UNUSED(readonly);
    WT_UNUSED(ignore_updates_dirty);
    if (pct_fullp != NULL)
        *pct_fullp = 0.0;
    return (false);
}

/*
 * __wt_evict_noop_favor_clearing_dirty --
 *     Noop favor clearing dirty: nothing to do.
 */
void
__wt_evict_noop_favor_clearing_dirty(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_app_assist_worker_check --
 *     Noop app assist worker check: no work to do.
 */
int
__wt_evict_noop_app_assist_worker_check(
  WT_EVICT *evict, WT_SESSION_IMPL *session, bool busy, bool readonly, bool interruptible, bool *didworkp)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(busy);
    WT_UNUSED(readonly);
    WT_UNUSED(interruptible);
    if (didworkp != NULL)
        *didworkp = false;
    return (0);
}

/*
 * __wt_evict_noop_page_init --
 *     Noop page init: nothing to initialize.
 */
void
__wt_evict_noop_page_init(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
}

/*
 * __wt_evict_noop_touch_page --
 *     Noop touch page: nothing to do.
 */
void
__wt_evict_noop_touch_page(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page, bool internal_only, bool wont_need)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    WT_UNUSED(internal_only);
    WT_UNUSED(wont_need);
}

/*
 * __wt_evict_noop_page_soon --
 *     Noop page soon: nothing to do.
 */
void
__wt_evict_noop_page_soon(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(ref);
}

/*
 * __wt_evict_noop_page_is_soon --
 *     Noop page is soon: never soon.
 */
bool
__wt_evict_noop_page_is_soon(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    return (false);
}

/*
 * __wt_evict_noop_page_is_soon_or_wont_need --
 *     Noop page is soon or wont need: never.
 */
bool
__wt_evict_noop_page_is_soon_or_wont_need(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    return (false);
}

/*
 * __wt_evict_noop_page_first_dirty --
 *     Noop page first dirty: nothing to do.
 */
void
__wt_evict_noop_page_first_dirty(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
}

/*
 * __wt_evict_noop_inherit_page_state --
 *     Noop inherit page state: nothing to do.
 */
void
__wt_evict_noop_inherit_page_state(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *orig_page, WT_PAGE *new_page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(orig_page);
    WT_UNUSED(new_page);
}

/*
 * __wt_evict_noop_page_cache_bytes_decr --
 *     Noop page cache bytes decr: nothing to do.
 */
void
__wt_evict_noop_page_cache_bytes_decr(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
}

/*
 * __wt_evict_noop_clear_npos --
 *     Noop clear npos: nothing to do.
 */
void
__wt_evict_noop_clear_npos(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_reset_checkpoint_stats --
 *     Noop reset checkpoint stats: nothing to do.
 */
void
__wt_evict_noop_reset_checkpoint_stats(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_get_walk_tree --
 *     Noop get walk tree: no walk tree.
 */
WT_DATA_HANDLE *
__wt_evict_noop_get_walk_tree(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (NULL);
}

/*
 * __wt_evict_noop_pass_interrupt_inc --
 *     Noop pass interrupt inc: nothing to do.
 */
void
__wt_evict_noop_pass_interrupt_inc(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_pass_interrupt_dec --
 *     Noop pass interrupt dec: nothing to do.
 */
void
__wt_evict_noop_pass_interrupt_dec(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_get_evict_pass_gen --
 *     Noop get evict pass gen: always zero.
 */
uint64_t
__wt_evict_noop_get_evict_pass_gen(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_get_page_evict_pass_gen --
 *     Noop get page evict pass gen: always zero.
 */
uint64_t
__wt_evict_noop_get_page_evict_pass_gen(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    return (0);
}

/*
 * __wt_evict_noop_save_evict_state --
 *     Noop save evict state: nothing to save.
 */
void
__wt_evict_noop_save_evict_state(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE_MODIFY *mod)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(mod);
}

/*
 * __wt_evict_noop_copy_evict_state --
 *     Noop copy evict state: nothing to copy.
 */
void
__wt_evict_noop_copy_evict_state(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE_MODIFY *dst, WT_PAGE_MODIFY *src)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(dst);
    WT_UNUSED(src);
}

/*
 * __wt_evict_noop_page_evict_retry --
 *     Noop page evict retry: always allow retry.
 */
bool
__wt_evict_noop_page_evict_retry(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    return (true);
}

/*
 * __wt_evict_noop_page_set_cache_create_gen --
 *     Noop page set cache create gen: nothing to set.
 */
void
__wt_evict_noop_page_set_cache_create_gen(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
}

/*
 * __wt_evict_noop_page_get_cache_create_gen --
 *     Noop page get cache create gen: always zero.
 */
uint64_t
__wt_evict_noop_page_get_cache_create_gen(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(page);
    return (0);
}

/*
 * __wt_evict_noop_btree_get_priority --
 *     Noop btree get priority: always zero.
 */
uint64_t
__wt_evict_noop_btree_get_priority(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_evict_noop_btree_save_walk_period --
 *     Noop btree save walk period: nothing to do.
 */
void
__wt_evict_noop_btree_save_walk_period(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_btree_restore_walk_period --
 *     Noop btree restore walk period: nothing to do.
 */
void
__wt_evict_noop_btree_restore_walk_period(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_btree_is_eviction_disabled --
 *     Noop btree is eviction disabled: always disabled.
 */
bool
__wt_evict_noop_btree_is_eviction_disabled(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (true);
}

/*
 * __wt_evict_noop_btree_set_disabled_open --
 *     Noop btree set disabled open: nothing to do.
 */
void
__wt_evict_noop_btree_set_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_btree_is_disabled_open --
 *     Noop btree is disabled open: always false.
 */
bool
__wt_evict_noop_btree_is_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (false);
}

/*
 * __wt_evict_noop_btree_clear_disabled_open --
 *     Noop btree clear disabled open: nothing to do.
 */
void
__wt_evict_noop_btree_clear_disabled_open(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
}

/*
 * __wt_evict_noop_btree_busy_inc --
 *     Noop btree evict busy inc: nothing to do.
 */
void
__wt_evict_noop_btree_busy_inc(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_btree_busy_dec --
 *     Noop btree evict busy dec: nothing to do.
 */
void
__wt_evict_noop_btree_busy_dec(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_btree_prefetch_busy_inc --
 *     Noop btree prefetch busy inc: nothing to do.
 */
void
__wt_evict_noop_btree_prefetch_busy_inc(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_btree_prefetch_busy_dec --
 *     Noop btree prefetch busy dec: nothing to do.
 */
void
__wt_evict_noop_btree_prefetch_busy_dec(WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_btree_prefetch_busy_wait --
 *     Noop btree prefetch busy wait: nothing to wait for.
 */
void
__wt_evict_noop_btree_prefetch_busy_wait(
  WT_EVICT *evict, WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    WT_UNUSED(btree);
}

/*
 * __wt_evict_noop_btree_get_evict_ref --
 *     Noop btree get evict ref: no evict ref.
 */
WT_REF *
__wt_evict_noop_btree_get_evict_ref(WT_EVICT *evict, WT_SESSION_IMPL *session)
{
    WT_UNUSED(evict);
    WT_UNUSED(session);
    return (NULL);
}
