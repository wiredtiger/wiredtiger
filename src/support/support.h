#pragma once

extern WT_HAZARD *__wt_hazard_check(WT_SESSION_IMPL *session, WT_REF *ref,
  WT_SESSION_IMPL **sessionp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_gen_active(WT_SESSION_IMPL *session, int which, uint64_t generation)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_hazard_check_assert(WT_SESSION_IMPL *session, void *ref, bool waitfor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_ispo2(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_modify_idempotent(const void *modify)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_rwlock_islocked(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern char *__wt_time_aggregate_to_string(WT_TIME_AGGREGATE *ta, char *ta_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern char *__wt_time_point_to_string(wt_timestamp_t ts, wt_timestamp_t durable_ts,
  uint64_t txn_id, char *tp_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern char *__wt_time_window_to_string(WT_TIME_WINDOW *tw, char *tw_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern char *__wt_timestamp_to_string(wt_timestamp_t ts, char *ts_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_buf_set_printable(WT_SESSION_IMPL *session, const void *p, size_t size,
  bool hexonly, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_buf_set_printable_format(WT_SESSION_IMPL *session, const void *buffer,
  size_t size, const char *format, bool hexonly, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_buf_set_size(WT_SESSION_IMPL *session, uint64_t size, bool exact,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_ext_strerror(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bad_object_type(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_buf_catfmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_buf_fmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_buf_grow_worker(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cond_auto_alloc(WT_SESSION_IMPL *session, const char *name, uint64_t min,
  uint64_t max, WT_CONDVAR **condp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_decrypt(WT_SESSION_IMPL *session, WT_ENCRYPTOR *encryptor, size_t skip, WT_ITEM *in,
  WT_ITEM *out) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_encrypt(WT_SESSION_IMPL *session, WT_KEYED_ENCRYPTOR *kencryptor, size_t skip,
  WT_ITEM *in, WT_ITEM *out) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_esc_hex_to_raw(WT_SESSION_IMPL *session, const char *from, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_err_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_msg_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_spin_init(WT_EXTENSION_API *wt_api, WT_EXTENSION_SPINLOCK *ext_spinlock,
  const char *name) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hazard_clear(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hazard_set_func(WT_SESSION_IMPL *session, WT_REF *ref, bool *busyp
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hex2byte(const u_char *from, u_char *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_hex_to_raw(WT_SESSION_IMPL *session, const char *from, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_inmem_unsupported_op(WT_SESSION_IMPL *session, const char *tag)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_library_init(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_modify_apply_api(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_modify_apply_item(WT_SESSION_IMPL *session, const char *value_format,
  WT_ITEM *value, const void *modify) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_modify_pack(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries, WT_ITEM **modifyp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_modify_reconstruct_from_upd_list(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *modify, WT_UPDATE_VALUE *upd_value, WT_OP_CONTEXT context)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_msg(WT_SESSION_IMPL *session, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 2, 3)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_nhex_to_raw(WT_SESSION_IMPL *session, const char *from, size_t size, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_object_unsupported(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_panic_func(WT_SESSION_IMPL *session, int error, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 6, 7)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_progress(WT_SESSION_IMPL *session, const char *s, uint64_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_raw_to_esc_hex(WT_SESSION_IMPL *session, const uint8_t *from, size_t size,
  WT_ITEM *to) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_raw_to_hex(WT_SESSION_IMPL *session, const uint8_t *from, size_t size, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_rwlock_init(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_scr_alloc_func(WT_SESSION_IMPL *session, size_t size, WT_ITEM **scratchp
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_set_return_func(WT_SESSION_IMPL *session, const char *func, int line, int err,
  const char *strerr) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stash_add(WT_SESSION_IMPL *session, int which, uint64_t generation, void *p,
  size_t len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_connection_desc(WT_CURSOR_STAT *cst, int slot, const char **p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_connection_init(WT_SESSION_IMPL *session, WT_CONNECTION_IMPL *handle)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_dsrc_desc(WT_CURSOR_STAT *cst, int slot, const char **p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_dsrc_init(WT_SESSION_IMPL *session, WT_DATA_HANDLE *handle)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_join_desc(WT_CURSOR_STAT *cst, int slot, const char **p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_stat_session_desc(WT_CURSOR_STAT *cst, int slot, const char **p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_thread_group_create(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group,
  const char *name, uint32_t min, uint32_t max, uint32_t flags,
  bool (*chk_func)(WT_SESSION_IMPL *session),
  int (*run_func)(WT_SESSION_IMPL *session, WT_THREAD *context),
  int (*stop_func)(WT_SESSION_IMPL *session, WT_THREAD *context))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_thread_group_destroy(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_thread_group_resize(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group,
  uint32_t new_min, uint32_t new_max, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_time_aggregate_validate(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta,
  WT_TIME_AGGREGATE *parent, bool silent) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_time_value_validate(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw,
  WT_TIME_AGGREGATE *parent, bool silent) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_try_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_try_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_unexpected_object_type(
  WT_SESSION_IMPL *session, const char *uri, const char *expect) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_update_vector_push(WT_UPDATE_VECTOR *updates, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern u_int __wt_hazard_count(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_log2_int(uint32_t n) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_nlpo2(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_nlpo2_round(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_random(WT_RAND_STATE volatile *rnd_state) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_rduppo2(uint32_t n, uint32_t po2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_gen(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_hash_city64(const void *s, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_hash_fnv64(const void *string, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_session_gen(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void *__wt_ext_scr_alloc(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, size_t size);
extern void __wt_cond_auto_wait(
  WT_SESSION_IMPL *session, WT_CONDVAR *cond, bool progress, bool (*run_func)(WT_SESSION_IMPL *));
extern void __wt_cond_auto_wait_signal(WT_SESSION_IMPL *session, WT_CONDVAR *cond, bool progress,
  bool (*run_func)(WT_SESSION_IMPL *), bool *signalled);
extern void __wt_encrypt_size(
  WT_SESSION_IMPL *session, WT_KEYED_ENCRYPTOR *kencryptor, size_t incoming_size, size_t *sizep);
extern void __wt_err_func(WT_SESSION_IMPL *session, int error, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 6, 7)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern void __wt_errx_func(WT_SESSION_IMPL *session, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 5, 6)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern void __wt_event_handler_set(WT_SESSION_IMPL *session, WT_EVENT_HANDLER *handler);
extern void __wt_ext_scr_free(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, void *p);
extern void __wt_ext_spin_destroy(WT_EXTENSION_API *wt_api, WT_EXTENSION_SPINLOCK *ext_spinlock);
extern void __wt_ext_spin_lock(
  WT_EXTENSION_API *wt_api, WT_SESSION *session, WT_EXTENSION_SPINLOCK *ext_spinlock);
extern void __wt_ext_spin_unlock(
  WT_EXTENSION_API *wt_api, WT_SESSION *session, WT_EXTENSION_SPINLOCK *ext_spinlock);
extern void __wt_fill_hex(
  const uint8_t *src, size_t src_max, uint8_t *dest, size_t dest_max, size_t *lenp);
extern void __wt_gen_init(WT_SESSION_IMPL *session);
extern void __wt_gen_next(WT_SESSION_IMPL *session, int which, uint64_t *genp);
extern void __wt_gen_next_drain(WT_SESSION_IMPL *session, int which);
extern void __wt_hazard_close(WT_SESSION_IMPL *session);
extern void __wt_random_init(WT_RAND_STATE volatile *rnd_state)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern void __wt_random_init_custom_seed(WT_RAND_STATE volatile *rnd_state, uint64_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern void __wt_random_init_seed(WT_SESSION_IMPL *session, WT_RAND_STATE volatile *rnd_state)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern void __wt_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l);
extern void __wt_readunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l);
extern void __wt_rwlock_destroy(WT_SESSION_IMPL *session, WT_RWLOCK *l);
extern void __wt_scr_discard(WT_SESSION_IMPL *session);
extern void __wt_session_gen_enter(WT_SESSION_IMPL *session, int which);
extern void __wt_session_gen_leave(WT_SESSION_IMPL *session, int which);
extern void __wt_stash_discard(WT_SESSION_IMPL *session);
extern void __wt_stash_discard_all(WT_SESSION_IMPL *session_safe, WT_SESSION_IMPL *session);
extern void __wt_stat_connection_aggregate(WT_CONNECTION_STATS **from, WT_CONNECTION_STATS *to);
extern void __wt_stat_connection_clear_all(WT_CONNECTION_STATS **stats);
extern void __wt_stat_connection_clear_single(WT_CONNECTION_STATS *stats);
extern void __wt_stat_connection_discard(WT_SESSION_IMPL *session, WT_CONNECTION_IMPL *handle);
extern void __wt_stat_connection_init_single(WT_CONNECTION_STATS *stats);
extern void __wt_stat_dsrc_aggregate(WT_DSRC_STATS **from, WT_DSRC_STATS *to);
extern void __wt_stat_dsrc_aggregate_single(WT_DSRC_STATS *from, WT_DSRC_STATS *to);
extern void __wt_stat_dsrc_clear_all(WT_DSRC_STATS **stats);
extern void __wt_stat_dsrc_clear_single(WT_DSRC_STATS *stats);
extern void __wt_stat_dsrc_discard(WT_SESSION_IMPL *session, WT_DATA_HANDLE *handle);
extern void __wt_stat_dsrc_init_single(WT_DSRC_STATS *stats);
extern void __wt_stat_join_aggregate(WT_JOIN_STATS **from, WT_JOIN_STATS *to);
extern void __wt_stat_join_clear_all(WT_JOIN_STATS **stats);
extern void __wt_stat_join_clear_single(WT_JOIN_STATS *stats);
extern void __wt_stat_join_init_single(WT_JOIN_STATS *stats);
extern void __wt_stat_session_clear_single(WT_SESSION_STATS *stats);
extern void __wt_stat_session_init_single(WT_SESSION_STATS *stats);
extern void __wt_thread_group_start_one(
  WT_SESSION_IMPL *session, WT_THREAD_GROUP *group, bool is_locked);
extern void __wt_thread_group_stop_one(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group);
extern void __wt_timestamp_to_hex_string(wt_timestamp_t ts, char *hex_timestamp);
extern void __wt_update_vector_clear(WT_UPDATE_VECTOR *updates);
extern void __wt_update_vector_free(WT_UPDATE_VECTOR *updates);
extern void __wt_update_vector_init(WT_SESSION_IMPL *session, WT_UPDATE_VECTOR *updates);
extern void __wt_update_vector_peek(WT_UPDATE_VECTOR *updates, WT_UPDATE **updp);
extern void __wt_update_vector_pop(WT_UPDATE_VECTOR *updates, WT_UPDATE **updp);
extern void __wt_verbose_timestamp(WT_SESSION_IMPL *session, wt_timestamp_t ts, const char *msg);
extern void __wt_verbose_worker(WT_SESSION_IMPL *session, WT_VERBOSE_CATEGORY category,
  WT_VERBOSE_LEVEL level, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold));
extern void __wt_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l);
extern void __wt_writeunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l);

#ifdef HAVE_UNITTEST

#endif
