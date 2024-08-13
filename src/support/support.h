#pragma once

/*
 * __wt_bad_object_type --
 *     Print a standard error message when given an unknown or unsupported object type.
 */
extern int __wt_bad_object_type(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_catfmt --
 *     Grow a buffer to append a formatted string.
 */
extern int __wt_buf_catfmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_fmt --
 *     Grow a buffer to accommodate a formatted string.
 */
extern int __wt_buf_fmt(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_grow_worker --
 *     Grow a buffer that may be in-use, and ensure that all data is local to the buffer.
 */
extern int __wt_buf_grow_worker(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_set_printable --
 *     Set the contents of the buffer to a printable representation of a byte string.
 */
extern const char *__wt_buf_set_printable(WT_SESSION_IMPL *session, const void *p, size_t size,
  bool hexonly, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_set_printable_format --
 *     Set the contents of the buffer to a printable representation of a byte string, based on a
 *     format.
 */
extern const char *__wt_buf_set_printable_format(WT_SESSION_IMPL *session, const void *buffer,
  size_t size, const char *format, bool hexonly, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_buf_set_size --
 *     Set the contents of the buffer to a printable representation of a byte size.
 */
extern const char *__wt_buf_set_size(WT_SESSION_IMPL *session, uint64_t size, bool exact,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_auto_alloc --
 *     Allocate and initialize an automatically adjusting condition variable.
 */
extern int __wt_cond_auto_alloc(WT_SESSION_IMPL *session, const char *name, uint64_t min,
  uint64_t max, WT_CONDVAR **condp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_auto_wait --
 *     Wait on a mutex, optionally timing out. If we get it before the time out period expires, let
 *     the caller know.
 */
extern void __wt_cond_auto_wait(WT_SESSION_IMPL *session, WT_CONDVAR *cond, bool progress,
  bool (*run_func)(WT_SESSION_IMPL *)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_auto_wait_signal --
 *     Wait on a mutex, optionally timing out. If we get it before the time out period expires, let
 *     the caller know.
 */
extern void __wt_cond_auto_wait_signal(WT_SESSION_IMPL *session, WT_CONDVAR *cond, bool progress,
  bool (*run_func)(WT_SESSION_IMPL *), bool *signalled)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_decrypt --
 *     Common code to decrypt and verify the encrypted data in a WT_ITEM and return the decrypted
 *     buffer.
 */
extern int __wt_decrypt(WT_SESSION_IMPL *session, WT_ENCRYPTOR *encryptor, size_t skip, WT_ITEM *in,
  WT_ITEM *out) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_encrypt --
 *     Common code to encrypt a WT_ITEM and return the encrypted buffer.
 */
extern int __wt_encrypt(WT_SESSION_IMPL *session, WT_KEYED_ENCRYPTOR *kencryptor, size_t skip,
  WT_ITEM *in, WT_ITEM *out) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_encrypt_size --
 *     Return the size needed for the destination buffer.
 */
extern void __wt_encrypt_size(WT_SESSION_IMPL *session, WT_KEYED_ENCRYPTOR *kencryptor,
  size_t incoming_size, size_t *sizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_err_func --
 *     Report an error.
 */
extern void __wt_err_func(WT_SESSION_IMPL *session, int error, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 6, 7)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_errx_func --
 *     Report an error with no error code.
 */
extern void __wt_errx_func(WT_SESSION_IMPL *session, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 5, 6)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_esc_hex_to_raw --
 *     Convert a printable string, encoded in escaped hex, to a chunk of data.
 */
extern int __wt_esc_hex_to_raw(WT_SESSION_IMPL *session, const char *from, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_event_handler_set --
 *     Set an event handler, fill in any NULL methods with the defaults.
 */
extern void __wt_event_handler_set(WT_SESSION_IMPL *session, WT_EVENT_HANDLER *handler)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_err_printf --
 *     Extension API call to print to the error stream.
 */
extern int __wt_ext_err_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_msg_printf --
 *     Extension API call to print to the message stream.
 */
extern int __wt_ext_msg_printf(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, const char *fmt,
  ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_scr_alloc --
 *     Allocate a scratch buffer, and return the memory reference.
 */
extern void *__wt_ext_scr_alloc(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_scr_free --
 *     Free a scratch buffer based on the memory reference.
 */
extern void __wt_ext_scr_free(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, void *p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_spin_destroy --
 *     Destroy the spinlock.
 */
extern void __wt_ext_spin_destroy(WT_EXTENSION_API *wt_api, WT_EXTENSION_SPINLOCK *ext_spinlock)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_spin_init --
 *     Allocate and initialize a spinlock.
 */
extern int __wt_ext_spin_init(WT_EXTENSION_API *wt_api, WT_EXTENSION_SPINLOCK *ext_spinlock,
  const char *name) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_spin_lock --
 *     Lock the spinlock.
 */
extern void __wt_ext_spin_lock(WT_EXTENSION_API *wt_api, WT_SESSION *session,
  WT_EXTENSION_SPINLOCK *ext_spinlock) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_spin_unlock --
 *     Unlock the spinlock.
 */
extern void __wt_ext_spin_unlock(WT_EXTENSION_API *wt_api, WT_SESSION *session,
  WT_EXTENSION_SPINLOCK *ext_spinlock) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_strerror --
 *     Extension API call to return an error as a string.
 */
extern const char *__wt_ext_strerror(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session, int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fill_hex --
 *     In-memory conversion of raw bytes to a hexadecimal representation.
 */
extern void __wt_fill_hex(const uint8_t *src, size_t src_max, uint8_t *dest, size_t dest_max,
  size_t *lenp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_gen --
 *     Return the resource's generation.
 */
extern uint64_t __wt_gen(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_gen_active --
 *     Return if a specified generation is in use for the resource.
 */
extern bool __wt_gen_active(WT_SESSION_IMPL *session, int which, uint64_t generation)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_gen_init --
 *     Initialize the connection's generations.
 */
extern void __wt_gen_init(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_gen_next --
 *     Switch the resource to its next generation.
 */
extern void __wt_gen_next(WT_SESSION_IMPL *session, int which, uint64_t *genp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_gen_next_drain --
 *     Switch the resource to its next generation, then wait for it to drain.
 */
extern void __wt_gen_next_drain(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hash_city64 --
 *     WiredTiger wrapper around third party hash implementation.
 */
extern uint64_t __wt_hash_city64(const void *s, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hash_fnv64 --
 *     WiredTiger wrapper around third party hash implementation.
 */
extern uint64_t __wt_hash_fnv64(const void *string, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_check --
 *     Return if there's a hazard pointer to the page in the system.
 */
extern WT_HAZARD *__wt_hazard_check(WT_SESSION_IMPL *session, WT_REF *ref,
  WT_SESSION_IMPL **sessionp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_check_assert --
 *     Assert there's no hazard pointer to the page.
 */
extern bool __wt_hazard_check_assert(WT_SESSION_IMPL *session, void *ref, bool waitfor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_clear --
 *     Clear a hazard pointer.
 */
extern int __wt_hazard_clear(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_close --
 *     Verify that no hazard pointers are set.
 */
extern void __wt_hazard_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_count --
 *     Count how many hazard pointers this session has on the given page.
 */
extern u_int __wt_hazard_count(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hazard_set_func --
 *     Set a hazard pointer.
 */
extern int __wt_hazard_set_func(WT_SESSION_IMPL *session, WT_REF *ref, bool *busyp
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hex2byte --
 *     Convert a pair of hex characters into a byte.
 */
extern int __wt_hex2byte(const u_char *from, u_char *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_hex_to_raw --
 *     Convert a nul-terminated printable hex string to a chunk of data.
 */
extern int __wt_hex_to_raw(WT_SESSION_IMPL *session, const char *from, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_inmem_unsupported_op --
 *     Print a standard error message for an operation that's not supported for in-memory
 *     configurations.
 */
extern int __wt_inmem_unsupported_op(WT_SESSION_IMPL *session, const char *tag)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ispo2 --
 *     Return if a number is a power-of-two.
 */
extern bool __wt_ispo2(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_library_init --
 *     Some things to do, before we do anything else.
 */
extern int __wt_library_init(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_log2_int --
 *     Find the log base 2 of an integer in O(N) operations;
 *     http://graphics.stanford.edu/~seander/bithacks.html
 */
extern uint32_t __wt_log2_int(uint32_t n) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_modify_apply_api --
 *     Apply a single set of WT_MODIFY changes to a buffer, the cursor API interface.
 */
extern int __wt_modify_apply_api(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_modify_apply_item --
 *     Apply a single set of WT_MODIFY changes to a WT_ITEM buffer. This function assumes the size
 *     of the value is larger than or equal to 0 except for the string format which must be larger
 *     than 0.
 */
extern int __wt_modify_apply_item(WT_SESSION_IMPL *session, const char *value_format,
  WT_ITEM *value, const void *modify) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_modify_idempotent --
 *     Check if a modify operation is idempotent.
 */
extern bool __wt_modify_idempotent(const void *modify)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_modify_pack --
 *     Pack a modify structure into a buffer.
 */
extern int __wt_modify_pack(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries, WT_ITEM **modifyp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_modify_reconstruct_from_upd_list --
 *     Takes an in-memory modify and populates an update value with the reconstructed full value.
 */
extern int __wt_modify_reconstruct_from_upd_list(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *modify, WT_UPDATE_VALUE *upd_value, WT_OP_CONTEXT context)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_msg --
 *     Informational message.
 */
extern int __wt_msg(WT_SESSION_IMPL *session, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 2, 3)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_nhex_to_raw --
 *     Convert a printable hex string to a chunk of data.
 */
extern int __wt_nhex_to_raw(WT_SESSION_IMPL *session, const char *from, size_t size, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_nlpo2 --
 *     Return the next largest power-of-two.
 */
extern uint32_t __wt_nlpo2(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_nlpo2_round --
 *     Round up to the next-largest power-of-two for a 32-bit unsigned value. In 12 operations, this
 *     code computes the next highest power of 2 for a 32-bit integer. The result may be expressed
 *     by the formula 1U << (lg(v - 1) + 1). Note that in the edge case where v is 0, it returns 0,
 *     which isn't a power of 2; you might append the expression v += (v == 0) to remedy this if it
 *     matters. It would be faster by 2 operations to use the formula and the log base 2 method that
 *     uses a lookup table, but in some situations, lookup tables are not suitable, so the above
 *     code may be best. (On a Athlon XP 2100+ I've found the above shift-left and then OR code is
 *     as fast as using a single BSR assembly language instruction, which scans in reverse to find
 *     the highest set bit.) It works by copying the highest set bit to all of the lower bits, and
 *     then adding one, which results in carries that set all of the lower bits to 0 and one bit
 *     beyond the highest set bit to 1. If the original number was a power of 2, then the decrement
 *     will reduce it to one less, so that we round up to the same original value. Devised by Sean
 *     Anderson, September 14, 2001. Pete Hart pointed me to a couple newsgroup posts by him and
 *     William Lewis in February of 1997, where they arrive at the same algorithm.
 *     http://graphics.stanford.edu/~seander/bithacks.html Sean Eron Anderson,
 *     seander@cs.stanford.edu
 */
extern uint32_t __wt_nlpo2_round(uint32_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_object_unsupported --
 *     Print a standard error message for an object that doesn't support a particular operation.
 */
extern int __wt_object_unsupported(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_panic_func --
 *     A standard error message when we panic.
 */
extern int __wt_panic_func(WT_SESSION_IMPL *session, int error, const char *func, int line,
  WT_VERBOSE_CATEGORY category, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 6, 7)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
      WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_progress --
 *     Progress message.
 */
extern int __wt_progress(WT_SESSION_IMPL *session, const char *s, uint64_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_random --
 *     Return a 32-bit pseudo-random number.
 */
extern uint32_t __wt_random(WT_RAND_STATE volatile *rnd_state)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_random_init --
 *     Initialize return of a 32-bit pseudo-random number.
 */
extern void __wt_random_init(WT_RAND_STATE volatile *rnd_state)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_random_init_custom_seed --
 *     Initialize the state of a 32-bit pseudo-random number with custom seed.
 */
extern void __wt_random_init_custom_seed(WT_RAND_STATE volatile *rnd_state, uint64_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_random_init_seed --
 *     Initialize the state of a 32-bit pseudo-random number.
 */
extern void __wt_random_init_seed(WT_SESSION_IMPL *session, WT_RAND_STATE volatile *rnd_state)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_raw_to_esc_hex --
 *     Convert a chunk of data to a nul-terminated printable string using escaped hex, as necessary.
 */
extern int __wt_raw_to_esc_hex(WT_SESSION_IMPL *session, const uint8_t *from, size_t size,
  WT_ITEM *to) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_raw_to_hex --
 *     Convert a chunk of data to a nul-terminated printable hex string.
 */
extern int __wt_raw_to_hex(WT_SESSION_IMPL *session, const uint8_t *from, size_t size, WT_ITEM *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rduppo2 --
 *     Round the given int up to the next multiple of N, where N is power of 2.
 */
extern uint32_t __wt_rduppo2(uint32_t n, uint32_t po2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_readlock --
 *     Get a shared lock.
 */
extern void __wt_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_readunlock --
 *     Release a shared lock.
 */
extern void __wt_readunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rwlock_destroy --
 *     Destroy a read/write lock.
 */
extern void __wt_rwlock_destroy(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rwlock_init --
 *     Initialize a read/write lock.
 */
extern int __wt_rwlock_init(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rwlock_islocked --
 *     Return if a read/write lock is currently locked for reading or writing.
 */
extern bool __wt_rwlock_islocked(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_scr_alloc_func --
 *     Scratch buffer allocation function.
 */
extern int __wt_scr_alloc_func(WT_SESSION_IMPL *session, size_t size, WT_ITEM **scratchp
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_scr_discard --
 *     Free all memory associated with the scratch buffers.
 */
extern void __wt_scr_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_gen --
 *     Return the thread's resource generation.
 */
extern uint64_t __wt_session_gen(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_gen_enter --
 *     Publish a thread's resource generation.
 */
extern void __wt_session_gen_enter(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_gen_leave --
 *     Leave a thread's resource generation.
 */
extern void __wt_session_gen_leave(WT_SESSION_IMPL *session, int which)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_set_return_func --
 *     Conditionally log the source of an error code and return the error.
 */
extern int __wt_set_return_func(WT_SESSION_IMPL *session, const char *func, int line, int err,
  const char *strerr) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_stash_add --
 *     Add a new entry into a session stash list.
 */
extern int __wt_stash_add(WT_SESSION_IMPL *session, int which, uint64_t generation, void *p,
  size_t len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_stash_discard --
 *     Discard any memory from a session stash that we can.
 */
extern void __wt_stash_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_stash_discard_all --
 *     Discard all memory from a session's stash.
 */
extern void __wt_stash_discard_all(WT_SESSION_IMPL *session_safe, WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_group_create --
 *     Create a new thread group, assumes incoming group structure is zero initialized.
 */
extern int __wt_thread_group_create(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group,
  const char *name, uint32_t min, uint32_t max, uint32_t flags,
  bool (*chk_func)(WT_SESSION_IMPL *session),
  int (*run_func)(WT_SESSION_IMPL *session, WT_THREAD *context),
  int (*stop_func)(WT_SESSION_IMPL *session, WT_THREAD *context))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_group_destroy --
 *     Shut down a thread group. Our caller must hold the lock.
 */
extern int __wt_thread_group_destroy(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_group_resize --
 *     Resize an array of utility threads taking the lock.
 */
extern int __wt_thread_group_resize(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group,
  uint32_t new_min, uint32_t new_max, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_group_start_one --
 *     Start a new thread if possible.
 */
extern void __wt_thread_group_start_one(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group,
  bool is_locked) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_group_stop_one --
 *     Pause one thread if possible.
 */
extern void __wt_thread_group_stop_one(WT_SESSION_IMPL *session, WT_THREAD_GROUP *group)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_time_aggregate_to_string --
 *     Converts a time aggregate to a standard string representation.
 */
extern char *__wt_time_aggregate_to_string(WT_TIME_AGGREGATE *ta, char *ta_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_time_aggregate_validate --
 *     Aggregated time window validation.
 */
extern int __wt_time_aggregate_validate(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta,
  WT_TIME_AGGREGATE *parent, bool silent) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_time_point_to_string --
 *     Converts a time point to a standard string representation.
 */
extern char *__wt_time_point_to_string(wt_timestamp_t ts, wt_timestamp_t durable_ts,
  uint64_t txn_id, char *tp_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_time_value_validate --
 *     Value time window validation.
 */
extern int __wt_time_value_validate(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw,
  WT_TIME_AGGREGATE *parent, bool silent) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_time_window_to_string --
 *     Converts a time window to a standard string representation.
 */
extern char *__wt_time_window_to_string(WT_TIME_WINDOW *tw, char *tw_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timestamp_to_hex_string --
 *     Convert a timestamp to hex string representation.
 */
extern void __wt_timestamp_to_hex_string(wt_timestamp_t ts, char *hex_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_timestamp_to_string --
 *     Convert a timestamp to the MongoDB string representation.
 */
extern char *__wt_timestamp_to_string(wt_timestamp_t ts, char *ts_string)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_try_readlock --
 *     Try to get a shared lock, fail immediately if unavailable.
 */
extern int __wt_try_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_try_writelock --
 *     Try to get an exclusive lock, fail immediately if unavailable.
 */
extern int __wt_try_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_unexpected_object_type --
 *     Print a standard error message when given an unexpected object type.
 */
extern int __wt_unexpected_object_type(
  WT_SESSION_IMPL *session, const char *uri, const char *expect) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_clear --
 *     Clear a update vector.
 */
extern void __wt_update_vector_clear(WT_UPDATE_VECTOR *updates)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_free --
 *     Free any resources associated with a update vector. If we exceeded the allowed stack space on
 *     the vector and had to fallback to dynamic allocations, we'll be doing a free here.
 */
extern void __wt_update_vector_free(WT_UPDATE_VECTOR *updates)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_init --
 *     Initialize a update vector.
 */
extern void __wt_update_vector_init(WT_SESSION_IMPL *session, WT_UPDATE_VECTOR *updates)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_peek --
 *     Peek an update pointer off a update vector.
 */
extern void __wt_update_vector_peek(WT_UPDATE_VECTOR *updates, WT_UPDATE **updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_pop --
 *     Pop an update pointer off a update vector.
 */
extern void __wt_update_vector_pop(WT_UPDATE_VECTOR *updates, WT_UPDATE **updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_vector_push --
 *     Push a update pointer to a update vector. If we exceed the allowed stack space in the vector,
 *     we'll be doing malloc here.
 */
extern int __wt_update_vector_push(WT_UPDATE_VECTOR *updates, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_timestamp --
 *     Output a verbose message along with the specified timestamp.
 */
extern void __wt_verbose_timestamp(WT_SESSION_IMPL *session, wt_timestamp_t ts, const char *msg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_worker --
 *     Verbose message.
 */
extern void __wt_verbose_worker(WT_SESSION_IMPL *session, WT_VERBOSE_CATEGORY category,
  WT_VERBOSE_LEVEL level, const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_writelock --
 *     Wait to get an exclusive lock.
 */
extern void __wt_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_writeunlock --
 *     Release an exclusive lock.
 */
extern void __wt_writeunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
