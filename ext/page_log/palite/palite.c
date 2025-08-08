/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <gcc.h>
#include <swap.h> /* for __wt_bswap64 */
#include "queue.h"

#include "palite_kv.h"
#include "palite_verbose.h"

/*
 * This page log implementation is used for demonstration and testing. All objects are stored as
 * local files in a designated directory.
 */

#ifdef __GNUC__
#if __GNUC__ > 7 || (__GNUC__ == 7 && __GNUC_MINOR__ > 0)
/*
 * !!!
 * GCC with -Wformat-truncation complains about calls to snprintf in this file.
 * There's nothing wrong, this makes the warning go away.
 */
#pragma GCC diagnostic ignored "-Wformat-truncation"
#endif
#endif

#define PALITE_KV_RET(palite, session, r)                                                              \
    {                                                                                              \
        int _ret = (r);                                                                            \
        if (_ret != 0)                                                                             \
            return (                                                                               \
              palite_kv_err(palite, session, _ret, "%s: %d: \"%s\": failed", __FILE__, __LINE__, #r)); \
    }

#define PALITE_KV_ERR(palite, session, r)                                                            \
    {                                                                                            \
        ret = (r);                                                                               \
        if (ret != 0) {                                                                          \
            ret =                                                                                \
              palite_kv_err(palite, session, ret, "%s: %d: \"%s\": failed", __FILE__, __LINE__, #r); \
            goto err;                                                                            \
        }                                                                                        \
    }

#define PALITE_KV_DONE_ERR(palite, session, r)                               \
    {                                                                   \
        ret = (r);                                                      \
        if (ret != SQLITE_DONE) {                                                 \
            ret =                                                       \
              palite_kv_err(palite, session, ret, "%s: %d: \"%s\": failed", __FILE__, __LINE__, #r); \
            goto err;                                                   \
        }                                                               \
        else                                                            \
            ret = 0;                                                    \
    }

#define PALITE_ENCRYPTION_EQUAL(e1, e2) (memcmp((e1).dek, (e2).dek, sizeof((e1).dek)) == 0)
/*
 * The default cache size for SQLite. Instead of changing this here, consider setting
 * cache_size_mb=.... when loading the extension library.
 */
#define DEFAULT_PALITE_CACHE_SIZE_MB 500

/* Directory page log structure. */
typedef struct {
    WT_PAGE_LOG page_log; /* Must come first */

    WT_EXTENSION_API *wt_api; /* Extension API */

    char *kv_home;
    PALITE_KV_ENV *kv_env;

    /* We use random for artificial delays */
    uint32_t rand_w, rand_z;

    /*
     * Locks are used to protect the file handle queue.
     */
    pthread_rwlock_t pl_handle_lock;

    /* The LSN when the KV database is opened, used to check encryption. */
    uint64_t begin_lsn;

    /*
     * Keep the number of references to this page log.
     */
    uint32_t reference_count;

    uint32_t cache_size_mb;            /* Size of cache in megabytes */
    uint32_t delay_ms;                 /* Average length of delay when simulated */
    uint32_t error_ms;                 /* Average length of sleep when simulated */
    uint32_t force_delay;              /* Force a simulated network delay every N operations */
    uint32_t force_error;              /* Force a simulated network error every N operations */
    uint32_t materialization_delay_ms; /* Average length of materialization delay */
    uint64_t last_materialized_lsn;    /* The last materialized LSN (0 if not set) */
    uint32_t verbose;                  /* Verbose level */
    bool verbose_msg;                  /* Send verbose messages to msg callback interface */

    /*
     * Statistics are collected but not yet exposed.
     */
    uint64_t object_puts; /* (What would be) network writes */
    uint64_t object_gets; /* (What would be) network requests for data */

    /* Queue of file handles */
    TAILQ_HEAD(palite_handle_qh, palite_handle) fileq;

} PALITE;

typedef struct palite_handle {
    WT_PAGE_LOG_HANDLE iface; /* Must come first */

    PALITE *palite; /* Enclosing PALITE  */
    uint64_t table_id;

    TAILQ_ENTRY(palite_handle) q; /* Queue of handles */
} PALITE_HANDLE;

/*
 * Forward function declarations for internal functions
 */
static int palite_configure(PALITE *, WT_CONFIG_ARG *);
static int palite_configure_bool(PALITE *, WT_CONFIG_PARSER *, WT_CONFIG_ARG *, const char *, bool *);
static int palite_configure_int(
  PALITE *, WT_CONFIG_PARSER *, WT_CONFIG_ARG *, const char *, uint32_t *);
static int palite_err(PALITE *, WT_SESSION *, int, const char *, ...);
static int palite_kv_err(PALITE *, WT_SESSION *, int, const char *, ...);
static int palite_get_dek(PALITE *, WT_SESSION *, const WT_PAGE_LOG_ENCRYPTION *, uint64_t, uint64_t,
  bool, uint64_t, WT_PAGE_LOG_ENCRYPTION *);
static void palite_init_context(PALITE *, PALITE_KV_CONTEXT *);
static int palite_init_lsn(PALITE *);

/*
 * Forward function declarations for page log API implementation
 */
static int palite_add_reference(WT_PAGE_LOG *);
static int palite_terminate(WT_PAGE_LOG *, WT_SESSION *);

/*
 * palite_configure
 *     Parse the configuration for the keys we care about.
 */
static int
palite_configure(PALITE *palite, WT_CONFIG_ARG *config)
{
    WT_CONFIG_PARSER *env_parser;
    const char *env_config;
    int ret, t_ret;

    if ((env_config = getenv("WT_PALITE_CONFIG")) == NULL)
        env_config = "";

    /* A null session is allowed. */
    if ((ret = palite->wt_api->config_parser_open(
           palite->wt_api, NULL, env_config, strlen(env_config), &env_parser)) != 0)
        goto err;

    palite->cache_size_mb = DEFAULT_PALITE_CACHE_SIZE_MB;
    if ((ret = palite_configure_int(
           palite, env_parser, config, "cache_size_mb", &palite->cache_size_mb)) != 0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "delay_ms", &palite->delay_ms)) != 0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "error_ms", &palite->error_ms)) != 0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "force_delay", &palite->force_delay)) !=
      0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "force_error", &palite->force_error)) !=
      0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "materialization_delay_ms",
           &palite->materialization_delay_ms)) != 0)
        goto err;
    if ((ret = palite_configure_int(palite, env_parser, config, "verbose", &palite->verbose)) != 0)
        goto err;
    if ((ret = palite_configure_bool(palite, env_parser, config, "verbose_msg", &palite->verbose_msg)) !=
      0)
        goto err;
 
err:
    if (env_parser != NULL) {
        t_ret = env_parser->close(env_parser);
        if (ret == 0)
            ret = t_ret;
    }
    return (ret);
}

/*
 * palite_configure_bool
 *     Look for a particular configuration key, and return its boolean value.
 */
static int
palite_configure_bool(
    PALITE *palite, WT_CONFIG_PARSER *env_parser, WT_CONFIG_ARG *config, const char *key, bool *valuep)
{
    WT_CONFIG_ITEM v;
    int ret;

    ret = 0;

    /*
     * Environment configuration overrides configuration used with loading the library, so check
     * that first.
     */
    if ((ret = env_parser->get(env_parser, key, &v)) == 0 ||
      (ret = palite->wt_api->config_get(palite->wt_api, NULL, config, key, &v)) == 0) {
        if (v.len == 0 || (v.type != WT_CONFIG_ITEM_NUM && v.type != WT_CONFIG_ITEM_BOOL))
            ret = palite_err(palite, NULL, EINVAL, "force_error config arg: bool required");
        else
            *valuep = (v.val != 0);
    } else if (ret == WT_NOTFOUND)
        ret = 0;
    else
        ret = palite_err(palite, NULL, EINVAL, "WT_API->config_get");

    return (ret);
}

/*
 * palite_configure_int
 *     Look for a particular configuration key, and return its integer value.
 */
static int
palite_configure_int(PALITE *palite, WT_CONFIG_PARSER *env_parser, WT_CONFIG_ARG *config, const char *key,
  uint32_t *valuep)
{
    WT_CONFIG_ITEM v;
    int ret;

    ret = 0;

    /*
     * Environment configuration overrides configuration used with loading the library, so check
     * that first.
     */
    if ((ret = env_parser->get(env_parser, key, &v)) == 0 ||
      (ret = palite->wt_api->config_get(palite->wt_api, NULL, config, key, &v)) == 0) {
        if (v.len == 0 || v.type != WT_CONFIG_ITEM_NUM)
            ret = palite_err(palite, NULL, EINVAL, "force_error config arg: integer required");
        else
            *valuep = (uint32_t)v.val;
    } else if (ret == WT_NOTFOUND)
        ret = 0;
    else
        ret = palite_err(palite, NULL, EINVAL, "WT_API->config_get");

    return (ret);
}

/*
 * sleep_us --
 *     Sleep for the specified microseconds.
 */
static void
sleep_us(uint64_t us)
{
    struct timeval tv;

    /* Cast needed for some compilers that suspect the calculation can overflow (it can't). */
    tv.tv_sec = (time_t)(us / WT_MILLION);
    tv.tv_usec = (suseconds_t)(us % WT_MILLION);
    (void)select(0, NULL, NULL, NULL, &tv);
}

/*
 * palite_compute_delay_us --
 *     Compute a random delay around a given average. Use a uniform random distribution from 0.5 of
 *     the given delay to 1.5 of the given delay.
 */
static uint64_t
palite_compute_delay_us(PALITE *palite, uint64_t avg_delay_us)
{
    uint32_t w, z, r;
    if (avg_delay_us == 0)
        return (0);

    /*
     * Note: this is WiredTiger's RNG algorithm. Since this module is packaged independent of
     * WiredTiger's internals, it's not feasible to call directly into its implementation.
     */
    w = palite->rand_w;
    z = palite->rand_z;
    if (w == 0 || z == 0) {
        palite->rand_w = w = 521288629;
        palite->rand_z = z = 362436069;
    }
    palite->rand_z = (36969 * (z & 65535) + (z >> 16)) & 0xffffffff;
    palite->rand_w = (18000 * (w & 65535) + (w >> 16)) & 0xffffffff;
    r = ((z << 16) + (w & 65535)) & 0xffffffff;

    return (avg_delay_us / 2 + r % avg_delay_us);
}

/*
 * palite_delay --
 *     Add any artificial delay or simulated network error during an object transfer.
 */
static int
palite_delay(PALITE *palite, WT_SESSION *session)
{
    int ret;
    uint64_t us;

    ret = 0;
    if (palite->force_delay != 0 &&
      (palite->object_gets + palite->object_puts) % palite->force_delay == 0) {
        us = palite_compute_delay_us(palite, (uint64_t)palite->delay_ms * WT_THOUSAND);
        PALITE_VERBOSE_PRINT(palite, session,
          "Artificial delay %" PRIu64 " microseconds after %" PRIu64 " object reads, %" PRIu64
          " object writes\n",
          us, palite->object_gets, palite->object_puts);
        sleep_us(us);
    }
    if (palite->force_error != 0 &&
      (palite->object_gets + palite->object_puts) % palite->force_error == 0) {
        us = palite_compute_delay_us(palite, (uint64_t)palite->error_ms * WT_THOUSAND);
        PALITE_VERBOSE_PRINT(palite, session,
          "Artificial error returned after %" PRIu64 " microseconds sleep, %" PRIu64
          " object reads, %" PRIu64 " object writes\n",
          us, palite->object_gets, palite->object_puts);
        sleep_us(us);
        ret = ENETUNREACH;
    }

    return (ret);
}

/*
 * palite_err --
 *     Print errors from the interface. Returns "ret", the third argument.
 */
static int
palite_err(PALITE *palite, WT_SESSION *session, int ret, const char *format, ...)
{
    va_list ap;
    WT_EXTENSION_API *wt_api;
    char buf[1000];

    va_start(ap, format);
    wt_api = palite->wt_api;
    if (vsnprintf(buf, sizeof(buf), format, ap) >= (int)sizeof(buf))
        wt_api->err_printf(wt_api, session, "palite: error overflow");
    wt_api->err_printf(
      wt_api, session, "palite: %s: %s", wt_api->strerror(wt_api, session, ret), buf);
    va_end(ap);

    return (ret);
}

/*
 * palite_kv_err --
 *     Print errors from the interface. Returns "ret", the third argument.
 */
static int
palite_kv_err(PALITE *palite, WT_SESSION *session, int ret, const char *format, ...)
{
    va_list ap;
    WT_EXTENSION_API *wt_api;
    char buf[1000];
    const char *sqlite_error;

    va_start(ap, format);
    wt_api = palite->wt_api;
    if (vsnprintf(buf, sizeof(buf), format, ap) >= (int)sizeof(buf))
        wt_api->err_printf(wt_api, session, "palite: error overflow");
    sqlite_error = sqlite3_errstr(ret);
    wt_api->err_printf(wt_api, session, "palite SQLite: %s: %s", sqlite_error, buf);
    PALITE_VERBOSE_PRINT(palite, session, "palite SQLite: %s: %s\n", sqlite_error, buf);
    va_end(ap);

    return (WT_ERROR);
}

/*
 * palite_get_dek --
 *     Check or generate a DEK (encryption key).
 */
static int
palite_get_dek(PALITE *palite, WT_SESSION *session, const WT_PAGE_LOG_ENCRYPTION *encrypt_in,
  uint64_t table_id, uint64_t page_id, bool is_delta, uint64_t base_lsn,
  WT_PAGE_LOG_ENCRYPTION *encrypt_out)
{
    static WT_PAGE_LOG_ENCRYPTION zero_encryption;
    WT_PAGE_LOG_ENCRYPTION tmp;
    bool was_zeroed;

    /*
     * The DEK is an encrypted encryption key. A production implementation of the page log interface
     * would do encryption, using the DEK when it is set. If the DEK is not set, the implementation
     * must figure out what the DEK should be, which may take some time. The DEK is stored with the
     * page, and when the implementation gets a page it knows how to decrypt it. It also passes the
     * DEK to the user of the interface (WiredTiger). That DEK must be kept and used for subsequent
     * deltas to the page. Thus when deltas are written, the DEK doesn't have to be recomputed.
     *
     * Here in PALITE, we don't want to do any encryption. Since the encrypt/decryption would
     * invisible to the calling layer (WiredTiger), having encryption doesn't help test WiredTiger
     * at all. Also, it gets in the way of efficient debugging.
     *
     * However, we do want to test that WiredTiger is passing along the DEK whenever it can and
     * should. If it stopped doing so, the production page log would need to determine the DEK for
     * itself more often, and we might not notice the error.
     *
     * So WiredTiger receives a DEK with every page get. When writing a delta for such a page, it
     * needs to pass that DEK. One the other hand, when writing a delta for page that WiredTiger
     * generated and wrote during the current connection, it uses a zeroed DEK, that's the best it
     * can do.
     *
     * To test this without doing any extra KV requests, we generate and store a DEK for any page
     * write that doesn't already have it - a simple encoding of the table id and page id. Then,
     * we'd expect that if a DEK is ever passed to us in the put path, it must match that simple
     * encoding. That tests that the correct DEK is being passed.
     *
     * To test that we're passing a DEK when we should, we compare the base_lsn to the LSN we
     * started the run with. If the base_lsn is less than that, then WiredTiger must have previously
     * gotten the page from the page log interface, hence the DEK should be set.
     */
#define PALITE_DEK_FORMAT ("%" PRIu64 ":%" PRIu64)
    tmp = zero_encryption;
    if ((size_t)snprintf(&tmp.dek[0], sizeof(tmp.dek), PALITE_DEK_FORMAT, table_id, page_id) >
      sizeof(tmp.dek))
        assert(false); /* should never overflow */

    was_zeroed = PALITE_ENCRYPTION_EQUAL(*encrypt_in, zero_encryption);
    if (was_zeroed)
        *encrypt_out = tmp;
    else {
        if (!PALITE_ENCRYPTION_EQUAL(*encrypt_in, tmp))
            return (palite_err(palite, session, EINVAL,
              "encryption dek %31s does not match expected value %31s", encrypt_in->dek, tmp.dek));
        PALITE_VERBOSE_PRINT(palite, session, "palite using saved dek: %s\n", encrypt_in->dek);
        *encrypt_out = *encrypt_in;
    }

    if (was_zeroed && is_delta && base_lsn < palite->begin_lsn)
        return (palite_err(palite, session, EINVAL, "expected non-zero encryption dek"));

    return (0);
}

/*
 * palite_resize_item --
 *     Resize a buffer as needed.
 */
static int
palite_resize_item(WT_ITEM *item, size_t new_size)
{
    if (item->memsize < new_size) {
        item->mem = realloc(item->mem, new_size);
        if (item->mem == NULL)
            return (errno);
        item->memsize = new_size;
    }
    item->data = item->mem;
    item->size = new_size;
    return (0);
}

/*
 * palite_init_context --
 *     Initialize a context in a standard way.
 */
static void
palite_init_context(PALITE *palite, PALITE_KV_CONTEXT *context)
{
    memset(context, 0, sizeof(*context));

    /*
     * To get more testing variation, we could call palite_compute_delay_us to randomize this number.
     * If we do so, we need to make sure items are materialized in the same order they are written.
     * So when setting PAGE_KEY.timestamp_materialized_us, we'd need to make each value set was
     * monotonically increasing.
     */
    context->materialization_delay_us = palite->materialization_delay_ms * WT_THOUSAND;
    context->last_materialized_lsn = palite->last_materialized_lsn;
}

/*
 * palite_init_lsn --
 *     Remember the current LSN when we started PALITE.
 */
static int
palite_init_lsn(PALITE *palite)
{
    PALITE_KV_CONTEXT context;
    int ret;

    palite_init_context(palite, &context);
    PALITE_KV_RET(palite, NULL, palite_kv_begin_transaction(&context, palite->kv_env, false));

    /*
     * Get the LSN. If it's never been set, we'll get not found, but that's okay, that will leave
     * our beginning LSN at zero, which is fine for our purposes.
     */
    ret = palite_kv_get_global(&context, PALITE_KV_GLOBAL_LSN, &palite->begin_lsn);
    if (ret == SQLITE_DONE)
        ret = 0;
    palite_kv_rollback_transaction(&context);
    return (ret);
}

/*
 * palite_add_reference --
 *     Add a reference to the page log so we can reference count to know when to really terminate.
 */
static int
palite_add_reference(WT_PAGE_LOG *page_log)
{
    PALITE *palite;

    palite = (PALITE *)page_log;

    /*
     * Missing reference or overflow?
     */
    if (palite->reference_count == 0 || palite->reference_count + 1 == 0)
        return (EINVAL);
    ++palite->reference_count;
    return (0);
}

/*
 * palite_begin_checkpoint --
 *     Begin a checkpoint.
 */
static int
palite_begin_checkpoint(WT_PAGE_LOG *page_log, WT_SESSION *session, uint64_t checkpoint_id)
{
    int ret;
    ret = 0;

    (void)page_log;      /* Unused parameter */
    (void)session;       /* Unused parameter */
    (void)checkpoint_id; /* Unused parameter */

    return (ret);
}

/*
 * palite_complete_checkpoint_ext --
 *     Complete a checkpoint.
 */
static int
palite_complete_checkpoint_ext(WT_PAGE_LOG *page_log, WT_SESSION *session, uint64_t checkpoint_id,
  uint64_t checkpoint_timestamp, const WT_ITEM *checkpoint_metadata, uint64_t *lsnp)
{
    PALITE *palite;
    PALITE_KV_CONTEXT context;
    uint64_t lsn;
    int ret;

    (void)checkpoint_id; /* Unused parameter */

    palite = (PALITE *)page_log;
    palite_init_context(palite, &context);

    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, false));
    ret = palite_kv_get_global(&context, PALITE_KV_GLOBAL_LSN, &lsn);
    if (ret == SQLITE_DONE) {
        lsn = 1;
        ret = 0;
    }
    PALITE_KV_ERR(palite, session, ret);

    PALITE_VERBOSE_PRINT(palite, session, "Write metadata for timestamp %d: %d bytes: %.*s\n",
      (int)checkpoint_timestamp, (int)checkpoint_metadata->size,
      (int)checkpoint_metadata->size, (char *)checkpoint_metadata->data);
    PALITE_KV_DONE_ERR(palite, session,
      palite_kv_put_checkpoint(
        &context, lsn, checkpoint_timestamp, checkpoint_metadata));
    PALITE_KV_DONE_ERR(palite, session, palite_kv_put_global(&context, PALITE_KV_GLOBAL_LSN, lsn + 1));
    PALITE_KV_ERR(palite, session, palite_kv_commit_transaction(&context));

    if (lsnp != NULL)
        *lsnp = lsn;
    return (0);

err:
    palite_kv_rollback_transaction(&context);
    return (ret);
}

/*
 * palite_get_complete_checkpoint_ext --
 *     Get information about the most recently completed checkpoint.
 */
static int
palite_get_complete_checkpoint_ext(WT_PAGE_LOG *page_log, WT_SESSION *session,
  uint64_t *checkpoint_lsn, uint64_t *checkpoint_id, uint64_t *checkpoint_timestamp,
  WT_ITEM *checkpoint_metadata)
{
    PALITE *palite;
    PALITE_KV_CONTEXT context;
    const void *metadata;
    size_t metadata_len;
    int ret;

    (void)checkpoint_id; /* Unused parameter */

    metadata = NULL;
    metadata_len = 0;
    if (checkpoint_lsn != NULL)
        *checkpoint_lsn = 0;
    if (checkpoint_timestamp != NULL)
        *checkpoint_timestamp = 0;
    if (checkpoint_metadata != NULL)
        memset(checkpoint_metadata, 0, sizeof(WT_ITEM));

    palite = (PALITE *)page_log;
    palite_init_context(palite, &context);

    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, true));

    ret = palite_kv_get_last_checkpoint(
      &context, checkpoint_lsn, checkpoint_timestamp, &metadata, &metadata_len);
    if (ret == SQLITE_DONE) {
        ret = WT_NOTFOUND;
        goto err;
    }
    PALITE_KV_ERR(palite, session, ret);
    PALITE_KV_ERR(palite, session, palite_resize_item(checkpoint_metadata, metadata_len));
    if (checkpoint_metadata != NULL) {
        PALITE_VERBOSE_PRINT(palite, session, "Read metadata for timestamp %d: %d bytes: %.*s\n", (int)*checkpoint_timestamp, (int)metadata_len, (int)metadata_len, (char *)metadata);
        PALITE_KV_ERR(palite, session, palite_resize_item(checkpoint_metadata, metadata_len));
        memcpy(checkpoint_metadata->mem, metadata, metadata_len);
    }

    PALITE_KV_ERR(palite, session, palite_kv_commit_transaction(&context));
    return (0);

err:
    palite_kv_rollback_transaction(&context);
    return (ret);
}

/*
 * palite_get_last_lsn --
 *     Get the last LSN.
 */
static int
palite_get_last_lsn(WT_PAGE_LOG *page_log, WT_SESSION *session, uint64_t *lsn)
{
    PALITE *palite;
    PALITE_KV_CONTEXT context;
    uint64_t kv_lsn;
    int ret;

    *lsn = 0;

    palite = (PALITE *)page_log;
    palite_init_context(palite, &context);

    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, true));
    PALITE_KV_ERR(palite, session, palite_kv_get_global(&context, PALITE_KV_GLOBAL_LSN, &kv_lsn));
    PALITE_KV_ERR(palite, session, palite_kv_commit_transaction(&context));

    *lsn = kv_lsn > 0 ? kv_lsn - 1 : 0;

    return (0);

err:
    palite_kv_rollback_transaction(&context);
    return (ret);
}

/*
 * palite_handle_discard --
 *     Discard a page.
 */
static int
palite_handle_discard(WT_PAGE_LOG_HANDLE *plh, WT_SESSION *session, uint64_t page_id,
  uint64_t checkpoint_id, WT_PAGE_LOG_DISCARD_ARGS *discard_args)
{
    static WT_PAGE_LOG_ENCRYPTION zero_encryption;
    PALITE_KV_CONTEXT context;
    WT_ITEM *tombstone = NULL;
    PALITE_HANDLE *palite_handle = (PALITE_HANDLE *)plh;
    PALITE *palite = palite_handle->palite;

    palite_delay(palite, session);
    palite_init_context(palite, &context);

    (void)checkpoint_id; /* Unused parameter */

    /* We always write full pages for tombstones, PALITE has its own flag. */
    bool is_delta = false;
    uint32_t flags = WT_PALITE_KV_TOMBSTONE;

    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, false));
    uint64_t lsn;
    int ret = palite_kv_get_global(&context, PALITE_KV_GLOBAL_LSN, &lsn);
    if (ret == SQLITE_DONE) {
        lsn = 1;
        ret = 0;
    }
    PALITE_KV_ERR(palite, session, ret);

    PALITE_VERBOSE_PRINT(palite_handle->palite, session,
      "palite_handle_discard(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", backlink_lsn=%" PRIu64
      ", base_lsn=%" PRIu64 ")\n",
      (void *)plh, palite_handle->table_id, page_id, discard_args->backlink_lsn,
      discard_args->base_lsn);

    /* There should not be any flag set. */
    assert(discard_args->flags == 0);

    /* Create an empty record as a tombstone. */
    if ((tombstone = calloc(1, sizeof(WT_ITEM))) == NULL)
        return (errno);

    PALITE_KV_DONE_ERR(palite, session,
      palite_kv_put_page(&context, palite_handle->table_id, page_id, lsn, is_delta,
        discard_args->backlink_lsn, discard_args->base_lsn, &zero_encryption, flags, tombstone));
    PALITE_KV_DONE_ERR(palite, session, palite_kv_put_global(&context, PALITE_KV_GLOBAL_LSN, lsn + 1));
     PALITE_KV_ERR(palite, session, palite_kv_commit_transaction(&context));
 
    discard_args->lsn = lsn;
 
    if (0) {
 err:
        palite_kv_rollback_transaction(&context);

        PALITE_VERBOSE_PRINT(palite_handle->palite, session,
          "palite_handle_discard(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64
          ", is_delta=%d) returned %d\n",
          (void *)plh, palite_handle->table_id, page_id, lsn, is_delta, ret);
    }

    free(tombstone);

    return (ret);
}

static int
palite_handle_put(WT_PAGE_LOG_HANDLE *plh, WT_SESSION *session, uint64_t page_id,
  uint64_t checkpoint_id, WT_PAGE_LOG_PUT_ARGS *put_args, const WT_ITEM *buf)
{
    PALITE *palite;
    PALITE_KV_CONTEXT context;
    PALITE_HANDLE *palite_handle;
    uint64_t lsn;
    int ret;
    bool is_delta;
    WT_PAGE_LOG_ENCRYPTION encryption;

    (void)checkpoint_id; /* Unused parameter */

    is_delta = (put_args->flags & WT_PAGE_LOG_DELTA) != 0;
    lsn = 0;
    palite_handle = (PALITE_HANDLE *)plh;
    palite = palite_handle->palite;
    palite_delay(palite, session);

    palite_init_context(palite, &context);

    /* TODO - debug code */
    /*if (palite_handle->table_id == 25 && page_id == 100)*/
        context.sqlite_trace = true;

    /* Check or initialize the encryption field. */
    PALITE_KV_RET(palite, session,
      palite_get_dek(palite, session, &put_args->encryption, palite_handle->table_id, page_id, is_delta,
        put_args->base_lsn, &encryption));

    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, false));
    ret = palite_kv_get_global(&context, PALITE_KV_GLOBAL_LSN, &lsn);
    if (ret == SQLITE_DONE) {
        lsn = 1;
        ret = 0;
    }
    PALITE_KV_ERR(palite, session, ret);

    PALITE_VERBOSE_PRINT(palite_handle->palite, session,
      "palite_handle_put(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64
      ", backlink_lsn=%" PRIu64 ", base_lsn=%" PRIu64 ", is_delta=%d, buf=\n%s)\n",
      (void *)plh, palite_handle->table_id, page_id, lsn, put_args->backlink_lsn, put_args->base_lsn,
      is_delta, palite_verbose_item(buf));

    PALITE_KV_DONE_ERR(palite, session,
      palite_kv_put_page(&context, palite_handle->table_id, page_id, lsn, is_delta,
        put_args->backlink_lsn, put_args->base_lsn, &encryption, put_args->flags, buf));
    PALITE_KV_DONE_ERR(palite, session, palite_kv_put_global(&context, PALITE_KV_GLOBAL_LSN, lsn + 1));
    PALITE_KV_ERR(palite, session, palite_kv_commit_transaction(&context));
    put_args->lsn = lsn;
    return (0);

err:
    palite_kv_rollback_transaction(&context);

    PALITE_VERBOSE_PRINT(palite_handle->palite, session,
      "palite_handle_put(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64
      ", is_delta=%d) returned %d\n",
      (void *)plh, palite_handle->table_id, page_id, lsn, is_delta, ret);
    return (ret);
}

#define PALITE_GET_VERIFY_EQUAL(a, b)                                                                \
    do {                                                                                              \
        if ((a) != (b)) {                                                                          \
            ret = palite_kv_err(palite, session, EINVAL,                                               \
              "%s:%d: Delta chain validation failed at position %" PRIu32                          \
              ": %s != %s. Page details: table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64  \
              ", %s=%" PRIu64 ", %s=%" PRIu64,                                                     \
              __func__, __LINE__, count, #a, #b, palite_handle->table_id, page_id, lsn, #a, (a), #b, \
              (b));                                                                                \
            goto err;                                                                              \
        }                                                                                          \
    } while (0)

static int
palite_handle_get(WT_PAGE_LOG_HANDLE *plh, WT_SESSION *session, uint64_t page_id,
  uint64_t checkpoint_id, WT_PAGE_LOG_GET_ARGS *get_args, WT_ITEM *results_array,
  uint32_t *results_count)
{
    static WT_PAGE_LOG_ENCRYPTION zero_encryption;
    PALITE *palite;
    PALITE_KV_CONTEXT context;
    PALITE_HANDLE *palite_handle;
    PALITE_KV_PAGE_MATCHES matches;
    uint32_t count, i;
    uint64_t last_lsn, lsn;
    int ret;
    bool zeroed_encryption, was_zeroed_encryption;

    (void)checkpoint_id; /* Unused parameter */
 
    count = 0;
    last_lsn = 0;
    lsn = get_args->lsn;
    palite_handle = (PALITE_HANDLE *)plh;
    palite = palite_handle->palite;
    palite_delay(palite, session);

    /* Ensure that regular shared tables use LSNs. */
    assert(palite_handle->table_id == 1 || lsn > 0);

    palite_init_context(palite, &context);

    /*if (palite_handle->table_id == 25 && page_id == 100)*/
        context.sqlite_trace = true;

    PALITE_VERBOSE_PRINT(palite_handle->palite, session,
      "palite_handle_get(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64 ")...\n",
      (void *)plh, palite_handle->table_id, page_id, lsn);
    PALITE_KV_RET(palite, session, palite_kv_begin_transaction(&context, palite->kv_env, false));
    PALITE_KV_ERR(palite, session,
      palite_kv_get_page_matches(&context, palite_handle->table_id, page_id, lsn, &matches));
    get_args->encryption = zero_encryption;
    was_zeroed_encryption = true;
    for (count = 0; count < *results_count; ++count) {
        if (!palite_kv_next_page_match(&matches))
            break;
        PALITE_VERBOSE_PRINT(palite_handle->palite, session,
          "  palite_handle_get iteration %" PRIu32 ", page_id=%" PRIu64 ", lsn=%" PRIu64
          ", is_delta=%d"
          ", backlink_lsn=%" PRIu64
          ", base_lsn=%" PRIu64
          ")\n",
          count, page_id, matches.lsn, (int)matches.is_delta,
          matches.backlink_lsn, matches.base_lsn);
        memset(&results_array[count], 0, sizeof(WT_ITEM));
        PALITE_KV_ERR(palite, session, palite_resize_item(&results_array[count], matches.size));
        memcpy(results_array[count].mem, matches.data, matches.size);

        /* Validate back links. */
        if (count > 0)
            PALITE_GET_VERIFY_EQUAL(matches.backlink_lsn, last_lsn);

        /* Validate base. */
        if (count == 1)
            PALITE_GET_VERIFY_EQUAL(matches.base_lsn, last_lsn);
        else if (count > 1)
            PALITE_GET_VERIFY_EQUAL(matches.base_lsn, get_args->base_lsn);

        /* We should not request a page that is discarded. */
        ret = (matches.flags & WT_PALITE_KV_TOMBSTONE) == 0 ? 0 : EINVAL;
        PALITE_KV_ERR(palite, session, ret);

        last_lsn = matches.lsn;
        get_args->backlink_lsn = matches.backlink_lsn;
        get_args->base_lsn = matches.base_lsn;
        get_args->encryption = matches.encryption;
        zeroed_encryption = PALITE_ENCRYPTION_EQUAL(get_args->encryption, zero_encryption);
        if (zeroed_encryption)
            PALITE_VERBOSE_PRINT(palite, session, "palite got zero dek%s\n", "");
        else
            PALITE_VERBOSE_PRINT(palite, session, "palite got non-zero dek: %s\n", get_args->encryption.dek);
        if (zeroed_encryption && !was_zeroed_encryption) {
            ret = palite_err(palite, session, EINVAL,
              "base dek is not zeroed, delta encryption is zero and should not be");
            goto err;
        }
        get_args->delta_count = count;
    }
    /* Did the caller give us enough output entries to hold all the results? */
    if (count == *results_count && palite_kv_next_page_match(&matches))
        PALITE_KV_ERR(palite, session, ENOMEM);

    *results_count = count;
    PALITE_KV_ERR(palite, session, matches.error);

err:
    palite_kv_rollback_transaction(&context);
    PALITE_VERBOSE_PRINT(palite_handle->palite, session,
      "palite_handle_get(plh=%p, table_id=%" PRIu64 ", page_id=%" PRIu64 ", lsn=%" PRIu64
      ") returns %d (in %d parts)\n",
      (void *)plh, palite_handle->table_id, page_id, lsn, ret, (int)count);
    if (ret == 0) {
        for (i = 0; i < count; ++i)
            PALITE_VERBOSE_PRINT(
              palite_handle->palite, session, "   part %d: %s\n", (int)i, palite_verbose_item(&results_array[i]));
        PALITE_VERBOSE_PRINT(palite_handle->palite, session,
          "   metadata: backlink_lsn=%" PRIu64 ", base_lsn=%" PRIu64 "\n",
          get_args->backlink_lsn, get_args->base_lsn);
    }
    return (ret);
}

/*
 * palite_handle_close_internal --
 *     Internal file handle close.
 */
static int
palite_handle_close_internal(PALITE *palite, PALITE_HANDLE *palite_handle)
{
    int ret;
    WT_PAGE_LOG_HANDLE *plh;

    ret = 0;
    plh = (WT_PAGE_LOG_HANDLE *)palite_handle;

    (void)palite;
    (void)plh;
    /* TODO: placeholder for more actions */

    free(palite_handle);

    return (ret);
}

static int
palite_handle_close(WT_PAGE_LOG_HANDLE *plh, WT_SESSION *session)
{
    PALITE_HANDLE *palite_handle;

    (void)session;

    palite_handle = (PALITE_HANDLE *)plh;
    return (palite_handle_close_internal(palite_handle->palite, palite_handle));
}

/*
 * palite_open_handle --
 *     Open a handle for further operations on a table.
 */
static int
palite_open_handle(
  WT_PAGE_LOG *page_log, WT_SESSION *session, uint64_t table_id, WT_PAGE_LOG_HANDLE **plh)
{
    PALITE *palite;
    PALITE_HANDLE *palite_handle;

    (void)session;

    palite = (PALITE *)page_log;
    if ((palite_handle = calloc(1, sizeof(PALITE_HANDLE))) == NULL)
        return (errno);
    palite_handle->iface.page_log = page_log;
    palite_handle->iface.plh_discard = palite_handle_discard;
    palite_handle->iface.plh_put = palite_handle_put;
    palite_handle->iface.plh_get = palite_handle_get;
    palite_handle->iface.plh_close = palite_handle_close;
    palite_handle->palite = palite;
    palite_handle->table_id = table_id;

    *plh = &palite_handle->iface;

    return (0);
}

/*
 * palite_set_last_materialized_lsn --
 *     Set the last materialized LSN for testing purposes.
 */
static int
palite_set_last_materialized_lsn(WT_PAGE_LOG *storage, WT_SESSION *session, uint64_t lsn)
{
    PALITE *palite;

    (void)session;

    palite = (PALITE *)storage;
    palite->last_materialized_lsn = lsn;

    return (0);
}

/*
 * palite_terminate --
 *     Discard any resources on termination
 */
static int
palite_terminate(WT_PAGE_LOG *storage, WT_SESSION *session)
{
    PALITE_HANDLE *palite_handle, *safe_handle;
    PALITE *palite;
    int ret;

    ret = 0;
    palite = (PALITE *)storage;

    if (--palite->reference_count != 0)
        return (0);

    /*
     * We should be single threaded at this point, so it is safe to destroy the lock and access the
     * file handle list without locking it.
     */
    if ((ret = pthread_rwlock_destroy(&palite->pl_handle_lock)) != 0)
        (void)palite_err(palite, session, ret, "terminate: pthread_rwlock_destroy");

    TAILQ_FOREACH_SAFE(palite_handle, &palite->fileq, q, safe_handle)
    palite_handle_close_internal(palite, palite_handle);

    if (palite->kv_env != NULL)
        palite_kv_env_close(palite->kv_env);
    if (palite->kv_home != NULL)
        free(palite->kv_home);
    free(palite);

    return (ret);
}

int palite_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config);

/*
 * palite_extension_init --
 *     A standalone, durable implementation of the WT_PAGE_LOG interface (PALI).
 */
int
palite_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    PALITE *palite;
    const char *home;
    char *t, *tmp_buf;
    size_t len;
    int ret;

    tmp_buf = NULL;
    if ((palite = calloc(1, sizeof(PALITE))) == NULL)
        return (errno);
    palite->wt_api = connection->get_extension_api(connection);
    if ((ret = pthread_rwlock_init(&palite->pl_handle_lock, NULL)) != 0) {
        (void)palite_err(palite, NULL, ret, "pthread_rwlock_init");
        free(palite);
        return (ret);
    }

    /*
     * Allocate a palite storage structure, with a WT_STORAGE structure as the first field, allowing
     * us to treat references to either type of structure as a reference to the other type.
     */
    palite->page_log.pl_add_reference = palite_add_reference;
    palite->page_log.pl_begin_checkpoint = palite_begin_checkpoint;
    palite->page_log.pl_complete_checkpoint = NULL;
    palite->page_log.pl_complete_checkpoint_ext = palite_complete_checkpoint_ext;
    palite->page_log.pl_get_complete_checkpoint = NULL;
    palite->page_log.pl_get_complete_checkpoint_ext = palite_get_complete_checkpoint_ext;
    palite->page_log.pl_get_last_lsn = palite_get_last_lsn;
    palite->page_log.pl_get_open_checkpoint = NULL;
    palite->page_log.pl_open_handle = palite_open_handle;
    palite->page_log.pl_set_last_materialized_lsn = palite_set_last_materialized_lsn;
    palite->page_log.terminate = palite_terminate;

    /*
     * The first reference is implied by the call to add_page_log.
     */
    palite->reference_count = 1;

    if ((ret = palite_configure(palite, config)) != 0)
        goto err;

    /* Load the storage */
    PALITE_KV_ERR(palite, NULL, connection->add_page_log(connection, "palite", &palite->page_log, NULL));
    PALITE_KV_ERR(palite, NULL, palite_kv_env_create(&palite->kv_env, palite->cache_size_mb));

    /* Build the SQLite home string. */
    home = connection->get_home(connection);
    len = strlen(home) + 20;
    palite->kv_home = malloc(len);
    if (palite->kv_home == NULL) {
        ret = palite_err(palite, NULL, errno, "malloc");
        goto err;
    }
    strncpy(palite->kv_home, home, len);
    strncat(palite->kv_home, "/kv_home", len);

    tmp_buf = malloc(len + 100);
    if (readlink(palite->kv_home, tmp_buf, len + 100) == 0) {
        t = palite->kv_home;
        palite->kv_home = tmp_buf;
        tmp_buf = t;
    }

    /* Create the SQLite home, or if it exists, use what is already there. */
    ret = mkdir(palite->kv_home, 0777);
    if (ret != 0) {
        ret = errno;
        if (ret == EEXIST)
            ret = 0;
        else {
            ret = palite_err(palite, NULL, ret, "mkdir");
            goto err;
        }
    }

    /* Open the SQLite environment. */
    PALITE_KV_ERR(palite, NULL, palite_kv_env_open(palite->kv_env, palite->kv_home));

    if ((ret = palite_init_lsn(palite)) != 0)
        goto err;

err:
    free(tmp_buf);
    if (ret != 0) {
        if (palite->kv_env != NULL)
            palite_kv_env_close(palite->kv_env);
        if (palite->kv_home != NULL)
            free(palite->kv_home);
        free(palite);
    }
    return (ret);
}

/*
 * We have to remove this symbol when building as a builtin extension otherwise it will conflict
 * with other builtin libraries.
 */
#ifndef HAVE_BUILTIN_EXTENSION_PALITE
/*
 * wiredtiger_extension_init --
 *     WiredTiger page and log mock extension.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    return palite_extension_init(connection, config);
}
#endif
