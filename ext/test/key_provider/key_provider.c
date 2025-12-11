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

#include "key_provider.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

/* Format specifier for size_t */
#if defined(_MSC_VER) && _MSC_VER < 1900
#define PRIzu "Iu" /* size_t format string for MSVC before VS2015 */
#else
#define PRIzu "zu" /* size_t format string */
#endif

/* Logging macros */
#define LOG_AT(kp, session, level, fmt, ...)                                                   \
    do {                                                                                       \
        if ((kp)->verbose >= (level)) {                                                        \
            ((level) == WT_VERBOSE_ERROR ? (kp)->wtext->err_printf : (kp)->wtext->msg_printf)( \
              (kp)->wtext, (session), "%p, %s: " fmt, (void *)(kp), __func__, ##__VA_ARGS__);  \
        }                                                                                      \
    } while (0)

#define LOG_INFO(kp, session, ...) LOG_AT((kp), (session), WT_VERBOSE_INFO, __VA_ARGS__)
#define LOG_DEBUG(kp, session, ...) LOG_AT((kp), (session), WT_VERBOSE_DEBUG_1, __VA_ARGS__)
#define LOG_ERROR(kp, session, ...) LOG_AT((kp), (session), WT_VERBOSE_ERROR, __VA_ARGS__)

#define CLOCK_SECS(ct) ((double)(ct) / CLOCKS_PER_SEC)

/*
 * A test key provider extension. This extension implements the WT_KEY_PROVIDER interface to provide
 * encryption key management functionality for testing purposes.
 */

/*
 * kp_free_key --
 *     Free the current key stored in the key provider.
 */
static void
kp_free_key(KEY_PROVIDER *kp)
{
    if (kp->state.current_key != NULL)
        free(kp->state.current_key);
    memset(&kp->state, 0, sizeof(kp->state));
}

/*
 * kp_set_key --
 *     Set a new current key in the key provider.
 */
static int
kp_set_key(KEY_PROVIDER *kp, const WT_CRYPT_KEYS *crypt)
{
    kp_free_key(kp);

    kp->state.current_key = malloc(crypt->keys.size);
    if (kp->state.current_key == NULL)
        return (ENOMEM);

    memcpy(kp->state.current_key, crypt->keys.data, crypt->keys.size);
    kp->state.key_size = crypt->keys.size;
    kp->state.current_lsn = crypt->r.lsn;

    kp->state.key_time = clock();

    return (0);
}

/*
 * kp_load_key --
 *     Loads the current persisted key during checkpoint load. This is called by WiredTiger when
 *     loading a checkpoint to retrieve the key that was used when that checkpoint was created.
 */
static int
kp_load_key(WT_KEY_PROVIDER *wtkp, WT_SESSION *session, const WT_CRYPT_KEYS *crypt)
{
    KEY_PROVIDER *kp = (KEY_PROVIDER *)wtkp;
    LOG_DEBUG(kp, session, "Current key: LSN=%" PRIu64 ", key_time=%.2f, size=%" PRIzu,
      kp->state.current_lsn, CLOCK_SECS(kp->state.key_time), kp->state.key_size);

    LOG_INFO(
      kp, session, "Loading key for LSN=%" PRIu64 ", size=%" PRIzu, crypt->r.lsn, crypt->keys.size);

    kp_set_key(kp, crypt);

    return (0);
}

/*
 * kp_key_expired --
 *     Check if the current key has expired based on the configured expiration time.
 */
static bool
kp_key_expired(KEY_PROVIDER *kp)
{
    if (kp->key_expires == 0)
        return (false); /* Key does not expire */

    const clock_t now = clock();
    double elapsed_sec = CLOCK_SECS(now - kp->state.key_time);

    return (elapsed_sec >= kp->key_expires);
}

/*
 * kp_rotate_key --
 *     Rotate the current key by generating a new key with a repeating alphabet pattern.
 */
static int
kp_rotate_key(KEY_PROVIDER *kp)
{
    /* Calculate new key size with 20% random fluctuation */
    const size_t base_size = 1024;
    const int fluctuation = (rand() % 41) - 20; /* -20% to +20% */
    const size_t new_key_size = base_size + (size_t)((int)base_size * fluctuation / 100);

    /* Allocate new key buffer */
    uint8_t *new_key = malloc(new_key_size);
    if (new_key == NULL)
        return (ENOMEM);

    /* Fill with repeating alphabet pattern */
    const char *alphabet = "abcdefghijklmnopqrstuvwxyz";
    const size_t alphabet_len = strlen(alphabet);

    /* Fill buffer by repeatedly copying the alphabet pattern */
    size_t remaining = new_key_size;
    size_t offset = 0;
    while (remaining > 0) {
        const size_t copy_len = (remaining >= alphabet_len) ? alphabet_len : remaining;
        memcpy(new_key + offset, alphabet, copy_len);
        offset += copy_len;
        remaining -= copy_len;
    }

    /* Free old key and update state */
    kp_free_key(kp);
    kp->state.current_key = new_key;
    kp->state.key_size = new_key_size;
    kp->state.key_time = clock();
    kp->state.current_lsn = 0; /* New key, no LSN yet */

    return (0);
}

/*
 * kp_get_key --
 *     Fetch the latest key for checkpoint writes. The WT_CRYPT_KEYS::keys::size should be set to
 *     zero if the key has not changed. This is called by WiredTiger when creating a checkpoint to
 *     get the current encryption key.
 */
static int
kp_get_key(WT_KEY_PROVIDER *wtkp, WT_SESSION *session, WT_CRYPT_KEYS *crypt)
{
    KEY_PROVIDER *kp = (KEY_PROVIDER *)wtkp;
    LOG_DEBUG(kp, session, "Current key: LSN=%" PRIu64 ", key_time=%.2f, size=%" PRIzu,
      kp->state.current_lsn, CLOCK_SECS(kp->state.key_time), kp->state.key_size);

    /*
     * Real key provider may rotate the key independently of the get_key calls. In the mock
     * implementation the key is rotated only when its size has been requested. This is to prevent
     * key rotation between paired get_key calls: first requesting the size, then filling the data.
     */
    if (crypt->keys.data == NULL && kp_key_expired(kp)) {
        LOG_INFO(kp, session, "Key expired (key_time=%.2f)", CLOCK_SECS(kp->state.key_time));

        int ret = kp_rotate_key(kp);
        if (ret != 0) {
            LOG_ERROR(kp, session, "Failed to rotate key: %d", ret);
            return (ret);
        }

        LOG_INFO(kp, session, "Reporting new key (key_time=%.2f, key_size=%" PRIzu ")",
          CLOCK_SECS(kp->state.key_time), kp->state.key_size);
        crypt->keys.size = kp->state.key_size;
    } else if (crypt->keys.data != NULL) {
        /* The size of requested data must match previously reported key size. */
        assert(crypt->keys.size == kp->state.key_size);

        /*
         * If requesting key data, it means that key recently expired and has been rotated. The
         * current_lsn must be zero because the key is not persisted yet. on_key_update will update
         * LSN after persistence.
         */
        assert(kp->state.current_lsn == 0);

        LOG_INFO(kp, session, "Providing new key data (key_time=%.2f, key_size=%" PRIzu ")",
          CLOCK_SECS(kp->state.key_time), kp->state.key_size);
        memcpy((void *)crypt->keys.data, kp->state.current_key, crypt->keys.size);
    } else {
        LOG_INFO(kp, session, "Key is still valid, no change (key_time=%.2f)",
          CLOCK_SECS(kp->state.key_time));
        crypt->keys.size = 0;
    }

    return (0);
}

/*
 * kp_on_key_update --
 *     Callback function indicating whether the key has been persisted. On success, the result field
 *     contains LSN of the checkpoint the key belongs to. On failure, the result field is set to the
 *     error code and the size is set to 0.
 */
static int
kp_on_key_update(WT_KEY_PROVIDER *wtkp, WT_SESSION *session, const WT_CRYPT_KEYS *crypt)
{
    KEY_PROVIDER *kp = (KEY_PROVIDER *)wtkp;
    LOG_DEBUG(kp, session, "Current key: LSN=%" PRIu64 ", key_time=%.2f, size=%" PRIzu,
      kp->state.current_lsn, CLOCK_SECS(kp->state.key_time), kp->state.key_size);

    assert(kp->state.current_key != NULL);
    assert(kp->state.current_lsn == 0); /* Key must be new and not persisted yet */

    if (crypt->keys.size == 0) {
        /* Failure case - error is in keys->r.error */
        LOG_ERROR(kp, session, "Key persistence failed with error %d", crypt->r.error);
    } else {
        /* Success case - LSN is in keys->r.lsn */
        LOG_INFO(kp, session, "Key persisted successfully at LSN %" PRIu64, crypt->r.lsn);

        /* Update our internal state */
        kp->state.current_lsn = crypt->r.lsn;

        assert(memcmp(kp->state.current_key, crypt->keys.data, kp->state.key_size) == 0);
        assert(kp->state.key_size == crypt->keys.size);
    }

    return (0);
}

/*
 * kp_terminate --
 *     Cleanup function called when the key provider is being shut down.
 */
static int
kp_terminate(WT_KEY_PROVIDER *wtkp, WT_SESSION *session)
{
    KEY_PROVIDER *kp = (KEY_PROVIDER *)wtkp;

    LOG_INFO(kp, session, "Terminating key provider");

    if (kp->state.current_key != NULL)
        free(kp->state.current_key);

    free(kp);

    return (0);
}

#define CONFIGURE_BEGIN(kp) \
    if (kp == NULL) {       \
        assert(kp != NULL); \
        return (EINVAL);    \
    }

#define CONFIGURE_PARAM(kp, param, k, v, ctype, wt_type)                                   \
    else if (strncmp(#param, k.str, k.len) == 0 && k.len == strlen(#param) && v.len > 0 && \
      v.type == wt_type)                                                                   \
    {                                                                                      \
        kp->param = (ctype)v.val;                                                          \
        continue;                                                                          \
    }

#define CONFIGURE_INT(kp, param, k, v) CONFIGURE_PARAM(kp, param, k, v, int, WT_CONFIG_ITEM_NUM)
#define CONFIGURE_UINT(kp, param, k, v) \
    CONFIGURE_PARAM(kp, param, k, v, unsigned int, WT_CONFIG_ITEM_NUM)

#define CONFIGURE_END(kp, k, v)                                                           \
    else                                                                                  \
    {                                                                                     \
        LOG_ERROR(kp, NULL, "WT_CONFIG_PARSER.next: unexpected configuration: %.*s=%.*s", \
          (int)k.len, k.str, (int)v.len, v.str);                                          \
        ret = EINVAL;                                                                     \
        goto err;                                                                         \
    }

/*
 * kp_configure --
 *     Parse configuration options for the key provider.
 */
static int
kp_configure(KEY_PROVIDER *kp, WT_CONFIG_ARG *config)
{
    WT_EXTENSION_API *wtext = kp->wtext;
    WT_CONFIG_PARSER *config_parser = NULL;
    WT_CONFIG_ITEM k = {0}, v = {0};
    int ret = 0;

    if ((ret = wtext->config_parser_open_arg(wtext, NULL, config, &config_parser)) != 0) {
        LOG_ERROR(kp, NULL, "WT_EXTENSION_API.config_parser_open_arg: error: %d (%s)", ret,
          wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    /* Parse configuration key-value pairs */
    while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
        CONFIGURE_BEGIN(kp)
        CONFIGURE_INT(kp, verbose, k, v)
        CONFIGURE_UINT(kp, key_expires, k, v)
        CONFIGURE_END(kp, k, v)
    }

    if (ret != WT_NOTFOUND) {
        LOG_ERROR(kp, NULL, "WT_CONFIG_PARSER.next: error: %d (%s)", ret,
          wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    ret = config_parser->close(config_parser);
    config_parser = NULL;
    if (ret != 0) {
        LOG_ERROR(kp, NULL, "WT_CONFIG_PARSER.close: error: %d (%s)", ret,
          wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    return (0);

err:
    if (config_parser != NULL)
        (void)config_parser->close(config_parser);

    return (ret);
}

/*
 * wiredtiger_extension_init --
 *     WiredTiger test key provider extension initialization.
 */
int
wiredtiger_extension_init(WT_CONNECTION *conn, WT_CONFIG_ARG *config)
{
    WT_EXTENSION_API *wtext = conn->get_extension_api(conn);

    /* Allocate the key provider structure */
    KEY_PROVIDER *kp;
    if ((kp = calloc(1, sizeof(KEY_PROVIDER))) == NULL) {
        wtext->err_printf(wtext, NULL, "%s: %s", __func__, wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }

    kp->wtext = wtext;
    kp->verbose = WT_VERBOSE_INFO; /* Default verbosity level */
    kp->key_expires = 0;           /* Default: key does not expire */

    WT_KEY_PROVIDER *wtkp = (WT_KEY_PROVIDER *)kp;

    int ret = 0;
    /* Parse configuration options */
    if ((ret = kp_configure(kp, config)) != 0)
        goto err;

    /* Initialize the key provider function table */
    wtkp->load_key = kp_load_key;
    wtkp->get_key = kp_get_key;
    wtkp->on_key_update = kp_on_key_update;
    wtkp->terminate = kp_terminate;

    /* Register the key provider with WiredTiger */
    if ((ret = conn->set_key_provider(conn, wtkp, NULL)) != 0) {
        LOG_ERROR(kp, NULL, "WT_CONNECTION.set_key_provider: %d (%s)", ret,
          wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    LOG_INFO(kp, NULL, "Key provider initialized successfully");

    return (0);

err:
    if (kp != NULL)
        kp_terminate((WT_KEY_PROVIDER *)kp, NULL);

    return (ret);
}
