/*-
 * Public Domain 2014-2019 MongoDB, Inc.
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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

/*! [WT_ENCRYPTOR initialization structure] */
/* Local encryptor structure. */
typedef struct {
    WT_ENCRYPTOR encryptor; /* Must come first */

    WT_EXTENSION_API *wt_api; /* Extension API */

    unsigned long nop_calls; /* Count of calls */

} NOP_ENCRYPTOR;
/*! [WT_ENCRYPTOR initialization structure] */

/*
 * nop_error --
 *     Display an error from this module in a standard way.
 */
static int
nop_error(NOP_ENCRYPTOR *encryptor, WT_SESSION *session, int err, const char *msg)
{
    WT_EXTENSION_API *wt_api;

    wt_api = encryptor->wt_api;
    (void)wt_api->err_printf(
      wt_api, session, "nop encryption: %s: %s", msg, wt_api->strerror(wt_api, NULL, err));
    return (err);
}

/*! [WT_ENCRYPTOR encrypt] */
/*
 * nop_encrypt --
 *     A simple encryption example that passes data through unchanged.
 */
static int
nop_encrypt(WT_ENCRYPTOR *encryptor, WT_SESSION *session, uint8_t *src, size_t src_len,
  uint8_t *dst, size_t dst_len, size_t *result_lenp)
{
    NOP_ENCRYPTOR *nop_encryptor = (NOP_ENCRYPTOR *)encryptor;

    (void)session; /* Unused parameters */

    ++nop_encryptor->nop_calls; /* Call count */

    if (dst_len < src_len)
        return (nop_error(nop_encryptor, session, ENOMEM, "encrypt buffer not big enough"));

    memcpy(dst, src, src_len);
    *result_lenp = src_len;

    return (0);
}
/*! [WT_ENCRYPTOR encrypt] */

/*! [WT_ENCRYPTOR decrypt] */
/*
 * nop_decrypt --
 *     A simple decryption example that passes data through unchanged.
 */
static int
nop_decrypt(WT_ENCRYPTOR *encryptor, WT_SESSION *session, uint8_t *src, size_t src_len,
  uint8_t *dst, size_t dst_len, size_t *result_lenp)
{
    NOP_ENCRYPTOR *nop_encryptor = (NOP_ENCRYPTOR *)encryptor;

    (void)session; /* Unused parameters */
    (void)src_len;

    ++nop_encryptor->nop_calls; /* Call count */

    /*
     * The destination length is the number of unencrypted bytes we're expected to return.
     */
    memcpy(dst, src, dst_len);
    *result_lenp = dst_len;
    return (0);
}
/*! [WT_ENCRYPTOR decrypt] */

/*! [WT_ENCRYPTOR sizing] */
/*
 * nop_sizing --
 *     A simple sizing example that tells wiredtiger that the encrypted buffer is always the same as
 *     the source buffer.
 */
static int
nop_sizing(WT_ENCRYPTOR *encryptor, WT_SESSION *session, size_t *expansion_constantp)
{
    NOP_ENCRYPTOR *nop_encryptor = (NOP_ENCRYPTOR *)encryptor;

    (void)session; /* Unused parameters */

    ++nop_encryptor->nop_calls; /* Call count */

    *expansion_constantp = 0;
    return (0);
}
/*! [WT_ENCRYPTOR sizing] */

/*! [WT_ENCRYPTOR terminate] */
/*
 * nop_terminate --
 *     WiredTiger no-op encryption termination.
 */
static int
nop_terminate(WT_ENCRYPTOR *encryptor, WT_SESSION *session)
{
    NOP_ENCRYPTOR *nop_encryptor = (NOP_ENCRYPTOR *)encryptor;

    (void)session; /* Unused parameters */

    ++nop_encryptor->nop_calls; /* Call count */

    /* Free the allocated memory. */
    free(encryptor);

    return (0);
}
/*! [WT_ENCRYPTOR terminate] */

/*! [WT_ENCRYPTOR initialization function] */
/*
 * wiredtiger_extension_init --
 *     A simple shared library encryption example.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    NOP_ENCRYPTOR *nop_encryptor;
    int ret;

    (void)config; /* Unused parameters */

    if ((nop_encryptor = calloc(1, sizeof(NOP_ENCRYPTOR))) == NULL)
        return (errno);

    /*
     * Allocate a local encryptor structure, with a WT_ENCRYPTOR structure
     * as the first field, allowing us to treat references to either type of
     * structure as a reference to the other type.
     *
     * Heap memory (not static), because it can support multiple databases.
     */
    nop_encryptor->encryptor.encrypt = nop_encrypt;
    nop_encryptor->encryptor.decrypt = nop_decrypt;
    nop_encryptor->encryptor.sizing = nop_sizing;
    nop_encryptor->encryptor.terminate = nop_terminate;

    nop_encryptor->wt_api = connection->get_extension_api(connection);

    /* Load the encryptor */
    if ((ret = connection->add_encryptor(connection, "nop", (WT_ENCRYPTOR *)nop_encryptor, NULL)) ==
      0)
        return (0);

    free(nop_encryptor);
    return (ret);
}
/*! [WT_ENCRYPTOR initialization function] */
