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
 *
 * ex_key_provider.c
 * 	demonstrates how to use the key provider API.
 */
#include <test_util.h>

/*
 * Extension initialization function.
 */
#ifdef _WIN32
/*
 * Explicitly export this function so it is visible when loading extensions.
 */
__declspec(dllexport)
#endif
  int set_my_key_provider(WT_CONNECTION *, WT_CONFIG_ARG *);

typedef struct {
    int id;
    int data;
} MY_CRYPT_DATA;

/*! [key provider struct implementation] */
typedef struct {
    WT_KEY_PROVIDER kp; /* Must come first */

    /* This example stores a fixed size blob in the key provider struct. It is not required. */
    MY_CRYPT_DATA key;
    uint64_t returned_lsn;
} MY_KEY_PROVIDER;

/*
 * my_load_key --
 *     A placeholder example of set_key call.
 */
static int
my_load_key(WT_KEY_PROVIDER *kp, WT_CRYPT_KEY *key)
{
    MY_KEY_PROVIDER *my_kp = (MY_KEY_PROVIDER *)kp;
    WT_CRYPT_KEY *my_key = (WT_CRYPT_KEY *)key;

    memcpy((uint8_t *)&my_kp->key, my_key->data, my_key->size);
    return (0);
}

/*
 * my_get_key --
 *     An simple example of key rotation done on get_key call.
 */
static int
my_get_key(WT_KEY_PROVIDER *kp, WT_CRYPT_KEY *key)
{
    MY_KEY_PROVIDER *my_kp = (MY_KEY_PROVIDER *)kp;

    if ((key = calloc(1, sizeof(MY_CRYPT_DATA) + sizeof(MY_CRYPT_DATA))) == NULL)
        return (ENOMEM);

    MY_CRYPT_DATA *crypt_data = (MY_CRYPT_DATA *)key->data;

    /* Set fields in the MY_CRYPT_DATA structure. */
    crypt_data->data = my_kp->key.data;
    crypt_data->id = my_kp->key.id;

    /* Set the WT_CRYPT_KEY size field to match the allocation. */
    key->size = sizeof(MY_CRYPT_DATA);
    return (0);
}

/*
 * my_on_key_commit --
 *     A simple example of on_key_commit call.
 */
static int
my_on_key_commit(WT_KEY_PROVIDER *kp, WT_CRYPT_KEY *key)
{
    MY_KEY_PROVIDER *my_kp = (MY_KEY_PROVIDER *)kp;

    my_kp->returned_lsn = key->result;
    return (0);
}

/*
 * set_my_key_provider --
 *     A simple example of setting the key provider system.
 */
int
set_my_key_provider(WT_CONNECTION *conn, WT_CONFIG_ARG *config)
{
    MY_KEY_PROVIDER *kps;
    WT_KEY_PROVIDER *wt;
    WT_EXTENSION_API *wtext;

    WT_UNUSED(config);
    wtext = conn->get_extension_api(conn);
    /* Initialize our key provider system. */
    if ((kps = calloc(1, sizeof(MY_KEY_PROVIDER))) == NULL) {
        (void)wtext->err_printf(
          wtext, NULL, "set_my_key_provider: %s", wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }
    wt = (WT_KEY_PROVIDER *)&kps->kp;
    wt->load_key = my_load_key;
    wt->get_key = my_get_key;
    wt->on_key_commit = my_on_key_commit;

    kps->key.id = 1;
    kps->key.data = 1234;
    error_check(conn->set_key_provider(conn, (WT_KEY_PROVIDER *)kps, NULL));
    return (0);
}

static const char *home;

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    const char *open_config;
    int ret = 0;

    WT_UNUSED(argc);
    WT_UNUSED(argv);

    /*
     * Create a clean test directory for this run of the test program if the environment variable
     * isn't already set (as is done by make check).
     */
    if (getenv("WIREDTIGER_HOME") == NULL) {
        home = "WT_HOME";
        ret = system("rm -rf WT_HOME && mkdir WT_HOME");
    } else
        home = NULL;

    /*! [WT_KEY_PROVIDER register] */
    /*
     * Setup a configuration string that will load our key provider system. Use the special local
     * extension to indicate that the entry point is in the same executable. Also enable early load
     * for this extension, since WiredTiger needs to be able to find it before doing any operations.
     */
    open_config =
      "create,log=(enabled=true),extensions=(local={entry=set_my_key_provider,early_load=true})";
    /* Open a connection to the database, creating it if necessary. */
    if ((ret = wiredtiger_open(home, NULL, open_config, &conn)) != 0) {
        fprintf(stderr, "Error connecting to %s: %s\n", home == NULL ? "." : home,
          wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    /*! [WT_KEY_PROVIDER register] */

    return (EXIT_SUCCESS);
}
